/*-------------------------------------------------------------------------
 *
 * seq_buf.c
 *		Routines for sequential buffered data access.
 *
 * Copyright (c) 2021-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/utils/seq_buf.c
 *
 * TODO
 *	  make it lockless with state of following structure
 *		AABBCCCCDDDDDDDD
 *		^ ^	^	^
 *		| |	|   on-disk page number
 *		| |	page offset
 *		| usage count for odd page
 *		usage count for event number
 *	  It would be possible to read/write a value in one CAS and one atomic
 *	  decrement.
 *
 *-------------------------------------------------------------------------
 */
#include "postgres.h"

#include "orioledb.h"

#include "btree/io.h"
#include "catalog/o_sys_cache.h"
#include "utils/seq_buf.h"

#include "pgstat.h"
#include "sys/stat.h"
#include "utils/memdebug.h"

#include <unistd.h>

/*
 * We does not use orioledb page header and should not
 * write it to sequence buffer files.
 */
#define SEQBUF_ALIGN (sizeof(uint64))
#define SEQBUF_CHUNK_SIZE (TYPEALIGN_DOWN(SEQBUF_ALIGN, \
										  ORIOLEDB_BLCKSZ - O_PAGE_HEADER_SIZE))

/*
 * The offset to aligned data.
 */
#define SEQBUF_DATA_OFF (ORIOLEDB_BLCKSZ - SEQBUF_CHUNK_SIZE)

/* we should skip orioledb page header on io operations */
#define SEQBUF_DATA_POS(page) ((Pointer)(page) + SEQBUF_DATA_OFF)

/* offset of current sequence buffer page in file */
#define SEQBUF_FILE_OFFSET(shared, blkno) ((off_t) SEQBUF_CHUNK_SIZE * (blkno) \
												+ (shared)->evictOffset)

/*
 * this functions returns true if success
 */
static bool seq_buf_tag_eq(SeqBufTag *t1, SeqBufTag *t2);
static bool seq_buf_check_open_file(SeqBufDescPrivate *seqBufPrivate);
static bool seq_buf_switch_page(SeqBufDescPrivate *seqBufPrivate);
static inline bool seq_buf_rw(SeqBufDescPrivate *seqBufPrivate,
							  char *data, Size data_size, bool write);
static bool seq_buf_read_pages(SeqBufDescPrivate *seqBufPrivate,
							   SeqBufDescShared *shared, int header_off, off_t evicted_off);

/*
 * Initialize sequential buffered access to given file.
 */
bool
init_seq_buf(SeqBufDescPrivate *seqBufPrivate, SeqBufDescShared *shared,
			 SeqBufTag *tag, bool write, bool init_shared,
			 int skip_len, EvictedSeqBufData *evicted)
{
	bool		evicted_used = evicted != NULL;
	bool		ok = true;

	seqBufPrivate->shared = shared;
	seqBufPrivate->file = -1;
	seqBufPrivate->write = write;

	if (init_shared)
	{
		int			i;

		SpinLockInit(&shared->lock);
		SpinLockAcquire(&shared->lock);

		Assert(OInMemoryBlknoIsValid(shared->pages[0])
			   && OInMemoryBlknoIsValid(shared->pages[1]));

		shared->curPageNum = 0;
		shared->filePageNum = 0;
		shared->freeBytesNum = 0;
		shared->location = SEQBUF_DATA_OFF + (evicted_used ? 0 : skip_len);
		shared->tag = *tag;
		shared->prevPageState = SeqBufPrevPageDone;
		shared->evictOffset = evicted_used ? evicted->offset : 0;
		seqBufPrivate->tag = *tag;

		for (i = 0; i < 2; i++)
		{
			OrioleDBPageDesc *page_desc = O_GET_IN_MEMORY_PAGEDESC(shared->pages[i]);

			page_desc->oids = create_oids();
			page_desc->type = 0;
		}

		if (!write && (tag->type == 'm' || seq_buf_file_exist(tag)))
		{
			ok = seq_buf_read_pages(seqBufPrivate, shared, skip_len,
									evicted_used ? evicted->offset : 0);
		}
		else
		{
			ok = true;
			Assert(write || (tag->type == 't' && (!evicted_used || evicted->offset == 0)));
		}

		SpinLockRelease(&shared->lock);
		VALGRIND_CHECK_MEM_IS_DEFINED(shared, sizeof(*shared));
	}
	else
	{
		seqBufPrivate->tag = shared->tag;
	}

	return ok;
}

char *
get_seq_buf_filename(SeqBufTag *tag)
{
	char	   *result;
	char	   *typename;
	char	   *db_prefix;

	o_get_prefixes_for_relnode(tag->datoid, tag->relnode, NULL, &db_prefix);
	if (tag->type == 't')
		typename = "tmp";
	else if (tag->type == 'm')
		typename = "map";
	else
	{
		Assert(false);
		return NULL;
	}
	/* this format is used by recovery_cleanup_old_files() */
	result = psprintf("%s/%u-%u.%s", db_prefix, tag->relnode, tag->num, typename);
	pfree(db_prefix);
	return result;
}

static bool
seq_buf_tag_eq(SeqBufTag *t1, SeqBufTag *t2)
{
	if (t1->datoid == t2->datoid &&
		t1->relnode == t2->relnode &&
		t1->num == t2->num &&
		t1->type == t2->type)
		return true;
	else
		return false;
}

/*
 * Open underlying file.
 */
static bool
seq_buf_check_open_file(SeqBufDescPrivate *seqBufPrivate)
{
	SeqBufDescShared *shared = seqBufPrivate->shared;
	SeqBufTag	old_tag = seqBufPrivate->tag;
	int			flags;

	while (true)
	{
		char	   *filename;
		bool		file_exists = seqBufPrivate->file > 0;

		if (file_exists)
		{
			if (seq_buf_tag_eq(&seqBufPrivate->tag, &shared->tag))
				break;
		}
		seqBufPrivate->tag = shared->tag;
		SpinLockRelease(&shared->lock);

		filename = get_seq_buf_filename(&seqBufPrivate->tag);
		if (seqBufPrivate->write)
			flags = O_RDWR | O_CREAT | PG_BINARY;
		else
			flags = O_RDONLY | PG_BINARY;

		if (file_exists)
			FileClose(seqBufPrivate->file);

		seqBufPrivate->file = PathNameOpenFile(filename, flags);
		pfree(filename);

		SpinLockAcquire(&shared->lock);

		if (seqBufPrivate->file < 0)
		{
			SpinLockRelease(&shared->lock);
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not open seq buf file %s for %s: %m",
								   get_seq_buf_filename(&shared->tag),
								   (seqBufPrivate->write ? "write" : "read"))));
			seqBufPrivate->tag = old_tag;
			return false;
		}
	}
	return true;
}

void
seq_buf_close_file(SeqBufDescPrivate *seqBufPrivate)
{
	if (seqBufPrivate->file > 0)
	{
		FileClose(seqBufPrivate->file);
		seqBufPrivate->file = -1;
	}
}

static bool
seq_buf_wait_prev_page(SeqBufDescShared *shared)
{
	SpinDelayStatus status;

	if (shared->prevPageState != SeqBufPrevPageInProgress)
		return false;

	init_local_spin_delay(&status);
	while (shared->prevPageState == SeqBufPrevPageInProgress)
	{
		SpinLockRelease(&shared->lock);
		perform_spin_delay(&status);
		SpinLockAcquire(&shared->lock);
	}
	finish_spin_delay(&status);
	return true;
}

static bool
seq_buf_finish_prev_page(SeqBufDescPrivate *seqBufPrivate)
{
	SeqBufDescShared *shared = seqBufPrivate->shared;
	off_t		offset;

	if (seqBufPrivate->write)
	{
		offset = SEQBUF_FILE_OFFSET(shared, (off_t) shared->filePageNum - 1);

		/* Write previous page */
		if (OFileWrite(seqBufPrivate->file,
					   SEQBUF_DATA_POS(O_GET_IN_MEMORY_PAGE(shared->pages[1 - shared->curPageNum])),
					   SEQBUF_CHUNK_SIZE, offset, WAIT_EVENT_SLRU_WRITE) != SEQBUF_CHUNK_SIZE)
		{
			SpinLockRelease(&shared->lock);
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("Error write seq buf %s at offset %u: %m",
								   FilePathName(seqBufPrivate->file),
								   (uint32) offset)));
			return false;
		}
	}
	else
	{
		char		buf[ORIOLEDB_BLCKSZ];

		memset(buf, 0xFF, ORIOLEDB_BLCKSZ);
#ifdef USE_ASSERT_CHECKING
		put_page_image(shared->pages[1 - shared->curPageNum], buf);
#endif
		if (shared->freeBytesNum > 0)
		{
			/* Read next page */
			int			nbytes;

			offset = SEQBUF_FILE_OFFSET(shared, (off_t) shared->filePageNum + 1);

			if ((nbytes = OFileRead(seqBufPrivate->file, SEQBUF_DATA_POS(buf), SEQBUF_CHUNK_SIZE,
									offset, WAIT_EVENT_SLRU_READ)) == 0)
			{
				SpinLockRelease(&shared->lock);
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("Error read seq buf %s at offset %u: %m",
									   FilePathName(seqBufPrivate->file),
									   (uint32) offset)));
				return false;
			}

			if (shared->freeBytesNum >= SEQBUF_CHUNK_SIZE &&
				nbytes != SEQBUF_CHUNK_SIZE)
			{
				SpinLockRelease(&shared->lock);
				Assert(nbytes < SEQBUF_CHUNK_SIZE);
				elog(PANIC, "Error read sequence buffer file %s at offset %u."
					 "Bytes read = %d is less than expected = %ld.",
					 get_seq_buf_filename(&seqBufPrivate->tag), (uint32) offset,
					 nbytes, SEQBUF_CHUNK_SIZE);
				return false;
			}
			else if (shared->freeBytesNum < SEQBUF_CHUNK_SIZE &&
					 shared->freeBytesNum != nbytes)
			{
				SpinLockRelease(&shared->lock);
				elog(PANIC, "Error read sequence buffer file %s at offset %u. "
					 "Bytes read = %d is not equal than expected = %lu",
					 get_seq_buf_filename(&seqBufPrivate->tag), (uint32) offset,
					 nbytes, (uint64) shared->freeBytesNum);
				return false;
			}

			shared->freeBytesNum -= nbytes;
			Assert(shared->freeBytesNum >= 0);
			put_page_image(shared->pages[1 - shared->curPageNum], buf);
		}
	}
	return true;
}

/*
 * Switch to the next page after filePageNum.  Function returns control when
 * we have switched to the next page or other process did it instead of us.
 *
 * Private->shared should be locked. Call unlocks seqBufPrivate->shared.
 */
static bool
seq_buf_switch_page(SeqBufDescPrivate *seqBufPrivate)
{
	SeqBufDescShared *shared = seqBufPrivate->shared;
	uint32		filePageNum = shared->filePageNum;
	SeqBufPrevPageState resultState;

	if (!seq_buf_check_open_file(seqBufPrivate))
	{
		SpinLockRelease(&shared->lock);
		return false;
	}

	/* Check if it's already switched after given page number... */
	if (shared->filePageNum != filePageNum)
	{
		SpinLockRelease(&shared->lock);
		return true;
	}

	/*
	 * Check if it's already switched after waiting when previous page have
	 * been processed
	 */
	if (seq_buf_wait_prev_page(shared) &&
		shared->filePageNum != filePageNum)
	{
		SpinLockRelease(&shared->lock);
		return true;
	}

	if (shared->prevPageState == SeqBufPrevPageError)
	{
		if (!seq_buf_finish_prev_page(seqBufPrivate))
		{
			SpinLockRelease(&shared->lock);
			return false;
		}
		shared->prevPageState = SeqBufPrevPageDone;
	}

	Assert(shared->prevPageState == SeqBufPrevPageDone);
	Assert(shared->location == ORIOLEDB_BLCKSZ);

	shared->curPageNum = 1 - shared->curPageNum;
	shared->filePageNum++;
	shared->location = SEQBUF_DATA_OFF;
	shared->prevPageState = SeqBufPrevPageInProgress;
	SpinLockRelease(&shared->lock);

	resultState = seq_buf_finish_prev_page(seqBufPrivate) ? SeqBufPrevPageDone
		: SeqBufPrevPageError;

	SpinLockAcquire(&shared->lock);
	shared->prevPageState = resultState;
	SpinLockRelease(&shared->lock);

	/* If even we didn't finish the next page, current page is OK. */
	return true;
}

/*
 * Private function which reads/writes data from/to sequential file.
 */
static inline bool
seq_buf_rw(SeqBufDescPrivate *seqBufPrivate, char *data, Size data_size, bool write)
{
	SeqBufDescShared *shared = seqBufPrivate->shared;
	Page		page;
	bool		switched;

	Assert(seqBufPrivate->write == write);

	do
	{
		SpinLockAcquire(&shared->lock);
		if (shared->location + data_size <= ORIOLEDB_BLCKSZ)
		{
			page = O_GET_IN_MEMORY_PAGE(shared->pages[shared->curPageNum]);
			if (write)
				memcpy(page + shared->location, data, data_size);
			else
			{
				memcpy(data, page + shared->location, data_size);
			}
			shared->location += data_size;

			SpinLockRelease(&shared->lock);
			return true;
		}
		switched = seq_buf_switch_page(seqBufPrivate);	/* releases shared->lock */
	} while (switched);
	return false;				/* can not switch to another page */
}

/*
 * Writes uint32 offset to a sequential file.
 * Returns true if success.
 */
bool
seq_buf_write_u32(SeqBufDescPrivate *seqBufPrivate, uint32 offset)
{
	Assert((SEQBUF_CHUNK_SIZE % sizeof(FileExtent)) == 0);
	return seq_buf_rw(seqBufPrivate, (char *) &offset, sizeof(uint32), true);
}

/*
 * Writes FileExtent to a sequential file.
 * Returns true if success.
 */
bool
seq_buf_write_file_extent(SeqBufDescPrivate *seqBufPrivate, FileExtent extent)
{
	Assert((SEQBUF_CHUNK_SIZE % sizeof(FileExtent)) == 0);
	return seq_buf_rw(seqBufPrivate, (char *) &extent, sizeof(FileExtent), true);
}

/*
 * Reads uint32 offset from a sequential file.
 * Returns true if success.
 */
bool
seq_buf_read_u32(SeqBufDescPrivate *seqBufPrivate, uint32 *ptr)
{
	return seq_buf_rw(seqBufPrivate, (char *) ptr, sizeof(uint32), false);
}

/*
 * Reads FileExtent from a sequential file.
 * Returns true if success.
 */
bool
seq_buf_read_file_extent(SeqBufDescPrivate *seqBufPrivate, FileExtent *extent)
{
	Assert((SEQBUF_CHUNK_SIZE % sizeof(FileExtent)) == 0);
	return seq_buf_rw(seqBufPrivate, (char *) extent, sizeof(FileExtent), false);
}

/*
 * Finalize work with sequential file.
 */
uint64
seq_buf_finalize(SeqBufDescPrivate *seqBufPrivate)
{
	SeqBufDescShared *shared = seqBufPrivate->shared;
	off_t		result;

	SpinLockAcquire(&shared->lock);
	seq_buf_wait_prev_page(shared);
	if (shared->prevPageState == SeqBufPrevPageError)
	{
		if (!seq_buf_finish_prev_page(seqBufPrivate))
		{
			SpinLockRelease(&shared->lock);
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not finalize previous sequence buffer page to file %s: %m",
								   get_seq_buf_filename(&seqBufPrivate->tag))));
		}
		shared->prevPageState = SeqBufPrevPageDone;
	}

	if (seqBufPrivate->write)
	{
		if (!seq_buf_check_open_file(seqBufPrivate))
		{
			SpinLockRelease(&shared->lock);
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not open sequence buffer file %s: %m",
								   get_seq_buf_filename(&seqBufPrivate->tag))));
		}

		if (shared->location > 0)
		{
			off_t		offset = SEQBUF_FILE_OFFSET(shared, (off_t) shared->filePageNum);

			if (OFileWrite(seqBufPrivate->file, SEQBUF_DATA_POS(O_GET_IN_MEMORY_PAGE(shared->pages[shared->curPageNum])),
						   shared->location - SEQBUF_DATA_OFF, offset, WAIT_EVENT_SLRU_WRITE) != shared->location - SEQBUF_DATA_OFF)
			{
				SpinLockRelease(&shared->lock);
				ereport(PANIC, (errcode_for_file_access(),
								errmsg("could not finalize sequence buffer into file %s: %m",
									   FilePathName(seqBufPrivate->file))));
			}
		}
	}

	result = SEQBUF_FILE_OFFSET(shared, (off_t) shared->filePageNum)
		+ (shared->location - SEQBUF_DATA_OFF);
	SpinLockRelease(&shared->lock);

	seq_buf_close_file(seqBufPrivate);

	if (result == 0)
		seq_buf_remove_file(&seqBufPrivate->tag);

	return result;
}

/*
 * Get current offset in the file.
 */
uint64
seq_buf_get_offset(SeqBufDescPrivate *seqBufPrivate)
{
	SeqBufDescShared *shared = seqBufPrivate->shared;
	uint64		offset;

	SpinLockAcquire(&shared->lock);
	offset = SEQBUF_FILE_OFFSET(shared, (off_t) shared->filePageNum)
		+ (shared->location - SEQBUF_DATA_OFF);
	SpinLockRelease(&shared->lock);

	return offset;
}

/*
 * Try to replace sequential file with newer one.
 */
SeqBufReplaceResult
seq_buf_try_replace(SeqBufDescPrivate *seqBufPrivate, SeqBufTag *tag,
					pg_atomic_uint64 *size, Size data_size)
{
	SeqBufDescShared *shared = seqBufPrivate->shared;
	off_t		len;
	SeqBufTag	old_tag = {0};

	Assert(tag->type == 't');
	Assert(!seqBufPrivate->write);
	Assert((SEQBUF_CHUNK_SIZE % data_size) == 0);

	SpinLockAcquire(&shared->lock);
	Assert(shared->tag.datoid == tag->datoid && shared->tag.relnode == tag->relnode);

	seq_buf_wait_prev_page(shared);
	if (shared->prevPageState == SeqBufPrevPageError)
		shared->prevPageState = SeqBufPrevPageDone;

	if (shared->tag.num >= tag->num)
	{
		/* Already have newer sequential file */
		SpinLockRelease(&shared->lock);
		return SeqBufReplaceAlready;
	}

	old_tag = shared->tag;
	shared->tag = *tag;

	if (seq_buf_file_exist(tag))
	{
		if (!seq_buf_read_pages(seqBufPrivate, shared, 0, 0))
		{
			shared->tag = old_tag;
			SpinLockRelease(&shared->lock);
			return SeqBufReplaceError;
		}

		if ((len = FileSize(seqBufPrivate->file)) < 0)
		{
			shared->tag = old_tag;
			SpinLockRelease(&shared->lock);
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not seek to the end of file %s: %m",
								   FilePathName(seqBufPrivate->file))));
			return SeqBufReplaceError;
		}
		pg_atomic_write_u64(size, len / data_size);
	}
	else
	{
		pg_atomic_write_u64(size, 0);
	}

	shared->curPageNum = 0;
	shared->filePageNum = 0;
	/* reads data from tmp file, it has not header */
	shared->location = SEQBUF_DATA_OFF;
	shared->evictOffset = 0;
	shared->prevPageState = SeqBufPrevPageDone;

	SpinLockRelease(&shared->lock);

	return SeqBufReplaceSuccess;
}

static bool
seq_buf_read_pages(SeqBufDescPrivate *seqBufPrivate, SeqBufDescShared *shared,
				   int header_off, off_t evicted_off)
{
	char		buf_first[ORIOLEDB_BLCKSZ];
	char		buf_second[ORIOLEDB_BLCKSZ];
	int			nbytes,
				len,
				free_bytes,
				should_read;

	shared->freeBytesNum = 0;

	if (!seq_buf_check_open_file(seqBufPrivate))
		return false;

	len = FileSize(seqBufPrivate->file);
	if (len < header_off)
	{
		SpinLockRelease(&shared->lock);
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("length %d of file %s is less than header %d: %m",
							   len, FilePathName(seqBufPrivate->file), header_off)));
		return false;
	}

	if (len < evicted_off)
		return false;

	if (len == 0 && header_off == 0)
		return true;

	len -= evicted_off;
	if (len == 0 && header_off == 0)
		return true;

	memset(buf_first, 0xFF, ORIOLEDB_BLCKSZ);
	memset(buf_second, 0xFF, ORIOLEDB_BLCKSZ);

	/* read first page */
	should_read = len > SEQBUF_CHUNK_SIZE ? SEQBUF_CHUNK_SIZE : len;
	nbytes = OFileRead(seqBufPrivate->file, SEQBUF_DATA_POS(buf_first), should_read, evicted_off, WAIT_EVENT_SLRU_READ);
	if (nbytes != should_read)
	{
		SpinLockRelease(&shared->lock);
		ereport(PANIC, (errcode_for_file_access(),
						errmsg("could not to read first page from file %s, read = %d, expected = %d: %m",
							   FilePathName(seqBufPrivate->file), nbytes, should_read)));
		return false;
	}
	free_bytes = len - nbytes;

	if (len > SEQBUF_CHUNK_SIZE)
	{
		/* read second page */
		evicted_off += should_read;
		should_read = len - SEQBUF_CHUNK_SIZE;
		should_read = should_read > SEQBUF_CHUNK_SIZE ? SEQBUF_CHUNK_SIZE : should_read;
		nbytes = OFileRead(seqBufPrivate->file, SEQBUF_DATA_POS(buf_second), should_read, evicted_off, WAIT_EVENT_SLRU_READ);
		if (nbytes != should_read)
		{
			SpinLockRelease(&shared->lock);
			ereport(PANIC, (errcode_for_file_access(),
							errmsg("could not to read second page from file %s, read = %d, expected = %d: %m",
								   FilePathName(seqBufPrivate->file), nbytes, should_read)));
			return false;
		}
		free_bytes -= nbytes;
	}

	put_page_image(shared->pages[0], buf_first);
	put_page_image(shared->pages[1], buf_second);
	shared->freeBytesNum = free_bytes;
	return true;
}

static inline char *
seq_buf_filename_if_exist(SeqBufTag *tag)
{
	char	   *filename;
	struct stat not_used;

	filename = get_seq_buf_filename(tag);
	if (stat(filename, &not_used) == 0)
		return filename;
	pfree(filename);
	return NULL;
}

bool
seq_buf_file_exist(SeqBufTag *tag)
{
	char	   *filename;

	if ((filename = seq_buf_filename_if_exist(tag)) == NULL)
		return false;

	pfree(filename);
	return true;
}

bool
seq_buf_remove_file(SeqBufTag *tag)
{
	char	   *filename;

	if ((filename = seq_buf_filename_if_exist(tag)) == NULL)
		return false;

	unlink(filename);
	pfree(filename);
	return true;
}
