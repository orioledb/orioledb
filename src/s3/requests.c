/*-------------------------------------------------------------------------
 *
 * requests.c
 *		Implementation for S3 requests.
 *
 * Copyright (c) 2024-2025, Oriole DB Inc.
 * Copyright (c) 2025, Supabase Inc.
 *
 * IDENTIFICATION
 *	  contrib/orioledb/src/s3/requests.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <stdbool.h>
#include <unistd.h>

#include "orioledb.h"

#include "s3/requests.h"

#include "common/base64.h"
#include "lib/stringinfo.h"
#include "utils/wait_event.h"

#include "curl/curl.h"
#include "openssl/hmac.h"
#include "openssl/sha.h"

PG_FUNCTION_INFO_V1(s3_get);
PG_FUNCTION_INFO_V1(s3_put);

static void
hmac_sha256(char *input, char *output, char *secretkey, int secretkeylen)
{
	HMAC_CTX   *ctx;
	unsigned int len;

	ctx = HMAC_CTX_new();
	HMAC_Init_ex(ctx, secretkey, secretkeylen, EVP_sha256(), NULL);
	HMAC_Update(ctx, (unsigned char *) input, strlen(input));
	HMAC_Final(ctx, (unsigned char *) output, &len);
	HMAC_CTX_free(ctx);

	Assert(len == 32);
}

/*
 * Make the hex representation of the binary string.
 */
static char *
hex_string(Pointer data, int len)
{
	char	   *result = palloc(len * 2 + 1);

	hex_encode(data, len, result);

	result[len * 2] = '\0';
	return result;
}

/*
 * Calculate the checksum of canonical request according to AWS4-HMAC-SHA256.
 */
static char *
canonical_request_checksum(char *method, char *datetime, char *objectname,
						   char *contentchecksum)
{
	StringInfoData buf;
	unsigned char checksumbuf[32];

	initStringInfo(&buf);
	appendStringInfo(&buf, "%s\n", method);
	appendStringInfo(&buf, "/%s\n", objectname);
	appendStringInfo(&buf, "\n");
	appendStringInfo(&buf, "host:%s\n", s3_host);
	appendStringInfo(&buf, "x-amz-content-sha256:%s\n", contentchecksum);
	appendStringInfo(&buf, "x-amz-date:%s\n", datetime);
	appendStringInfo(&buf, "\n");
	appendStringInfo(&buf, "host;x-amz-content-sha256;x-amz-date\n");
	appendStringInfo(&buf, "%s", contentchecksum);

	(void) SHA256((unsigned char *) buf.data, buf.len, checksumbuf);
	pfree(buf.data);

	return hex_string((Pointer) checksumbuf, sizeof(checksumbuf));
}

/*
 * Construct signed string for the Authorization header,
 * following the Amazon S3 REST API spec.
 */
static char *
s3_signature(char *method, char *datetimestring, char *datestring,
			 char *objectname, char *secretkey, char *checksumstring)
{
	StringInfoData buf;
	char	   *key;
	char		checksumbuf[32];
	char	   *canonical_checksum;

	canonical_checksum = canonical_request_checksum(method, datetimestring,
													objectname, checksumstring);

	key = psprintf("AWS4%s", s3_secretkey);
	hmac_sha256(datestring, checksumbuf, key, strlen(key));
	hmac_sha256(s3_region, checksumbuf, checksumbuf, sizeof(checksumbuf));
	hmac_sha256("s3", checksumbuf, checksumbuf, sizeof(checksumbuf));
	hmac_sha256("aws4_request", checksumbuf, checksumbuf, sizeof(checksumbuf));

	initStringInfo(&buf);
	appendStringInfo(&buf, "AWS4-HMAC-SHA256\n");
	appendStringInfo(&buf, "%s\n", datetimestring);
	appendStringInfo(&buf, "%s/%s/s3/aws4_request\n", datestring, s3_region);
	appendStringInfo(&buf, "%s", canonical_checksum);

	hmac_sha256(buf.data, checksumbuf, checksumbuf, sizeof(checksumbuf));

	pfree(key);
	pfree(canonical_checksum);
	pfree(buf.data);

	return hex_string(checksumbuf, 32);
}

/*
 * Constructs GMT-style string for date.
 */
static char *
httpdate(time_t *timer)
{
	char	   *datetimestring;
	time_t		t;
	struct tm  *gt;

	t = time(timer);
	gt = gmtime(&t);
	datetimestring = (char *) palloc0(256 * sizeof(char));
	strftime(datetimestring, 256 * sizeof(char), "%Y%m%d", gt);
	return datetimestring;
}

/*
 * Constructs GMT-style string for date and time.
 */
static char *
httpdatetime(time_t *timer)
{
	char	   *datetimestring;
	time_t		t;
	struct tm  *gt;

	t = time(timer);
	gt = gmtime(&t);
	datetimestring = (char *) palloc0(256 * sizeof(char));
	strftime(datetimestring, 256 * sizeof(char), "%Y%m%dT%H%M%SZ", gt);
	return datetimestring;
}

/*
 * Curl callback, which appends data to String Info.
 */
static size_t
write_data_to_buf(void *buffer, size_t size, size_t nmemb, void *userp)
{
	size_t		segsize = size * nmemb;
	StringInfo	info = (StringInfo) userp;

	appendBinaryStringInfo(info, (const char *) buffer, segsize);

	return segsize;
}

/*
 * Get the binary content of an object from S3 into 'str'.
 *
 * Returns HTTP status code.
 */
long
s3_get_object(char *objectname, StringInfo str, bool missing_ok)
{
	CURL	   *curl;
	char	   *url;
	char	   *datestring;
	char	   *datetimestring;
	char	   *signature;
	struct curl_slist *slist;
	char	   *tmp;
	int			sc;
	unsigned char checksumbuf[SHA256_DIGEST_LENGTH];
	char	   *checksumstringbuf;
	char	   *objectpath = objectname;
	long		http_code = 0;

	(void) SHA256(NULL, 0, checksumbuf);
	checksumstringbuf = hex_string((Pointer) checksumbuf, sizeof(checksumbuf));

	if (s3_prefix)
	{
		int			prefix_len = strlen(s3_prefix);

		if (prefix_len != 0)
		{
			if (s3_prefix[prefix_len - 1] == '/')
				prefix_len--;
			objectpath = psprintf("%.*s/%s", prefix_len, s3_prefix, objectname);
		}
	}

	url = psprintf("%s://%s/%s",
				   s3_use_https ? "https" : "http", s3_host, objectpath);
	datestring = httpdate(NULL);
	datetimestring = httpdatetime(NULL);
	signature = s3_signature("GET", datetimestring, datestring, objectpath,
							 s3_secretkey, checksumstringbuf);

	slist = NULL;
	slist = curl_slist_append(slist, (tmp = psprintf("x-amz-date: %s", datetimestring)));
	pfree(tmp);
	slist = curl_slist_append(slist, (tmp = psprintf("x-amz-content-sha256: %s", checksumstringbuf)));
	pfree(tmp);
	slist = curl_slist_append(slist,
							  (tmp = psprintf("Authorization: AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s",
											  s3_accesskey, datestring, s3_region, signature)));
	pfree(tmp);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	if (s3_cainfo)
		curl_easy_setopt(curl, CURLOPT_CAINFO, s3_cainfo);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data_to_buf);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, str);

	sc = curl_easy_perform(curl);
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

	if (sc != 0 || http_code != S3_RESPONSE_OK)
	{
		if (missing_ok && http_code == S3_RESPONSE_NOT_FOUND)
		{
			/* Do nothing just return http_code */
		}
		else
			ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("could not get object from S3"),
							errdetail("return code = %d, http code = %ld, response = %s",
									  sc, http_code, str->data)));
	}

	curl_easy_cleanup(curl);

	curl_slist_free_all(slist);
	pfree(url);
	pfree(datestring);
	pfree(datetimestring);
	pfree(signature);
	if (objectpath != objectname)
		pfree(objectpath);
	pfree(checksumstringbuf);

	return http_code;
}

/*
 * A SQL function to get object from S3.  Currently only used for debugging
 * purposes.
 */
Datum
s3_get(PG_FUNCTION_ARGS)
{
	StringInfoData buf;

	initStringInfo(&buf);

	s3_get_object(text_to_cstring(PG_GETARG_TEXT_PP(0)), &buf, false);

	PG_RETURN_TEXT_P(cstring_to_text(buf.data));
}

/*
 * Delete an object from an S3 bucket.
 */
void
s3_delete_object(char *objectname)
{
	CURL	   *curl;
	char	   *url;
	char	   *datestring;
	char	   *datetimestring;
	char	   *signature;
	struct curl_slist *slist;
	char	   *tmp;
	int			sc;
	StringInfoData buf;
	unsigned char checksumbuf[SHA256_DIGEST_LENGTH];
	char	   *checksumstringbuf;
	char	   *objectpath = objectname;
	long		http_code = 0;

	(void) SHA256(NULL, 0, checksumbuf);
	checksumstringbuf = hex_string((Pointer) checksumbuf, sizeof(checksumbuf));

	if (s3_prefix)
	{
		int			prefix_len = strlen(s3_prefix);

		if (prefix_len != 0)
		{
			if (s3_prefix[prefix_len - 1] == '/')
				prefix_len--;
			objectpath = psprintf("%.*s/%s", prefix_len, s3_prefix, objectname);
		}
	}

	url = psprintf("%s://%s/%s",
				   s3_use_https ? "https" : "http", s3_host, objectpath);
	datestring = httpdate(NULL);
	datetimestring = httpdatetime(NULL);
	signature = s3_signature("DELETE", datetimestring, datestring, objectpath,
							 s3_secretkey, checksumstringbuf);

	slist = NULL;
	slist = curl_slist_append(slist, (tmp = psprintf("x-amz-date: %s", datetimestring)));
	pfree(tmp);
	slist = curl_slist_append(slist, (tmp = psprintf("x-amz-content-sha256: %s", checksumstringbuf)));
	pfree(tmp);
	slist = curl_slist_append(slist,
							  (tmp = psprintf("Authorization: AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s",
											  s3_accesskey, datestring, s3_region, signature)));
	pfree(tmp);

	initStringInfo(&buf);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "DELETE");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	if (s3_cainfo)
		curl_easy_setopt(curl, CURLOPT_CAINFO, s3_cainfo);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data_to_buf);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	sc = curl_easy_perform(curl);
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

	if (sc != 0 || http_code != 204 || strlen(buf.data) != 0)
		ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
						errmsg("could not delete object from S3"),
						errdetail("return code = %d, http code = %ld, response = %s",
								  sc, http_code, buf.data)));

	curl_easy_cleanup(curl);

	curl_slist_free_all(slist);
	pfree(url);
	pfree(datestring);
	pfree(datetimestring);
	pfree(signature);
	pfree(buf.data);
	if (objectpath != objectname)
		pfree(objectpath);
	pfree(checksumstringbuf);
}

/*
 * Reads the part of the file 'filename' from 'offset' with length 'maxSize'.
 * The actual length might appear to be lower, it's to be written to '*size'.
 */
static Pointer
read_file_part(const char *filename, uint64 offset,
			   uint64 maxSize, uint64 *size)
{
	int			file;
	Pointer		buffer,
				ptr;
	uint64		totalSize;

	file = BasicOpenFile(filename, O_RDONLY | PG_BINARY);
	if (file < 0)
	{
		ereport(WARNING,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", filename)));
		return NULL;
	}

	totalSize = lseek(file, 0, SEEK_END);
	totalSize = Min(totalSize, offset + maxSize);
	*size = Max(totalSize, offset) - offset;
	buffer = (Pointer) MemoryContextAllocHuge(CurrentMemoryContext, *size);

	ptr = buffer;
	while (offset < totalSize)
	{
		int			amount = Min(totalSize - offset, BLCKSZ);
		int			rc;

		pgstat_report_wait_start(WAIT_EVENT_DATA_FILE_READ);
		rc = pg_pread(file, ptr, amount, offset);
		pgstat_report_wait_end();

		if (rc < 0)
		{
			ereport(ERROR,
					(errcode_for_file_access(),
					 errmsg("could not read file \"%s\": %m", filename)));
			return NULL;
		}

		if (rc != amount)
		{
			amount = rc;
			*size = (ptr - buffer) + amount;
			break;
		}

		offset += amount;
		ptr += amount;
	}

	if (close(file) != 0)
		ereport(PANIC,
				(errcode_for_file_access(),
				 errmsg("could not close file \"%s\": %m", filename)));

	return buffer;
}

/*
 * Writes the part of the file 'filename' from 'offset' with length 'size'.
 */
static void
write_file_part(const char *filename, uint64 offset,
				Pointer data, uint64 size)
{
	File		file;
	int			rc;

	file = PathNameOpenFile(filename, O_CREAT | O_RDWR | PG_BINARY);
	if (file < 0)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not open file \"%s\": %m", filename)));
		return;
	}

	rc = FileWrite(file, data, size, offset, WAIT_EVENT_DATA_FILE_WRITE);

	if (rc < 0 || rc != size)
	{
		ereport(ERROR,
				(errcode_for_file_access(),
				 errmsg("could not write file \"%s\": %m", filename)));
		return;
	}

	FileWriteback(file, offset, size, WAIT_EVENT_DATA_FILE_FLUSH);

	FileClose(file);
}

/*
 * Read the whole file.
 */
Pointer
read_file(const char *filename, uint64 *size)
{
	return read_file_part(filename, 0, UINT64_MAX, size);
}

/*
 * Write the whole file.
 */
static void
write_file(const char *filename, Pointer data, uint64 size)
{
	write_file_part(filename, 0, data, size);
}

/*
 * Put object with given binary contents to S3.
 *
 * If dataChecksum is NULL the function calculates checksum of the content.
 *
 * Returns HTTP status code.
 */
long
s3_put_object_with_contents(char *objectname, Pointer data, uint64 dataSize,
							char *dataChecksum, bool ifNoneMatch)
{
	CURL	   *curl;
	char	   *url;
	char	   *datestring;
	char	   *datetimestring;
	char	   *signature;
	char	   *checksumstringbuf;
	char	   *objectpath = objectname;
	struct curl_slist *slist;
	char	   *tmp;
	int			sc;
	StringInfoData buf;
	long		http_code = 0;

	if (dataChecksum == NULL)
	{
		unsigned char checksumbuf[SHA256_DIGEST_LENGTH];

		(void) SHA256((unsigned char *) data, dataSize, checksumbuf);
		checksumstringbuf = hex_string((Pointer) checksumbuf, sizeof(checksumbuf));
	}
	else
		checksumstringbuf = dataChecksum;

	if (s3_prefix)
	{
		int			prefix_len = strlen(s3_prefix);

		if (prefix_len != 0)
		{
			if (s3_prefix[prefix_len - 1] == '/')
				prefix_len--;
			objectpath = psprintf("%.*s/%s", prefix_len, s3_prefix, objectname);
		}
	}

	url = psprintf("%s://%s/%s",
				   s3_use_https ? "https" : "http", s3_host, objectpath);
	datestring = httpdate(NULL);
	datetimestring = httpdatetime(NULL);
	signature = s3_signature("PUT", datetimestring, datestring, objectpath,
							 s3_secretkey, checksumstringbuf);

	slist = NULL;
	slist = curl_slist_append(slist, (tmp = psprintf("x-amz-date: %s", datetimestring)));
	pfree(tmp);
	slist = curl_slist_append(slist, (tmp = psprintf("x-amz-content-sha256: %s", checksumstringbuf)));
	pfree(tmp);
	slist = curl_slist_append(slist, (tmp = psprintf("Content-Length: %lu", dataSize)));
	pfree(tmp);
	slist = curl_slist_append(slist,
							  (tmp = psprintf("Authorization: AWS4-HMAC-SHA256 Credential=%s/%s/%s/s3/aws4_request, SignedHeaders=host;x-amz-content-sha256;x-amz-date, Signature=%s",
											  s3_accesskey, datestring, s3_region, signature)));
	pfree(tmp);
	slist = curl_slist_append(slist, "Content-Type: application/octet-stream");
	if (ifNoneMatch)
		slist = curl_slist_append(slist, "If-None-Match: *");

	initStringInfo(&buf);

	curl = curl_easy_init();
	curl_easy_setopt(curl, CURLOPT_CUSTOMREQUEST, "PUT");
	curl_easy_setopt(curl, CURLOPT_HTTPHEADER, slist);
	curl_easy_setopt(curl, CURLOPT_URL, url);
	if (s3_cainfo)
		curl_easy_setopt(curl, CURLOPT_CAINFO, s3_cainfo);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDS, data);
	curl_easy_setopt(curl, CURLOPT_POSTFIELDSIZE, dataSize);
	curl_easy_setopt(curl, CURLOPT_WRITEFUNCTION, write_data_to_buf);
	curl_easy_setopt(curl, CURLOPT_WRITEDATA, &buf);

	sc = curl_easy_perform(curl);
	curl_easy_getinfo(curl, CURLINFO_RESPONSE_CODE, &http_code);

	if (sc != 0 || http_code != S3_RESPONSE_OK || strlen(buf.data) != 0)
	{
		/*
		 * Return false if PUT failed to upload object it already exists in
		 * the bucket.
		 */
		if (ifNoneMatch && (http_code == S3_RESPONSE_CONDITION_FAILED ||
							http_code == S3_RESPONSE_CONDITION_CONFLICT))
		{
			/* Do nothing just return http_code */
		}
		else
			ereport(FATAL, (errcode(ERRCODE_CONNECTION_EXCEPTION),
							errmsg("could not put object to S3"),
							errdetail("return code = %d, http code = %ld, response = %s",
									  sc, http_code, buf.data)));
	}

	curl_easy_cleanup(curl);

	curl_slist_free_all(slist);
	pfree(url);
	pfree(datestring);
	pfree(datetimestring);
	pfree(signature);
	pfree(buf.data);
	if (objectpath != objectname)
		pfree(objectpath);
	if (checksumstringbuf != dataChecksum)
		pfree(checksumstringbuf);

	return http_code;
}

/*
 * Put the whole file as S3 object.
 */
long
s3_put_file(char *objectname, char *filename, bool ifNoneMatch)
{
	Pointer		data;
	uint64		dataSize = 0;
	long		res = -1;

	data = read_file(filename, &dataSize);
	if (data)
	{
		res = s3_put_object_with_contents(objectname, data, dataSize, NULL,
										  ifNoneMatch);
		pfree(data);
	}

	return res;
}

/*
 * Get the whole file from S3 object.
 */
void
s3_get_file(char *objectname, char *filename)
{
	StringInfoData buf;

	initStringInfo(&buf);
	s3_get_object(objectname, &buf, false);

	write_file(filename,
			   (Pointer) buf.data,
			   buf.len);

	pfree(buf.data);
}

/*
 * Put the file part as S3 object.
 */
long
s3_put_file_part(char *objectname, char *filename, int partnum)
{
	Pointer		data;
	uint64		dataSize;
	long		res = -1;

	data = read_file_part(filename,
						  partnum * ORIOLEDB_S3_PART_SIZE + ORIOLEDB_BLCKSZ,
						  ORIOLEDB_S3_PART_SIZE,
						  &dataSize);
	if (data)
	{
		res = s3_put_object_with_contents(objectname, data, dataSize, NULL, false);
		pfree(data);
	}

	return res;
}

/*
 * Get the file part from S3 object.
 */
void
s3_get_file_part(char *objectname, char *filename, int partnum)
{
	StringInfoData buf;

	initStringInfo(&buf);
	s3_get_object(objectname, &buf, false);

	write_file_part(filename,
					partnum * ORIOLEDB_S3_PART_SIZE + ORIOLEDB_BLCKSZ,
					buf.data,
					buf.len);

	pfree(buf.data);
}

/*
 * Put empty dir as S3 object.
 */
void
s3_put_empty_dir(char *objectname)
{
	s3_put_object_with_contents(objectname, NULL, 0, NULL, false);
}

/*
 * A SQL function to put object to S3.  Currently only used for debugging
 * purposes.
 */
Datum
s3_put(PG_FUNCTION_ARGS)
{
	char	   *objectname,
			   *filename;

	objectname = text_to_cstring(PG_GETARG_TEXT_PP(0));
	filename = text_to_cstring(PG_GETARG_TEXT_PP(1));

	s3_put_file(objectname, filename, false);

	PG_RETURN_NULL();
}
