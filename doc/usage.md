OrioleDB usage
==============

OrioleDB is implemented using the table access method interface.  Therefore, to define a table using the OrioleDB engine, one should specify `USING orioledb;` in the `CREATE TABLE` command.

OrioleDB uses index-organized tables.  So, the selection of the primary key is a very critical decision affecting performance.  If you specify no primary key, then a hidden surrogate primary key will be created over the virtual `ctid` column.

Example
-------

Let's define the `blog_post` table, which stores blog posts and has two indices: primary key on the `id` column and secondary key by `published_at` column.


```sql
CREATE TABLE blog_post
(
    id int8 NOT NULL,
    title text NOT NULL,
    body text NOT NULL,
    author text NOT NULL,
    published_at timestamp  NOT NULL,
    views bigint NOT NULL,
    PRIMARY KEY(id)
) USING orioledb;
CREATE INDEX blog_post_published_at ON blog_post(published_at);
```

The created table could be used in regular DML queries, including SELECT/INSERT/UPDATE/DELETE and INSERT ON CONFLICT.

Plans of queries involving OrioleDB tables could be viewed using `EXPLAIN` clause as usual.  OrioleDB uses Custom Scan nodes named o_scan for scanning tables.

```sql
# EXPLAIN SELECT * FROM blog_post ORDER BY published_at DESC LIMIT 10;
                                   QUERY PLAN
-----------------------------------------------------------------------
 Limit  (cost=0.15..1.67 rows=10 width=120)
   ->  Custom Scan (o_scan) on blog_post  (cost=0.15..48.95 rows=320
           width=120)
         Backward index scan of: blog_post_published_at
(3 rows)
```

```sql
# EXPLAIN SELECT * FROM blog_post WHERE id = 1;
                              QUERY PLAN
-----------------------------------------------------------------------
 Custom Scan (o_scan) on blog_post  (cost=0.15..8.17 rows=1 width=120)
   Forward index only scan of: blog_post_pkey
   Conds: (id = 1)
(3 rows)
```

`EXPLAIN (ANALYZE, BUFFERS)` clause allows to view page access statistics.

```sql
# EXPLAIN (ANALYZE, BUFFERS)
  SELECT * FROM blog_post ORDER BY published_at DESC LIMIT 10;
                                                        QUERY PLAN
----------------------------------------------------------------------
 Limit  (cost=0.15..1.64 rows=10 width=120) (actual time=0.024..0.044
     rows=10 loops=1)
   ->  Custom Scan (o_scan) on blog_post  (cost=0.15..66.87 rows=448
           width=120) (actual time=0.022..0.038 rows=20 loops=1)
         Backward index scan of: blog_post_published_at
         Primary pages: read=20
         Secondary index (blog_post_published_at) pages: read=2
 Planning:
   Buffers: shared hit=2
 Planning Time: 0.175 ms
 Execution Time: 0.079 ms
(9 rows)
```

Block-level data compression
----------------------------

OrioleDB implements block-level compression.  The following options control the compression level of a table:

 * `compress` – default compression level for all table data structures,
 * `primary_compress` – compression level for the table primary key,
 * `toast_compress` – compression level for the table TOASTed values.

Individual indexes also have the `compress` option, which controls the compression level of a particular index overriding the value of the table `compress` option.

Each of the options above should have integer values from `-1` to `22`.  The value of `-1` means no compression (default), values between 0 and 22 specified compression levels of zstd library.

*Example*

```sql
CREATE TABLE compression_test
(
  id int8 NOT NULL,
  value1 float8 NOT NULL,
  value2 text NOT NULL,
  PRIMARY KEY(id)
)  USING orioledb
   WITH (compress = 5, toast_compress = 10, primary_compress = -1);

CREATE INDEX compression_test_value1_idx ON compression_test(value1)
    WITH (compress = 22)
CREATE INDEX compression_test_value2_idx ON compression_test(value2);
```

In this example primary key of `compression_test` table is uncompressed, TOAST values are compressed with level of `10`, `compression_test_value1_idx` index is compressed with level of `22`, index `compression_test_value2_idx` is compressed with level of `5`.

Current limitations
-------------------

OrioleDB is currently in the development stage.  Therefore it has the following temporary limitations.

 1. `pg_rewind` copies OrioleDB tables completely. Shortly OrioleDB will implement incremental copying of OrioleDB tables using `pg_rewind`.
 2. OrioleDB supports parallel sequential scan, but not other types of scan.
 3. OrioleDB doesn't support prepared transactions.
 4. OrioleDB supports just B-tree indexes.  OrioleDB roadmap contains the implementation of analogs of GiST, GIN, and BRIN.
 5. OrioleDB supports bitmap scan only for int4, int8 and ctid primary keys.
 6. Row-level concurrency in OrioleDB has some [differences](row_concurrency.md).
 7. OrioleDB doesn't support `CLUSTER` and `VACUUM FULL` commands yet, because we doesn't implement rewrite of the tables for these commands. And also `CLUSTER` doesn't really makes much sense for index-organized tables.
 8. `REINDEX CONCURRENTLY` now is not supported.
 9. OrioleDB tables doesn't support `Sample Scans` yet.

Data deletion
-------------

OrioleDB automatically merges sparse pages.  Therefore, when many rows are deleted, data pages are freed and available for future usage.  Data files aren't currently shrunk in such a situation, but that would be implemented soon.

Checkpoints, WAL & recovery
---------------------------

OrioleDB has its own recovery mechanism: copy-on-write checkpoints and row-level WAL.  However, both OrioleDB's checkpoints and WAL are integrated into PostgreSQL.  PostgreSQL checkpointer process handles OrioleDB's tables as well.  PostgreSQL WAL stream contains both WAL-records of built-in PostgreSQL tables and row-level WAL-records of OrioleDB's tables.

Recovery using row-level WAL records might require significant CPU resources.  Therefore parallel recovery of OrioleDB's tables is implemented.  OrioleDB launches its own pool of recovery workers, each of them responsible for replaying a particular part of WAL records.

OrioleDB has its own pool background writer processes (the `orioledb.bgwriter_num_workers` GUC parameter defines the pool size).  Usage of multiple background writers increases the effectiveness of IO-utilization on modern hardware.

Experimental support of the block devices
-----------------------------------------

OrioleDB implements experimental support of direct interaction with block devices mode.  This mode removes the overhead of the filesystem.

In this mode, the main part of table data is stored in the filesystem, but small metadata is still stored in the data directory.

The current implementation of block devices support contains memory leaks, resulting in the error message `device file overflow` even if the actual data size is much less than block device size. In this case, only the re-initialization of the data directory could help.

We plan to fix memory leaks soon and develop tools for monitoring free block device space.

In order to activate block device mode, one should specify `orioledb.device_filename` and `orioledb.device_length` GUC parameters. When the `orioledb.use_mmap` GUC parameter is enabled, the block device is connected using `mmap`.  This mode is optimal for NVRAM, which directly connects to the data bus. `mmap` mode is not recommended for regular devices because the current `mmap` implementation in Linux has very bug concurrency.

Settings
--------

 * `orioledb.main_buffers` -- the size of shared memory, where hot data pages of OrioleDB tables are cached.  This parameter is analog of the built-in `shared_buffers` GUC parameter. Default is `64 MB`.
 * `orioledb.free_tree_buffers` -- shared memory size for metadata of block allocators for compressed tables. The default is `8 MB`. We recommend increasing the value of this parameter to work with large compressed tables.
 * `orioledb.catalog_buffers` -- shared memory size of table metadata. The default value is `8 MB`. We recommend increasing the value of this parameter to work with a large number of tables.
 * `orioledb.undo_buffers` -- the shared memory ring buffer size for older versions of rows and pages.  The default is `1 MB`.
 * `orioledb.recovery_pool_size` -- the number of recovery workers row-level WAL based recovery. The default is 3.  We recommend increasing the value of this parameter for the systems with a large number of CPU cores.
 * `orioledb.recovery_queue_size` -- the size of shared memory for message queues related to recovery workers. The default is `8 MB`.
 * `orioledb.checkpoint_completion_ratio` -- the fraction of OrioleDB tables checkpoint time within the whole checkpoint time.  The default is `0.5`.  We recommend setting this value to `1.0` if only OrioleDB tables are used.
 * `orioledb.bgwriter_num_workers` -- the number background writer processes, which flushes dirty pages of OrioleDB tables in background. We recommend setting values greater than `1` for the systems with a large number of CPU cores.  The default is `1`.
 * `orioledb.max_io_concurrency` -- maximum number of concurrent IO operations issued by OrioleDB in parallel. We recommend setting this parameter when the OS kernel becomes a bottleneck for high concurrent IO. The default is `0` (off).
 * `orioledb.device_filename` -- path to the block device for block device mode. Not set by default.
 * `orioledb.device_length` -- the length of the block device.  The default is `1 GB`.
 * `orioledb.use_mmap` -- specify whether use `mmap` to work with the block device.  It could be `on` and `off`.  We recommend setting `on` value for NVRAM.  The default is `off`.

All the GUC parameters above require the postmaster restart.
