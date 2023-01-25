Undo log storage
================

OrioleDB can store the undo log in files, but it also provides in-memory buffering.  Buffering is designed to provide the fastest access to the most recent undo records.  The total size of undo buffers is controlled by `orioledb.undo_buffers` GUC parameter.  This size is split into two halves, one for the circular buffer and another for block buffers.

OrioleDB keeps as many of undo log records as to fulfill the following requirements:

 * rollback any in-progress transactions;
 * serve any existing snapshots;
 * rollback any transactions during the checkpointing.

The picture below illustrates the filling of the undo circular buffer.  The retain location is the minimal position where we must keep undo records to fulfill the requirements.  The arrows between the undo records illustrate the insertion order, not the actual links.

![Undo circular buffer](images/undo_buffer_1.svg)

Once we reach the end of the circular buffer, we start from the beginning if the retain location allows us to do this without overwriting required undo records. Unless we need to retain too many undo records, we may store all of them in the circular buffer.

![Undo circular buffer overflow](images/undo_buffer_2.svg)

Once we need to add a new undo record, but the corresponding space is still occupied by retained undo records, we need to write some undo records to undo files.  The picture below shows undo log split between the circular buffer and undo file.  The records before written location are kept in undo files, and the records after written location are kept in the circular buffer.  If we don't need to retain much of undo log (for instance, some long-running transaction was finished), we may switch back to the storage of the whole undo log in the circular buffer.

![Undo log split](images/undo_buffer_3.svg)

The two pictures below explain how we write a chunk of undo records into the data file.  At first, we set the "write in-progress" location.  This means that undo records within the range [written location; write in-progress location) are about to be written to undo files.  During this period, undo records from this range can still be read from the circular buffer but can't be written (in some situations, we need to update existing undo records).

![Undo log write in-progress](images/undo_buffer_4.svg)

After the writing is finished, the written location is advanced, and the whole undo range is available for reading and writing.

![Undo log written](images/undo_buffer_5.svg)
