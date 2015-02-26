// Copyright 2012 Cloudera Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.


#ifndef IMPALA_EXEC_HDFS_SCAN_NODE_H_
#define IMPALA_EXEC_HDFS_SCAN_NODE_H_

#include <vector>
#include <memory>
#include <stdint.h>

<<<<<<< HEAD
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/recursive_mutex.hpp>
#include <boost/thread/thread.hpp>

#include <hdfs.h>

#include "exec/scan-node.h"
=======
#include <boost/unordered_map.hpp>
#include <boost/unordered_set.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/condition_variable.hpp>
#include <boost/thread/mutex.hpp>
#include <boost/thread/thread.hpp>

#include "exec/scan-node.h"
#include "exec/scanner-context.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/descriptors.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/string-buffer.h"
#include "util/progress-updater.h"
<<<<<<< HEAD
=======
#include "util/spinlock.h"
#include "util/thread.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

#include "gen-cpp/PlanNodes_types.h"

namespace impala {

<<<<<<< HEAD
class ByteStream;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class DescriptorTbl;
class HdfsScanner;
class RowBatch;
class Status;
<<<<<<< HEAD
class ScanRangeContext;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class Tuple;
class TPlanNode;
class TScanRange;

<<<<<<< HEAD
// Maintains per file information for files assigned to this scan node.  This includes 
// all the scan ranges for the file as well as a lock which can be used when updating 
// the file's metadata.
struct HdfsFileDesc {
  boost::mutex lock;
  std::string filename;
  std::vector<DiskIoMgr::ScanRange*> ranges;
  HdfsFileDesc(const std::string& filename) : filename(filename) {}
};

// A ScanNode implementation that is used for all tables read directly from 
// HDFS-serialised data. 
// A HdfsScanNode spawns multiple scanner threads to process the bytes in
// parallel.  There is a handshake between the scan node and the scanners 
// to get all the scan ranges queued and bytes processed.  
// 1. The scan node initially calls the Scanner with a list of files and ranges 
=======
// Maintains per file information for files assigned to this scan node.  This includes
// all the splits for the file. Note that it is not thread-safe.
struct HdfsFileDesc {
  std::string filename;

  // Length of the file. This is not related to which parts of the file have been
  // assigned to this node.
  int64_t file_length;

  THdfsCompression::type file_compression;

  // Splits (i.e. raw byte ranges) for this file, assigned to this scan node.
  std::vector<DiskIoMgr::ScanRange*> splits;
  HdfsFileDesc(const std::string& filename)
    : filename(filename), file_length(0), file_compression(THdfsCompression::NONE) {
  }
};

// Struct for additional metadata for scan ranges. This contains the partition id
// that this scan range is for.
struct ScanRangeMetadata {
  // The partition id that this range is part of.
  int64_t partition_id;

  ScanRangeMetadata(int64_t partition_id)
    : partition_id(partition_id) { }
};

// A ScanNode implementation that is used for all tables read directly from
// HDFS-serialised data.
// A HdfsScanNode spawns multiple scanner threads to process the bytes in
// parallel.  There is a handshake between the scan node and the scanners
// to get all the splits queued and bytes processed.
// 1. The scan node initially calls the Scanner with a list of files and splits
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
//    for that scanner/file format.
// 2. The scanner issues the initial byte ranges for each of those files.  For text
//    this is simply the entire range but for rc files, this would just be the header
//    byte range.  The scan node doesn't care either way.
<<<<<<< HEAD
// 3. Buffers for the issued ranges return and the scan node enqueues them to the
//    scanner's ready buffer queue.  
// 4. The scanner processes the buffers, issuing more byte ranges if necessary.
// 5. The scanner finishes the scan range and informs the scan node so it can track
//    end of stream.
// TODO: this class currently throttles ranges sent to the io mgr.  This should be
// updated to not have to do this once we can restrict the number of scanner threads
// a better way.
// TODO: this needs to be moved into the io mgr.  RegisterReader needs to take
// another argument for max parallel ranges or something like that.
// TODO: this class supports two types of scanners, TEXT and SEQUENCE which use
// the io mgr and RCFILE and TREVNI which don't.  Remove the non io mgr path.
=======
// 3. The scan node spins up a number of scanner threads. Each of those threads
//    pulls the next scan range to work on from the IoMgr and then processes the
//    range end to end.
// 4. The scanner processes the buffers, issuing more scan ranges if necessary.
// 5. The scanner finishes the scan range and informs the scan node so it can track
//    end of stream.
// TODO: this class allocates a bunch of small utility objects that should be
// recycled.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class HdfsScanNode : public ScanNode {
 public:
  HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode, const DescriptorTbl& descs);

  ~HdfsScanNode();

  // ExecNode methods
  virtual Status Prepare(RuntimeState* state);
  virtual Status Open(RuntimeState* state);
<<<<<<< HEAD

  // Checks for cancellation at the very beginning and then again after
  // each call to HdfsScanner::GetNext().
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);

  virtual Status Close(RuntimeState* state);

  // ScanNode methods
  virtual Status SetScanRanges(const std::vector<TScanRangeParams>& scan_ranges);

  // Methods for the scanners to use
  Status CreateConjuncts(std::vector<Expr*>* exprs);

  int limit() const { return limit_; }

  bool compact_data() const { return compact_data_; }
  
=======
  virtual Status GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos);
  virtual void Close(RuntimeState* state);

  int limit() const { return limit_; }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  const std::vector<SlotDescriptor*>& materialized_slots()
      const { return materialized_slots_; }

  // Returns the tuple idx into the row for this scan node to output to.
  // Currently this is always 0.
  int tuple_idx() const { return 0; }

  // Returns number of partition keys in the schema, including non-materialized slots
<<<<<<< HEAD
  int num_partition_keys() const { return num_partition_keys_; }
=======
  int num_partition_keys() const { return hdfs_table_->num_clustering_cols(); }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Returns number of materialized partition key slots
  int num_materialized_partition_keys() const { return partition_key_slots_.size(); }

<<<<<<< HEAD
  int num_cols() const { return column_idx_to_materialized_slot_idx_.size(); }
  
  const TupleDescriptor* tuple_desc() { return tuple_desc_; }

  hdfsFS hdfs_connection() { return hdfs_connection_; }

  RuntimeState* runtime_state() { return runtime_state_; }
 
  DiskIoMgr::ReaderContext* reader_context() { return reader_context_; }
=======
  // Number of columns, including partition keys
  int num_cols() const { return hdfs_table_->num_cols(); }

  const TupleDescriptor* tuple_desc() { return tuple_desc_; }

  const HdfsTableDescriptor* hdfs_table() { return hdfs_table_; }

  hdfsFS hdfs_connection() { return hdfs_connection_; }

  RuntimeState* runtime_state() { return runtime_state_; }

  DiskIoMgr::RequestContext* reader_context() { return reader_context_; }

  RuntimeProfile::HighWaterMarkCounter* max_compressed_text_file_length() {
    return max_compressed_text_file_length_;
  }

  const static int SKIP_COLUMN = -1;

  // Creates a clone of conjunct_ctxs_. 'ctxs' should be non-NULL and empty.
  // The returned contexts must be closed by the caller.
  Status GetConjunctCtxs(std::vector<ExprContext*>* ctxs);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Returns index into materialized_slots with 'col_idx'.  Returns SKIP_COLUMN if
  // that column is not materialized.
  int GetMaterializedSlotIdx(int col_idx) const {
    return column_idx_to_materialized_slot_idx_[col_idx];
  }

<<<<<<< HEAD
  // Returns the per format codegen'd function.  Returns NULL if codegen is not
  // possible.
  llvm::Function* GetCodegenFn(THdfsFileFormat::type);

  // Adds a materialized row batch for the scan node.  This is called from scanner
  // threads.
  void AddMaterializedRowBatch(RowBatch* row_batch);

  // Allocate a new scan range object.  This is thread safe.
  DiskIoMgr::ScanRange* AllocateScanRange(const char* file, int64_t len, int64_t offset,
      int64_t partition_id, int disk_id);

  // Adds a scan range to the disk io mgr queue.
  void AddDiskIoRange(DiskIoMgr::ScanRange* range);

  // Adds all ranges for file_desc to the io mgr queue.
  void AddDiskIoRange(const HdfsFileDesc* file_desc);

  // Scanners must call this when an entire file is queued.
  void FileQueued(const char* filename);

  // Allocates and initialises template_tuple_ with any values from
  // the partition columns for the current scan range
  Tuple* InitTemplateTuple(RuntimeState* state, const std::vector<Expr*>& expr_values);
=======
  // The result array is of length num_cols(). The i-th element is true iff column i
  // should be materialized.
  const bool* is_materialized_col() {
    return reinterpret_cast<const bool*>(&is_materialized_col_[0]);
  }

  // Returns the per format codegen'd function.  Scanners call this to get the
  // codegen'd function to use.  Returns NULL if codegen should not be used.
  void* GetCodegenFn(THdfsFileFormat::type);

  inline void IncNumScannersCodegenEnabled() {
    ++num_scanners_codegen_enabled_;
  }

  inline void IncNumScannersCodegenDisabled() {
    ++num_scanners_codegen_disabled_;
  }

  // Adds a materialized row batch for the scan node.  This is called from scanner
  // threads.
  // This function will block if materialized_row_batches_ is full.
  void AddMaterializedRowBatch(RowBatch* row_batch);

  // Allocate a new scan range object, stored in the runtime state's object pool.  For
  // scan ranges that correspond to the original hdfs splits, the partition id must be set
  // to the range's partition id. For other ranges (e.g. columns in parquet, read past
  // buffers), the partition_id is unused. expected_local should be true if this scan
  // range is not expected to require a remote read.
  // This is thread safe.
  DiskIoMgr::ScanRange* AllocateScanRange(const char* file, int64_t len, int64_t offset,
      int64_t partition_id, int disk_id, bool try_cache, bool expected_local);

  // Adds ranges to the io mgr queue and starts up new scanner threads if possible.
  Status AddDiskIoRanges(const std::vector<DiskIoMgr::ScanRange*>& ranges);

  // Adds all splits for file_desc to the io mgr queue.
  Status AddDiskIoRanges(const HdfsFileDesc* file_desc);

  // Indicates that this file_desc's scan ranges have all been issued to the IoMgr.
  // For each file, the scanner must call MarkFileDescIssued() or AddDiskIoRanges().
  // Issuing ranges happens asynchronously. For many of the file formats we synchronously
  // issue the file header/footer in Open() but the rest of the splits for the file are
  // issued asynchronously.
  void MarkFileDescIssued(const HdfsFileDesc* file_desc);

  // Allocates and initialises template_tuple_ with any values from
  // the partition columns for the current scan range
  // Returns NULL if there are no materialized partition keys.
  // TODO: cache the tuple template in the partition object.
  Tuple* InitTemplateTuple(RuntimeState* state,
                           const std::vector<ExprContext*>& value_ctxs);

  // Allocates and return an empty template tuple (i.e. with no values filled in).
  // Scanners can use this method to initialize a template tuple even if there are no
  // materialized partition keys (e.g. to hold Avro default values).
  Tuple* InitEmptyTemplateTuple();

  // Acquires all allocations from pool into scan_node_pool_. Thread-safe.
  void TransferToScanNodePool(MemPool* pool);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Returns the file desc for 'filename'.  Returns NULL if filename is invalid.
  HdfsFileDesc* GetFileDesc(const std::string& filename);

  // Gets scanner specific metadata for 'filename'.  Scanners can use this to store
  // file header information.
  // Returns NULL if there is no metadata.
  // This is thread safe.
  void* GetFileMetadata(const std::string& filename);

  // Sets the scanner specific metadata for 'filename'.
  // This is thread safe.
  void SetFileMetadata(const std::string& filename, void* metadata);

<<<<<<< HEAD
  // Called by the scanner when a range is complete.  Used to log progress.
  void RangeComplete();
=======
  // Called by the scanner when a range is complete.  Used to trigger done_ and
  // to log progress.  This *must* only be called after the scanner has completely
  // finished the scan range (i.e. context->Flush()).
  void RangeComplete(const THdfsFileFormat::type& file_type,
      const THdfsCompression::type& compression_type);
  // Same as above except for when multiple compression codecs were used
  // in the file. The metrics are incremented for each compression_type.
  void RangeComplete(const THdfsFileFormat::type& file_type,
      const std::vector<THdfsCompression::type>& compression_type);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Utility function to compute the order in which to materialize slots to allow  for
  // computing conjuncts as slots get materialized (on partial tuples).
  // 'order' will contain for each slot, the first conjunct it is associated with.
  // e.g. order[2] = 1 indicates materialized_slots[2] must be materialized before
  // evaluating conjuncts[1].  Slots that are not referenced by any conjuncts will have
  // order set to conjuncts.size()
  void ComputeSlotMaterializationOrder(std::vector<int>* order) const;
<<<<<<< HEAD
  
  const static int SKIP_COLUMN = -1;

 private:
=======

  // map from volume id to <number of split, per volume split lengths>
  typedef boost::unordered_map<int32_t, std::pair<int, int64_t> > PerVolumnStats;

  // Update the per volume stats with the given scan range params list
  static void UpdateHdfsSplitStats(
      const std::vector<TScanRangeParams>& scan_range_params_list,
      PerVolumnStats* per_volume_stats);

  // Output the per_volume_stats to stringstream. The output format is a list of:
  // <volume id>:<# splits>/<per volume split lengths>
  static void PrintHdfsSplitStats(const PerVolumnStats& per_volume_stats,
      std::stringstream* ss);

  // Description string for the per volume stats output.
  static const std::string HDFS_SPLIT_STATS_DESC;

 private:
  friend class ScannerContext;

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Cache of the plan node.  This is needed to be able to create a copy of
  // the conjuncts per scanner since our Exprs are not thread safe.
  boost::scoped_ptr<TPlanNode> thrift_plan_node_;

  RuntimeState* runtime_state_;

  // Tuple id resolved in Prepare() to set tuple_desc_;
<<<<<<< HEAD
  int tuple_id_;

  // Copy strings to tuple memory pool if true.
  // We try to avoid the overhead copying strings if the data will just
  // stream to another node that will release the memory.
  bool compact_data_;

  // ReaderContext object to use with the disk-io-mgr
  DiskIoMgr::ReaderContext* reader_context_;
=======
  const int tuple_id_;

  // RequestContext object to use with the disk-io-mgr for reads.
  DiskIoMgr::RequestContext* reader_context_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Descriptor for tuples this scan node constructs
  const TupleDescriptor* tuple_desc_;

  // Descriptor for the hdfs table, including partition and format metadata.
  // Set in Prepare, owned by RuntimeState
  const HdfsTableDescriptor* hdfs_table_;

  // If true, the warning that some disk ids are unknown was logged.  Only log
  // this once per scan node since it can be noisy.
  bool unknown_disk_id_warned_;

<<<<<<< HEAD
  // Mem pool for tuple buffer data. Used by scanners for allocation,
  // but owned here.
  boost::scoped_ptr<MemPool> tuple_pool_;

  // Files and their scan ranges
  typedef std::map<std::string, HdfsFileDesc*> ScanRangeMap;
  ScanRangeMap per_file_scan_ranges_;
  
  // Points at the (file, [scan ranges]) pair currently being processed
  ScanRangeMap::iterator current_file_scan_ranges_;

  // The index of the current scan range in the current file's scan range list
  int current_range_idx_;

  // Number of files that have not been issued from the scanners.
  int num_unqueued_files_;
=======
  // Partitions scanned by this scan node.
  boost::unordered_set<int64_t> partition_ids_;

  // File path => file descriptor (which includes the file's splits)
  typedef std::map<std::string, HdfsFileDesc*> FileDescMap;
  FileDescMap file_descs_;

  // File format => file descriptors.
  typedef std::map<THdfsFileFormat::type, std::vector<HdfsFileDesc*> > FileFormatsMap;
  FileFormatsMap per_type_files_;

  // Set to true when the initial scan ranges are issued to the IoMgr. This happens
  // on the first call to GetNext().
  bool initial_ranges_issued_;

  // The estimated memory required to start up a new scanner thread. If the memory
  // left (due to limits) is less than this value, we won't start up optional
  // scanner threads.
  int64_t scanner_thread_bytes_required_;

  // Number of files that have not been issued from the scanners.
  AtomicInt<int> num_unqueued_files_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Connection to hdfs, established in Open() and closed in Close().
  hdfsFS hdfs_connection_;

  // Map of HdfsScanner objects to file types.  Only one scanner object will be
<<<<<<< HEAD
  // created for each file type.  Objects stored in scanner_pool_.
  typedef std::map<THdfsFileFormat::type, HdfsScanner*> ScannerMap;
  ScannerMap scanner_map_;

  // Per scanner type codegen'd fn.  This is written to by the main thread and only
  // read from scanner threads so does not need locks.
  typedef std::map<THdfsFileFormat::type, llvm::Function*> CodegendFnMap;
  CodegendFnMap codegend_fn_map_;

  // Pool for storing allocated scanner objects.  We don't want to use the 
  // runtime pool to ensure that the scanner objects are deleted before this
  // object is.
  boost::scoped_ptr<ObjectPool> scanner_pool_;

  // The scanner in use for the current file / scan-range
  // combination.
  HdfsScanner* current_scanner_;

  // The source of byte data for consumption by the scanner for the
  // current file / scan-range combination.
  boost::scoped_ptr<ByteStream> current_byte_stream_;

  // Total number of partition slot descriptors, including non-materialized ones.
  int num_partition_keys_;

  // Vector containing indices into materialized_slots_.  The vector is indexed by
  // the slot_desc's col_pos.  Non-materialized slots and partition key slots will 
  // have SKIP_COLUMN as its entry.
  std::vector<int> column_idx_to_materialized_slot_idx_;
  
  // Vector containing slot descriptors for all materialized non-partition key
  // slots.  These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> materialized_slots_;

  // Vector containing slot descriptors for all materialized partition key slots  
  // These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> partition_key_slots_;

  // Thread that constantly reads from the disk io mgr and queues the work on the
  // context for that scan range.
  boost::scoped_ptr<boost::thread> disk_read_thread_;
 
  // Maximum number of simultaneous scanner threads to use.  The actual number of 
  // simultaneous scanner threads might be much less.  We have one scanner thread 
  // per scan range and the io mgr also throttles the number of scan ranges in flight.  
  // In general, for best performance, we should rely on the io mgr settings and should 
  // not throttle the query with this value.  In this case, this value is set
  // to the total number of scan ranges assigned to this node.
  // This setting should only be used to slow down the query (either for debugging or to 
  // share resources better before we use c-groups).
  int max_scanner_threads_;

  // Keeps track of total scan ranges and the number finished.
  ProgressUpdater progress_;

  // Scanner specific per file metadata (e.g. header information) and associated lock.
=======
  // created for each file type.  Objects stored in runtime_state's pool.
  typedef std::map<THdfsFileFormat::type, HdfsScanner*> ScannerMap;
  ScannerMap scanner_map_;

  // Per scanner type codegen'd fn.
  typedef std::map<THdfsFileFormat::type, void*> CodegendFnMap;
  CodegendFnMap codegend_fn_map_;

  // Contexts for each conjunct. These are cloned by the scanners so conjuncts can be
  // safely evaluated in parallel.
  std::vector<ExprContext*> conjunct_ctxs_;

  // Vector containing indices into materialized_slots_.  The vector is indexed by
  // the slot_desc's col_pos.  Non-materialized slots and partition key slots will
  // have SKIP_COLUMN as its entry.
  std::vector<int> column_idx_to_materialized_slot_idx_;

  // is_materialized_col_[i] = <true i-th column should be materialized, false otherwise>
  // for 0 <= i < total # columns
  //
  // This should be a vector<bool>, but bool vectors are special-cased and not stored
  // internally as arrays, so instead we store as chars and cast to bools as needed
  std::vector<char> is_materialized_col_;

  // Vector containing slot descriptors for all materialized non-partition key
  // slots.  These descriptors are sorted in order of increasing col_pos
  // TODO: Put this (with associated fields and logic) on ScanNode or ExecNode
  std::vector<SlotDescriptor*> materialized_slots_;

  // Vector containing slot descriptors for all materialized partition key slots
  // These descriptors are sorted in order of increasing col_pos
  std::vector<SlotDescriptor*> partition_key_slots_;

  // Keeps track of total splits and the number finished.
  ProgressUpdater progress_;

  // Scanner specific per file metadata (e.g. header information) and associated lock.
  // This lock cannot be taken together with any other locks except lock_.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  boost::mutex metadata_lock_;
  std::map<std::string, void*> per_file_metadata_;

  // Thread group for all scanner worker threads
<<<<<<< HEAD
  boost::thread_group scanner_threads_;

  // Lock and condition variable protecting materialized_row_batches_.  Row batches
  // are produced by the scanner threads and consumed by the main thread in GetNext
  // Lock to protect materialized_row_batches_
  boost::mutex row_batches_lock_;
  boost::condition_variable row_batch_added_cv_;
  std::list<RowBatch*> materialized_row_batches_;
=======
  ThreadGroup scanner_threads_;

  // Outgoing row batches queue. Row batches are produced asynchronously by the scanner
  // threads and consumed by the main thread.
  boost::scoped_ptr<RowBatchQueue> materialized_row_batches_;

  // Maximum size of materialized_row_batches_.
  int max_materialized_row_batches_;

  // This is the number of io buffers that are owned by the scan node and the scanners.
  // This is used just to help debug leaked io buffers to determine if the leak is
  // happening in the scanners vs other parts of the execution.
  AtomicInt<int> num_owned_io_buffers_;

  // The number of times a token was offered but no scanner threads started.
  // This is used for diagnostics only.
  AtomicInt<int> num_skipped_tokens_;

  // Counters which track the number of scanners that have codegen enabled for the
  // materialize and conjuncts evaluation code paths.
  AtomicInt<int> num_scanners_codegen_enabled_;
  AtomicInt<int> num_scanners_codegen_disabled_;

  // The size of the largest compressed text file to be scanned. This is used to estimate
  // scanner thread memory usage.
  RuntimeProfile::HighWaterMarkCounter* max_compressed_text_file_length_;

  // Disk accessed bitmap
  RuntimeProfile::Counter disks_accessed_bitmap_;

  // Total number of bytes read locally
  RuntimeProfile::Counter* bytes_read_local_;

  // Total number of bytes read via short circuit read
  RuntimeProfile::Counter* bytes_read_short_circuit_;

  // Total number of bytes read from data node cache
  RuntimeProfile::Counter* bytes_read_dn_cache_;

  // Total number of remote scan ranges
  RuntimeProfile::Counter* num_remote_ranges_;

  // Total number of bytes read remotely that were expected to be local
  RuntimeProfile::Counter* unexpected_remote_bytes_;

  // Lock protects access between scanner thread and main query thread (the one calling
  // GetNext()) for all fields below.  If this lock and any other locks needs to be taken
  // together, this lock must be taken first.
  boost::mutex lock_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Flag signaling that all scanner threads are done.  This could be because they
  // are finished, an error/cancellation occurred, or the limit was reached.
  // Setting this to true triggers the scanner threads to clean up.
<<<<<<< HEAD
  bool done_;

  // Lock protects access between scanner thread and main query thread (the one calling
  // GetNext()) for all fields below.  If this lock and one of the condition variable
  // locks needs to be take together, this lock must be taken first.
  // This lock is recursive since some of the functions provided to the scanner are
  // also called from internal functions.
  // TODO: we can split out 'external' functions for internal functions and make this
  // lock non-recursive.
  boost::recursive_mutex lock_;

  // Pool for allocating partition key tuple and string buffers
  boost::scoped_ptr<MemPool> partition_key_pool_;

  // The queue of all ranges that the scanners want to issue.
  std::vector<DiskIoMgr::ScanRange*> all_ranges_;

  // The next idx (in all_ranges_) that should be issued.
  int next_range_to_issue_idx_;

  // If true, all ranges have been queued by the scanners.  They haven't necessarily
  // made it to the io mgr yet though.
  bool all_ranges_in_queue_;
  
  // The number of ranges in flight in the io mgr.
  int ranges_in_flight_;

  // If true, all ranges have been sent to the io mgr.
  bool all_ranges_issued_;

  // Mapping of hdfs scan range to the scan range context for processing it.  
  typedef std::map<const DiskIoMgr::ScanRange*, ScanRangeContext*> ContextMap;
  ContextMap contexts_;

  // Status of failed operations.  This is set asynchronously in DiskThread and
  // ScannerThread.  Returned in GetNext() if an error occurred.  An non-ok
  // status triggers cleanup of the disk and scanner threads.
  Status status_;

  // Issue the next set of queued ranges to the io mgr.  This is used to throttle
  // the number of scan ranges being parsed to the number of scanner threads.
  Status IssueMoreRanges();
  
  // Called once per scan-range to initialise (potentially) a new byte
  // stream and to call the same method on the current scanner.
  Status InitNextScanRange(RuntimeState* state, bool* scan_ranges_finished);

  // Gets a scanner for the file type for this partition, creating one if none
  // exists.
  // TODO: remove when all scanners are updated.
  HdfsScanner* GetScanner(HdfsPartitionDescriptor*);

  // Create a new scanner for this partition type and initialize it.
  HdfsScanner* CreateScanner(HdfsPartitionDescriptor*);

  // Main function for disk thread which reads from the io mgr and pushes read
  // buffers to the scan range context.  This thread spawns new scanner threads
  // for each new context.
  void DiskThread();

  // Start a new scanner thread for this range with the initial 'buffer'.
  void StartNewScannerThread(DiskIoMgr::BufferDescriptor* buffer);

  // Main function for scanner thread.  This simply delegates to the scanner
  // to process the range.  This thread terminates when the scan range is complete
  // or an error occurred.
  void ScannerThread(HdfsScanner* scanner, ScanRangeContext*);
=======
  // This should not be explicitly set. Instead, call SetDone().
  bool done_;

  // Set to true if all ranges have started. Some of the ranges may still be in
  // flight being processed by scanner threads, but no new ScannerThreads
  // should be started.
  bool all_ranges_started_;

  // Pool for allocating some amounts of memory that is shared between scanners.
  // e.g. partition key tuple and their string buffers
  boost::scoped_ptr<MemPool> scan_node_pool_;

  // Status of failed operations.  This is set in the ScannerThreads
  // Returned in GetNext() if an error occurred.  An non-ok status triggers cleanup
  // scanner threads.
  Status status_;

  // Mapping of file formats (file type, compression type) to the number of
  // splits of that type and the lock protecting it.
  // This lock cannot be taken together with any other locks except lock_.
  SpinLock file_type_counts_lock_;
  typedef std::map<
      std::pair<THdfsFileFormat::type, THdfsCompression::type>, int> FileTypeCountsMap;
  FileTypeCountsMap file_type_counts_;

  // If true, counters are actively running and need to be reported in the runtime
  // profile.
  bool counters_running_;

  // The id of the callback added to the query resource manager when RM is enabled. Used
  // to remove the callback before this scan node is destroyed.
  int32_t rm_callback_id_;

  // Called when scanner threads are available for this scan node. This will
  // try to spin up as many scanner threads as the quota allows.
  // This is also called whenever a new range is added to the IoMgr to 'pull'
  // thread tokens if they are available.
  void ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool);

  // Create and prepare new scanner for this partition type.
  // If the scanner cannot be created, return NULL.
  HdfsScanner* CreateAndPrepareScanner(HdfsPartitionDescriptor* partition_desc,
      ScannerContext* context, Status* status);

  // Main function for scanner thread. This thread pulls the next range to be
  // processed from the IoMgr and then processes the entire range end to end.
  // This thread terminates when all scan ranges are complete or an error occurred.
  void ScannerThread();

  // Returns true if there is enough memory (against the mem tracker limits) to
  // have a scanner thread.
  // If new_thread is true, the calculation is for starting a new scanner thread.
  // If false, it determines whether there's adequate memory for the existing
  // set of scanner threads.
  // lock_ must be taken before calling this.
  bool EnoughMemoryForScannerThread(bool new_thread);

  // Checks for eos conditions and returns batches from materialized_row_batches_.
  Status GetNextInternal(RuntimeState* state, RowBatch* row_batch, bool* eos);

  // sets done_ to true and triggers threads to cleanup. Cannot be calld with
  // any locks taken. Calling it repeatedly ignores subsequent calls.
  void SetDone();

  // Stops periodic counters and aggregates counter values for the entire scan node.
  // This should be called as soon as the scan node is complete to get the most accurate
  // counter values.
  // This can be called multiple times, subsequent calls will be ignored.
  // This must be called on Close() to unregister counters.
  void StopAndFinalizeCounters();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

}

#endif
