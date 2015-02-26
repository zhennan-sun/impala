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


#ifndef IMPALA_RUNTIME_RUNTIME_STATE_H
#define IMPALA_RUNTIME_RUNTIME_STATE_H

// needed for scoped_ptr to work on ObjectPool
#include "common/object-pool.h"

#include <boost/scoped_ptr.hpp>
<<<<<<< HEAD
=======
#include <boost/shared_ptr.hpp>
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include <boost/thread/locks.hpp>
#include <boost/thread/mutex.hpp>
#include <vector>
#include <string>
// stringstream is a typedef, so can't forward declare it.
#include <sstream>

<<<<<<< HEAD
#include "runtime/exec-env.h"
=======
#include "statestore/query-resource-mgr.h"
#include "runtime/exec-env.h"
#include "runtime/descriptors.h"  // for PlanNodeId
#include "runtime/disk-io-mgr.h"  // for DiskIoMgr::RequestContext
#include "runtime/mem-tracker.h"
#include "runtime/thread-resource-mgr.h"
#include "gen-cpp/PlanNodes_types.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "gen-cpp/Types_types.h"  // for TUniqueId
#include "gen-cpp/ImpalaInternalService_types.h"  // for TQueryOptions
#include "util/runtime-profile.h"

namespace impala {

<<<<<<< HEAD
class DescriptorTbl;
class DiskIoMgr;
=======
class Bitmap;
class BufferedBlockMgr;
class DescriptorTbl;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class ObjectPool;
class Status;
class ExecEnv;
class Expr;
class LlvmCodeGen;
class TimestampValue;
<<<<<<< HEAD
=======
class DataStreamRecvr;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

// Counts how many rows an INSERT query has added to a particular partition
// (partitions are identified by their partition keys: k1=v1/k2=v2
// etc. Unpartitioned tables have a single 'default' partition which is
<<<<<<< HEAD
// identified by the empty string.
typedef std::map<std::string, int64_t> PartitionRowCount;
=======
// identified by ROOT_PARTITION_KEY.
typedef std::map<std::string, TInsertPartitionStatus> PartitionStatusMap;

// Stats per partition for insert queries. They key is the same as for PartitionRowCount
typedef std::map<std::string, TInsertStats> PartitionInsertStats;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

// Tracks files to move from a temporary (key) to a final destination (value) as
// part of query finalization. If the destination is empty, the file is to be
// deleted.
typedef std::map<std::string, std::string> FileMoveMap;

// A collection of items that are part of the global state of a
// query and shared across all execution nodes of that query.
class RuntimeState {
 public:
<<<<<<< HEAD
  RuntimeState(const TUniqueId& fragment_instance_id,
               const TQueryOptions& query_options,
               const std::string& now, ExecEnv* exec_env);

  // RuntimeState for executing queries w/o a from clause.
  RuntimeState();
  // Empty d'tor to avoid issues with scoped_ptr.
  ~RuntimeState();

  // Set per-query state.
  Status Init(const TUniqueId& fragment_instance_id,
              const TQueryOptions& query_options,
              const std::string& now, ExecEnv* exec_env);
=======
  RuntimeState(const TPlanFragmentInstanceCtx& fragment_instance_ctx,
      const std::string& cgroup, ExecEnv* exec_env);

  // RuntimeState for executing expr in fe-support.
  RuntimeState(const TQueryCtx& query_ctx);

  // Empty d'tor to avoid issues with scoped_ptr.
  ~RuntimeState();

  // Set up five-level hierarchy of mem trackers: process, pool, query, fragment
  // instance. The instance tracker is tied to our profile. Specific parts of the
  // fragment (i.e. exec nodes, sinks, data stream senders, etc) will add a fifth level
  // when they are initialized. This function also initializes a user function mem
  // tracker (in the fifth level). If 'request_pool' is null, no request pool mem
  // tracker is set up, i.e. query pools will have the process mem pool as the parent.
  void InitMemTrackers(const TUniqueId& query_id, const std::string* request_pool,
      int64_t query_bytes_limit, int64_t query_rm_reservation_limit_bytes = -1);

  // Gets/Creates the query wide block mgr.
  Status CreateBlockMgr();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  ObjectPool* obj_pool() const { return obj_pool_.get(); }
  const DescriptorTbl& desc_tbl() const { return *desc_tbl_; }
  void set_desc_tbl(DescriptorTbl* desc_tbl) { desc_tbl_ = desc_tbl; }
<<<<<<< HEAD
  int batch_size() const { return query_options_.batch_size; }
  bool abort_on_error() const { return query_options_.abort_on_error; }
  int max_errors() const { return query_options_.max_errors; }
  int max_io_buffers() const { return query_options_.max_io_buffers; }
  int num_scanner_threads() const { return query_options_.num_scanner_threads; }
=======
  const TQueryOptions& query_options() const {
    return query_ctx().request.query_options;
  }
  int batch_size() const { return query_ctx().request.query_options.batch_size; }
  bool abort_on_error() const {
    return query_ctx().request.query_options.abort_on_error;
  }
  bool abort_on_default_limit_exceeded() const {
    return query_ctx().request.query_options.abort_on_default_limit_exceeded;
  }
  int max_errors() const { return query_options().max_errors; }
  const TQueryCtx& query_ctx() const { return fragment_instance_ctx_.query_ctx; }
  const TPlanFragmentInstanceCtx& fragment_ctx() const { return fragment_instance_ctx_; }
  const std::string& effective_user() const {
    if (query_ctx().session.__isset.delegated_user &&
        !query_ctx().session.delegated_user.empty()) {
      return do_as_user();
    }
    return connected_user();
  }
  const std::string& do_as_user() const { return query_ctx().session.delegated_user; }
  const std::string& connected_user() const {
    return query_ctx().session.connected_user;
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  const TimestampValue* now() const { return now_.get(); }
  void set_now(const TimestampValue* now);
  const std::vector<std::string>& error_log() const { return error_log_; }
  const std::vector<std::pair<std::string, int> >& file_errors() const {
    return file_errors_;
  }
<<<<<<< HEAD
  const TUniqueId& fragment_instance_id() const { return fragment_instance_id_; }
  ExecEnv* exec_env() { return exec_env_; }
  DataStreamMgr* stream_mgr() { return exec_env_->stream_mgr(); }
  HdfsFsCache* fs_cache() { return exec_env_->fs_cache(); }
  HBaseTableCache* htable_cache() { return exec_env_->htable_cache(); }
  DiskIoMgr* io_mgr() { return exec_env_->disk_io_mgr(); }

  FileMoveMap* hdfs_files_to_move() { return &hdfs_files_to_move_; }
  PartitionRowCount* num_appended_rows() { return &num_appended_rows_; }
=======
  const TUniqueId& query_id() const { return query_ctx().query_id; }
  const TUniqueId& fragment_instance_id() const {
    return fragment_instance_ctx_.fragment_instance_id;
  }
  const std::string& cgroup() const { return cgroup_; }
  ExecEnv* exec_env() { return exec_env_; }
  DataStreamMgr* stream_mgr() { return exec_env_->stream_mgr(); }
  HBaseTableFactory* htable_factory() { return exec_env_->htable_factory(); }
  ImpalaInternalServiceClientCache* impalad_client_cache() {
    return exec_env_->impalad_client_cache();
  }
  CatalogServiceClientCache* catalogd_client_cache() {
    return exec_env_->catalogd_client_cache();
  }
  DiskIoMgr* io_mgr() { return exec_env_->disk_io_mgr(); }
  MemTracker* instance_mem_tracker() { return instance_mem_tracker_.get(); }
  MemTracker* query_mem_tracker() { return query_mem_tracker_.get(); }
  ThreadResourceMgr::ResourcePool* resource_pool() { return resource_pool_; }

  FileMoveMap* hdfs_files_to_move() { return &hdfs_files_to_move_; }
  std::vector<DiskIoMgr::RequestContext*>* reader_contexts() { return &reader_contexts_; }
  void set_fragment_root_id(PlanNodeId id) {
    DCHECK_EQ(root_node_id_, -1) << "Should not set this twice.";
    root_node_id_ = id;
  }

  // The seed value to use when hashing tuples.
  // See comment on root_node_id_. We add one to prevent having a hash seed of 0.
  uint32_t fragment_hash_seed() const { return root_node_id_ + 1; }

  // Size to use when building bitmap filters. This is a prime number which reduces
  // collisions and the resulting bitmap is just under 4Kb.
  // Having all bitmaps be the same size allows us to combine (i.e. AND) bitmaps.
  uint32_t slot_filter_bitmap_size() const { return 32213; }

  // Adds a bitmap filter on slot 'slot'. If hash(slot) % bitmap.Size() is false, this
  // value can be filtered out. Multiple bitmap filters can be added to a single slot.
  // If it is the first call to add a bitmap filter for the specific slot, indicated by
  // 'acquired_ownership', then the passed bitmap should not be deleted by the caller.
  // Thread safe.
  void AddBitmapFilter(SlotId slot, Bitmap* bitmap, bool* acquired_ownership);

  // Returns bitmap filter on 'slot'. Returns NULL if there are no bitmap filters on this
  // slot.
  // It is not safe to concurrently call AddBitmapFilter() and GetBitmapFilter().
  // All calls to AddBitmapFilter() should happen before.
  const Bitmap* GetBitmapFilter(SlotId slot) {
    if (slot_bitmap_filters_.find(slot) == slot_bitmap_filters_.end()) return NULL;
    return slot_bitmap_filters_[slot];
  }

  PartitionStatusMap* per_partition_status() { return &per_partition_status_; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Returns runtime state profile
  RuntimeProfile* runtime_profile() { return &profile_; }

<<<<<<< HEAD
  // Returns CodeGen object.  Returns NULL if codegen is disabled.
  LlvmCodeGen* llvm_codegen() { return codegen_.get(); }

  // Appends error to the error_log_
  void LogError(const std::string& error);

  // Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() const { return error_log_.size() < query_options_.max_errors; }

  // Clears the error log.
  void ClearErrorLog() { error_log_.clear(); }
=======
  // Returns true if codegen is enabled for this query.
  bool codegen_enabled() const { return !query_options().disable_codegen; }

  // Returns true if the codegen object has been created. Note that this may return false
  // even when codegen is enabled if nothing has been codegen'd.
  bool codegen_created() const { return codegen_.get() != NULL; }

  // Returns codegen_ in 'codegen'. If 'initialize' is true, codegen_ will be created if
  // it has not been initialized by a previous call already. If 'initialize' is false,
  // 'codegen' will be set to NULL if codegen_ has not been initialized.
  Status GetCodegen(LlvmCodeGen** codegen, bool initialize = true);

  BufferedBlockMgr* block_mgr() {
    DCHECK(block_mgr_.get() != NULL);
    return block_mgr_.get();
  }

  Status query_status() {
    boost::lock_guard<boost::mutex> l(query_status_lock_);
    return query_status_;
  };

  // Appends error to the error_log_ if there is space. Returns true if there was space
  // and the error was logged.
  bool LogError(const std::string& error);

  // If !status.ok(), appends the error to the error_log_
  void LogError(const Status& status);

  // Returns true if the error log has not reached max_errors_.
  bool LogHasSpace() {
    boost::lock_guard<boost::mutex> l(error_log_lock_);
    return error_log_.size() < query_options().max_errors;
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Report that num_errors occurred while parsing file_name.
  void ReportFileErrors(const std::string& file_name, int num_errors);

  // Clear the file errors.
  void ClearFileErrors() { file_errors_.clear(); }

  // Return true if error log is empty.
  bool ErrorLogIsEmpty();

  // Returns the error log lines as a string joined with '\n'.
  std::string ErrorLog();

  // Append all error_log_[unreported_error_idx_+] to new_errors and set
  // unreported_error_idx_ to errors_log_.size()
  void GetUnreportedErrors(std::vector<std::string>* new_errors);

  // Returns a string representation of the file_errors_.
  std::string FileErrors() const;

  bool is_cancelled() const { return is_cancelled_; }
  void set_is_cancelled(bool v) { is_cancelled_ = v; }

<<<<<<< HEAD
 private:
  static const int DEFAULT_BATCH_SIZE = 1024;
  // This is the number of buffers per disk.
  static const int DEFAULT_MAX_IO_BUFFERS = 5;
=======
  RuntimeProfile::Counter* total_cpu_timer() { return total_cpu_timer_; }
  RuntimeProfile::Counter* total_storage_wait_timer() {
    return total_storage_wait_timer_;
  }
  RuntimeProfile::Counter* total_network_send_timer() {
    return total_network_send_timer_;
  }
  RuntimeProfile::Counter* total_network_receive_timer() {
    return total_network_receive_timer_;
  }

  // Sets query_status_ with err_msg if no error has been set yet.
  void set_query_status(const std::string& err_msg) {
    boost::lock_guard<boost::mutex> l(query_status_lock_);
    if (!query_status_.ok()) return;
    query_status_ = Status(err_msg);
  }

  // Sets query_status_ to MEM_LIMIT_EXCEEDED and logs all the registered trackers.
  // Subsequent calls to this will be no-ops.
  // If failed_allocation_size is not 0, then it is the size of the allocation (in
  // bytes) that would have exceeded the limit allocated for 'tracker'.
  // This value and tracker are only used for error reporting.
  Status SetMemLimitExceeded(MemTracker* tracker = NULL,
      int64_t failed_allocation_size = 0);

  // Returns a non-OK status if query execution should stop (e.g., the query was cancelled
  // or a mem limit was exceeded). Exec nodes should check this periodically so execution
  // doesn't continue if the query terminates abnormally.
  Status CheckQueryState();

  QueryResourceMgr* query_resource_mgr() const { return query_resource_mgr_; }
  void SetQueryResourceMgr(QueryResourceMgr* res_mgr) { query_resource_mgr_ = res_mgr; }

 private:
  // Set per-fragment state.
  Status Init(ExecEnv* exec_env);

  // Create a codegen object in codegen_. No-op if it has already been called. This is
  // created on first use.
  Status CreateCodegen();

  static const int DEFAULT_BATCH_SIZE = 1024;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  DescriptorTbl* desc_tbl_;
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Lock protecting error_log_ and unreported_error_idx_
  boost::mutex error_log_lock_;

  // Logs error messages.
  std::vector<std::string> error_log_;

  // error_log_[unreported_error_idx_+] has been not reported to the coordinator.
  int unreported_error_idx_;

<<<<<<< HEAD
  // Stores the number of parse errors per file.
  std::vector<std::pair<std::string, int> > file_errors_;

  // Query-global timestamp, e.g., for implementing now().
  // Use pointer to avoid inclusion of timestampvalue.h and avoid clang issues.
  boost::scoped_ptr<TimestampValue> now_;

  TUniqueId fragment_instance_id_;
  TQueryOptions query_options_;
  ExecEnv* exec_env_;
  boost::scoped_ptr<LlvmCodeGen> codegen_;

  // Temporary Hdfs files created, and where they should be moved to ultimately.
  // Mapping a filename to a blank destination causes it to be deleted. 
  FileMoveMap hdfs_files_to_move_;

  // Records the total number of appended rows per created Hdfs partition
  PartitionRowCount num_appended_rows_;

  RuntimeProfile profile_;

  // if true, execution should stop with a CANCELLED status
  bool is_cancelled_;

  // prohibit copies
  RuntimeState(const RuntimeState&);

  // set codegen_
  Status CreateCodegen();
=======
  // Lock protecting file_errors_
  mutable boost::mutex file_errors_lock_;

  // Stores the number of parse errors per file.
  std::vector<std::pair<std::string, int> > file_errors_;

  // Context of this fragment instance, including its unique id, the total number
  // of fragment instances, the query context, the coordinator address, etc.
  TPlanFragmentInstanceCtx fragment_instance_ctx_;

  // Query-global timestamp, e.g., for implementing now(). Set from query_globals_.
  // Use pointer to avoid inclusion of timestampvalue.h and avoid clang issues.
  boost::scoped_ptr<TimestampValue> now_;

  // The Impala-internal cgroup into which execution threads are assigned.
  // If empty, no RM is enabled.
  std::string cgroup_;
  ExecEnv* exec_env_;
  boost::scoped_ptr<LlvmCodeGen> codegen_;

  // Thread resource management object for this fragment's execution.  The runtime
  // state is responsible for returning this pool to the thread mgr.
  ThreadResourceMgr::ResourcePool* resource_pool_;

  // Temporary Hdfs files created, and where they should be moved to ultimately.
  // Mapping a filename to a blank destination causes it to be deleted.
  FileMoveMap hdfs_files_to_move_;

  // Records summary statistics for the results of inserts into Hdfs partitions.
  PartitionStatusMap per_partition_status_;

  RuntimeProfile profile_;

  // Total CPU time (across all threads), including all wait times.
  RuntimeProfile::Counter* total_cpu_timer_;

  // Total time waiting in storage (across all threads)
  RuntimeProfile::Counter* total_storage_wait_timer_;

  // Total time spent sending over the network (across all threads)
  RuntimeProfile::Counter* total_network_send_timer_;

  // Total time spent receiving over the network (across all threads)
  RuntimeProfile::Counter* total_network_receive_timer_;

  // MemTracker that is shared by all fragment instances running on this host.
  // The query mem tracker must be released after the instance_mem_tracker_.
  boost::shared_ptr<MemTracker> query_mem_tracker_;

  // Memory usage of this fragment instance
  boost::scoped_ptr<MemTracker> instance_mem_tracker_;

  // if true, execution should stop with a CANCELLED status
  bool is_cancelled_;

  // Non-OK if an error has occurred and query execution should abort. Used only for
  // asynchronously reporting such errors (e.g., when a UDF reports an error), so this
  // will not necessarily be set in all error cases.
  boost::mutex query_status_lock_;
  Status query_status_;

  // Query-wide resource manager for resource expansion etc. Not owned by us; owned by the
  // ResourceBroker instead.
  QueryResourceMgr* query_resource_mgr_;

  // Reader contexts that need to be closed when the fragment is closed.
  std::vector<DiskIoMgr::RequestContext*> reader_contexts_;

  // BufferedBlockMgr object used to allocate and manage blocks of input data in memory
  // with a fixed memory budget.
  // The block mgr is shared by all fragments for this query.
  boost::shared_ptr<BufferedBlockMgr> block_mgr_;

  // This is the node id of the root node for this plan fragment. This is used as the
  // hash seed and has two useful properties:
  // 1) It is the same for all exec nodes in a fragment, so the resulting hash values
  // can be shared (i.e. for slot_bitmap_filters_).
  // 2) It is different between different fragments, so we do not run into hash
  // collisions after data partitioning (across fragments). See IMPALA-219 for more
  // details.
  PlanNodeId root_node_id_;

  // Lock protecting slot_bitmap_filters_
  SpinLock bitmap_lock_;

  // Bitmap filter on the hash for 'SlotId'. If bitmap[hash(slot]] is unset, this
  // value can be filtered out. These filters are generated during the query execution.
  boost::unordered_map<SlotId, Bitmap*> slot_bitmap_filters_;

  // prohibit copies
  RuntimeState(const RuntimeState&);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

#define RETURN_IF_CANCELLED(state) \
  do { \
    if (UNLIKELY((state)->is_cancelled())) return Status(TStatusCode::CANCELLED); \
  } while (false)

}

#endif
