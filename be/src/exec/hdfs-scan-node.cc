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

#include "exec/hdfs-scan-node.h"
<<<<<<< HEAD
#include "exec/hdfs-text-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-trevni-scanner.h"
#include "exec/hdfs-byte-stream.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
=======
#include "exec/base-sequence-scanner.h"
#include "exec/hdfs-text-scanner.h"
#include "exec/hdfs-lzo-text-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-avro-scanner.h"
#include "exec/hdfs-parquet-scanner.h"

#include <sstream>
#include <boost/algorithm/string.hpp>
#include <boost/foreach.hpp>
#include <boost/filesystem.hpp>
#include <gutil/strings/substitute.h>

#include <hdfs.h>
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

#include "codegen/llvm-codegen.h"
#include "common/logging.h"
#include "common/object-pool.h"
<<<<<<< HEAD
#include "exec/scan-range-context.h"
#include "exprs/expr.h"
=======
#include "exprs/expr-context.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "runtime/descriptors.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
#include "runtime/row-batch.h"
<<<<<<< HEAD
#include "util/debug-util.h"
=======
#include "util/bit-util.h"
#include "util/container-util.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/error-util.h"
#include "util/hdfs-util.h"
#include "util/impalad-metrics.h"
#include "util/periodic-counter-updater.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "util/runtime-profile.h"

#include "gen-cpp/PlanNodes_types.h"

<<<<<<< HEAD
// TODO: temp change to validate we don't have an incast problem for joins with big tables
DEFINE_bool(randomize_scan_ranges, false, 
    "if true, randomizes the order of scan ranges");
=======
DEFINE_int32(max_row_batches, 0, "the maximum size of materialized_row_batches_");
DECLARE_string(cgroup_hierarchy_path);
DECLARE_bool(enable_rm);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

using namespace boost;
using namespace impala;
using namespace llvm;
using namespace std;
<<<<<<< HEAD
=======
using namespace strings;

const string HdfsScanNode::HDFS_SPLIT_STATS_DESC =
    "Hdfs split stats (<volume id>:<# splits>/<split lengths>)";

// Amount of memory that we approximate a scanner thread will use not including IoBuffers.
// The memory used does not vary considerably between file formats (just a couple of MBs).
// This value is conservative and taken from running against the tpch lineitem table.
// TODO: revisit how we do this.
const int SCANNER_THREAD_MEM_USAGE = 32 * 1024 * 1024;

// Estimated upper bound on the compression ratio of compressed text files. Used to
// estimate scanner thread memory usage.
const int COMPRESSED_TEXT_COMPRESSION_RATIO = 11;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

HdfsScanNode::HdfsScanNode(ObjectPool* pool, const TPlanNode& tnode,
                           const DescriptorTbl& descs)
    : ScanNode(pool, tnode, descs),
      thrift_plan_node_(new TPlanNode(tnode)),
<<<<<<< HEAD
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      compact_data_(tnode.compact_data),
      reader_context_(NULL),
      tuple_desc_(NULL),
      unknown_disk_id_warned_(false),
      tuple_pool_(new MemPool()),
      num_unqueued_files_(0),
      scanner_pool_(new ObjectPool()),
      current_scanner_(NULL),
      current_byte_stream_(NULL),
      num_partition_keys_(0),
      done_(false),
      partition_key_pool_(new MemPool()),
      next_range_to_issue_idx_(0),
      all_ranges_in_queue_(false),
      ranges_in_flight_(0),
      all_ranges_issued_(false) {
=======
      runtime_state_(NULL),
      tuple_id_(tnode.hdfs_scan_node.tuple_id),
      reader_context_(NULL),
      tuple_desc_(NULL),
      unknown_disk_id_warned_(false),
      initial_ranges_issued_(false),
      scanner_thread_bytes_required_(0),
      disks_accessed_bitmap_(TCounterType::UNIT, 0),
      done_(false),
      all_ranges_started_(false),
      counters_running_(false),
      rm_callback_id_(-1) {
  max_materialized_row_batches_ = FLAGS_max_row_batches;
  if (max_materialized_row_batches_ <= 0) {
    // TODO: This parameter has an U-shaped effect on performance: increasing the value
    // would first improves performance, but further increasing would degrade performance.
    // Investigate and tune this.
    max_materialized_row_batches_ = 10 * DiskInfo::num_disks();
  }
  materialized_row_batches_.reset(new RowBatchQueue(max_materialized_row_batches_));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

HdfsScanNode::~HdfsScanNode() {
}

Status HdfsScanNode::GetNext(RuntimeState* state, RowBatch* row_batch, bool* eos) {
<<<<<<< HEAD
  RETURN_IF_CANCELLED(state);
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  {
    unique_lock<recursive_mutex> l(lock_);
    if (per_file_scan_ranges_.size() == 0 || ReachedLimit() || !status_.ok()) {
      *eos = true;
      return status_;
    }
  }

  *eos = false;
  while (true) {
    unique_lock<mutex> l(row_batches_lock_);
    while (materialized_row_batches_.empty() && !done_) {
      row_batch_added_cv_.wait(l);
    }

    // Return any errors
    if (!status_.ok()) return status_;

    if (!materialized_row_batches_.empty()) {
      RowBatch* materialized_batch = materialized_row_batches_.front();
      materialized_row_batches_.pop_front();

      row_batch->Swap(materialized_batch);
      // Update the number of materialized rows instead of when they are materialized.
      // This means that scanners might process and queue up more rows that are necessary
      // for the limit case but we want to avoid the synchronized writes to 
      // num_rows_returned_
      num_rows_returned_ += row_batch->num_rows();
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);
      
      if (ReachedLimit()) {
        int num_rows_over = num_rows_returned_ - limit_;
        row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
        num_rows_returned_ -= num_rows_over;
        COUNTER_SET(rows_returned_counter_, num_rows_returned_);

        *eos = true;
        // Wake up disk thread notifying it we are done.  This triggers tear down
        // of the scanner threads.
        done_ = true;
        state->io_mgr()->CancelReader(reader_context_);
      }
      delete materialized_batch;
      return Status::OK;
    } else {
      break;
    }
  } 

  // TODO: remove when all scanners are updated to use io mgr.
  if (current_scanner_ == NULL) {
    RETURN_IF_ERROR(InitNextScanRange(state, eos));
    if (*eos) return Status::OK;
  }

  // Loops until all the scan ranges are complete or batch is full
  *eos = false;
  do {
    bool eosr = false;
    RETURN_IF_ERROR(current_scanner_->GetNext(row_batch, &eosr));
    RETURN_IF_CANCELLED(state);

=======
  SCOPED_TIMER(runtime_profile_->total_time_counter());

  if (!initial_ranges_issued_) {
    // We do this in GetNext() to ensure that all execution time predicates have
    // been generated (e.g. probe side bitmap filters).
    // TODO: we could do dynamic partition pruning here as well.
    initial_ranges_issued_ = true;
    // Issue initial ranges for all file types.
    RETURN_IF_ERROR(HdfsTextScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::TEXT]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::SEQUENCE_FILE]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::RC_FILE]));
    RETURN_IF_ERROR(BaseSequenceScanner::IssueInitialRanges(this,
        per_type_files_[THdfsFileFormat::AVRO]));
    RETURN_IF_ERROR(HdfsParquetScanner::IssueInitialRanges(this,
          per_type_files_[THdfsFileFormat::PARQUET]));
    if (progress_.done()) SetDone();
  }

  Status status = GetNextInternal(state, row_batch, eos);
  if (status.IsMemLimitExceeded()) state->SetMemLimitExceeded();
  if (!status.ok() || *eos) StopAndFinalizeCounters();
  return status;
}

Status HdfsScanNode::GetNextInternal(
    RuntimeState* state, RowBatch* row_batch, bool* eos) {
  RETURN_IF_ERROR(ExecDebugAction(TExecNodePhase::GETNEXT, state));
  RETURN_IF_CANCELLED(state);
  RETURN_IF_ERROR(QueryMaintenance(state));

  {
    unique_lock<mutex> l(lock_);
    if (ReachedLimit() || !status_.ok()) {
      *eos = true;
      return status_;
    }
  }
  *eos = false;

  RowBatch* materialized_batch = materialized_row_batches_->GetBatch();
  if (materialized_batch != NULL) {
    num_owned_io_buffers_ -= materialized_batch->num_io_buffers();
    row_batch->AcquireState(materialized_batch);
    // Update the number of materialized rows instead of when they are materialized.
    // This means that scanners might process and queue up more rows than are necessary
    // for the limit case but we want to avoid the synchronized writes to
    // num_rows_returned_
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    num_rows_returned_ += row_batch->num_rows();
    COUNTER_SET(rows_returned_counter_, num_rows_returned_);

    if (ReachedLimit()) {
<<<<<<< HEAD
      *eos = true;
      return Status::OK;
    }

    if (eosr) { // Current scan range is finished
      ++current_range_idx_;

      // Will update file and current_file_scan_ranges_ so that subsequent check passes
      RETURN_IF_ERROR(InitNextScanRange(state, eos));
      // Done with all files
      if (*eos) break;
    }
  } while (!row_batch->IsFull());

  return Status::OK;
}

Status HdfsScanNode::CreateConjuncts(vector<Expr*>* expr) {
  // This is only used when codegen is not possible for this node.  We don't want to
  // codegen the copy of the expr.  
  // TODO: we really need to stop having to create copies of exprs
  RETURN_IF_ERROR(Expr::CreateExprTrees(runtime_state_->obj_pool(), 
      thrift_plan_node_->conjuncts, expr));
  for (int i = 0; i < expr->size(); ++i) {
    RETURN_IF_ERROR(Expr::Prepare((*expr)[i], runtime_state_, row_desc(), true));
  }
  return Status::OK;
}

Status HdfsScanNode::SetScanRanges(const vector<TScanRangeParams>& scan_range_params) {
  // Convert the input ranges into per file DiskIO::ScanRange objects
  for (int i = 0; i < scan_range_params.size(); ++i) {
    DCHECK(scan_range_params[i].scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = scan_range_params[i].scan_range.hdfs_file_split;
    const string& path = split.path;

    HdfsFileDesc* desc = NULL;
    ScanRangeMap::iterator desc_it = per_file_scan_ranges_.find(path);
    if (desc_it == per_file_scan_ranges_.end()) {
      desc = runtime_state_->obj_pool()->Add(new HdfsFileDesc(path));
      per_file_scan_ranges_[path] = desc;
    } else {
      desc = desc_it->second;
    }
      
    if (scan_range_params[i].volume_id == -1 && !unknown_disk_id_warned_) {
      LOG(WARNING) << "Unknown disk id.  This will negatively affect performance. "
                   << " Check your hdfs settings to enable block location metadata.";
      unknown_disk_id_warned_ = true;
    }

    desc->ranges.push_back(AllocateScanRange(desc->filename.c_str(), 
       split.length, split.offset, split.partition_id, scan_range_params[i].volume_id));
  }
=======
      int num_rows_over = num_rows_returned_ - limit_;
      row_batch->set_num_rows(row_batch->num_rows() - num_rows_over);
      num_rows_returned_ -= num_rows_over;
      COUNTER_SET(rows_returned_counter_, num_rows_returned_);

      *eos = true;
      SetDone();
    }
    DCHECK_EQ(materialized_batch->num_io_buffers(), 0);
    delete materialized_batch;
    return Status::OK;
  }

  *eos = true;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

DiskIoMgr::ScanRange* HdfsScanNode::AllocateScanRange(const char* file, int64_t len,
<<<<<<< HEAD
    int64_t offset, int64_t partition_id, int disk_id) {
=======
    int64_t offset, int64_t partition_id, int disk_id, bool try_cache,
    bool expected_local) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  DCHECK_GE(disk_id, -1);
  if (disk_id == -1) {
    // disk id is unknown, assign it a random one.
    static int next_disk_id = 0;
    disk_id = next_disk_id++;
<<<<<<< HEAD

  }
=======
  }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // TODO: we need to parse the config for the number of dirs configured for this
  // data node.
  disk_id %= runtime_state_->io_mgr()->num_disks();

<<<<<<< HEAD
  DiskIoMgr::ScanRange* range = 
      runtime_state_->obj_pool()->Add(new DiskIoMgr::ScanRange());
  range->Reset(file, len, offset, disk_id, reinterpret_cast<void*>(partition_id));
  return range;
}

Status HdfsScanNode::InitNextScanRange(RuntimeState* state, bool* scan_ranges_finished) {
  *scan_ranges_finished = false;
  while (true) {
    if (current_file_scan_ranges_ == per_file_scan_ranges_.end()) {
      // Defensively return if this gets called after all scan ranges are exhausted.
      *scan_ranges_finished = true;
      return Status::OK;
    } else if (current_range_idx_ == current_file_scan_ranges_->second->ranges.size()) {
      RETURN_IF_ERROR(current_byte_stream_->Close());
      current_byte_stream_.reset(NULL);
      ++current_file_scan_ranges_;
      current_range_idx_ = 0;
      if (current_file_scan_ranges_ == per_file_scan_ranges_.end()) {
        *scan_ranges_finished = true;
        return Status::OK; // We're done; caller will check for this condition and exit
      }
    }

    DiskIoMgr::ScanRange* range = 
        current_file_scan_ranges_->second->ranges[current_range_idx_];
    int64_t partition_id = reinterpret_cast<int64_t>(range->meta_data());
    HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);

    if (partition == NULL) {
      stringstream ss;
      ss << "Could not find partition with id: " << partition_id;
      return Status(ss.str());
    }

    // TODO: HACK.  Skip this file if it uses the io mgr, it is handled very differently
    if (partition->file_format() == THdfsFileFormat::TEXT ||
        partition->file_format() == THdfsFileFormat::SEQUENCE_FILE) {
      ++current_file_scan_ranges_;
      continue;
    }

    Tuple* template_tuple = NULL;
    // Only allocate template_tuple_ if there are partition keys.  The scanners
    // use template_tuple == NULL to determine if partition keys are necessary
    if (!partition_key_slots_.empty()) {
      DCHECK(!partition->partition_key_values().empty());
      template_tuple = InitTemplateTuple(state, partition->partition_key_values());
    } 

    if (current_byte_stream_ == NULL) {
      current_byte_stream_.reset(new HdfsByteStream(hdfs_connection_, this));
      RETURN_IF_ERROR(current_byte_stream_->Open(current_file_scan_ranges_->first));
      current_scanner_ = GetScanner(partition);
      DCHECK(current_scanner_ != NULL);
    }

    RETURN_IF_ERROR(current_scanner_->InitCurrentScanRange(partition, range, 
        template_tuple, current_byte_stream_.get()));
    return Status::OK;
  }
  return Status::OK;
}

HdfsFileDesc* HdfsScanNode::GetFileDesc(const string& filename) {
  DCHECK(per_file_scan_ranges_.find(filename) != per_file_scan_ranges_.end());
  return per_file_scan_ranges_[filename];
=======
  ScanRangeMetadata* metadata =
      runtime_state_->obj_pool()->Add(new ScanRangeMetadata(partition_id));
  DiskIoMgr::ScanRange* range =
      runtime_state_->obj_pool()->Add(new DiskIoMgr::ScanRange());
  range->Reset(file, len, offset, disk_id, try_cache, expected_local, metadata);
  return range;
}

HdfsFileDesc* HdfsScanNode::GetFileDesc(const string& filename) {
  DCHECK(file_descs_.find(filename) != file_descs_.end());
  return file_descs_[filename];
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

void HdfsScanNode::SetFileMetadata(const string& filename, void* metadata) {
  unique_lock<mutex> l(metadata_lock_);
  DCHECK(per_file_metadata_.find(filename) == per_file_metadata_.end());
  per_file_metadata_[filename] = metadata;
}

void* HdfsScanNode::GetFileMetadata(const string& filename) {
  unique_lock<mutex> l(metadata_lock_);
  map<string, void*>::iterator it = per_file_metadata_.find(filename);
  if (it == per_file_metadata_.end()) return NULL;
  return it->second;
}

<<<<<<< HEAD
Function* HdfsScanNode::GetCodegenFn(THdfsFileFormat::type type) {
=======
void* HdfsScanNode::GetCodegenFn(THdfsFileFormat::type type) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  CodegendFnMap::iterator it = codegend_fn_map_.find(type);
  if (it == codegend_fn_map_.end()) return NULL;
  return it->second;
}

<<<<<<< HEAD
HdfsScanner* HdfsScanNode::GetScanner(HdfsPartitionDescriptor* partition) {
  ScannerMap::iterator scanner_it = scanner_map_.find(partition->file_format());
  if (scanner_it != scanner_map_.end()) {
    return scanner_it->second;
  }
  HdfsScanner* scanner = CreateScanner(partition);
  scanner_map_[partition->file_format()] = scanner;
  return scanner;
}

HdfsScanner* HdfsScanNode::CreateScanner(HdfsPartitionDescriptor* partition) {
  HdfsScanner* scanner = NULL;

  // Create a new scanner for this file format
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      scanner = new HdfsTextScanner(this, runtime_state_);
=======
HdfsScanner* HdfsScanNode::CreateAndPrepareScanner(HdfsPartitionDescriptor* partition,
    ScannerContext* context, Status* status) {
  DCHECK(context != NULL);
  HdfsScanner* scanner = NULL;
  THdfsCompression::type compression =
      context->GetStream()->file_desc()->file_compression;

  // Create a new scanner for this file format and compression.
  switch (partition->file_format()) {
    case THdfsFileFormat::TEXT:
      // Lzo-compressed text files are scanned by a scanner that it is implemented as a
      // dynamic library, so that Impala does not include GPL code.
      if (compression == THdfsCompression::LZO) {
        scanner = HdfsLzoTextScanner::GetHdfsLzoTextScanner(this, runtime_state_);
      } else {
        scanner = new HdfsTextScanner(this, runtime_state_);
      }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      break;
    case THdfsFileFormat::SEQUENCE_FILE:
      scanner = new HdfsSequenceScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::RC_FILE:
<<<<<<< HEAD
      scanner = new HdfsRCFileScanner(this, runtime_state_, tuple_pool_.get());
      break;
    case THdfsFileFormat::TREVNI:
      scanner = new HdfsTrevniScanner(this, runtime_state_, tuple_pool_.get());
=======
      scanner = new HdfsRCFileScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::AVRO:
      scanner = new HdfsAvroScanner(this, runtime_state_);
      break;
    case THdfsFileFormat::PARQUET:
      scanner = new HdfsParquetScanner(this, runtime_state_);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      break;
    default:
      DCHECK(false) << "Unknown Hdfs file format type:" << partition->file_format();
      return NULL;
  }
<<<<<<< HEAD
  scanner_pool_->Add(scanner);
  // TODO better error handling
  Status status = scanner->Prepare();
  DCHECK(status.ok());
  return scanner;
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
=======
  DCHECK(scanner != NULL);
  runtime_state_->obj_pool()->Add(scanner);
  *status = scanner->Prepare(context);
  return scanner;
}

Tuple* HdfsScanNode::InitTemplateTuple(RuntimeState* state,
                                       const vector<ExprContext*>& value_ctxs) {
  if (partition_key_slots_.empty()) return NULL;

  // Look to protect access to partition_key_pool_ and value_ctxs
  // TODO: we can push the lock to the mempool and exprs_values should not
  // use internal memory.
  Tuple* template_tuple = InitEmptyTemplateTuple();

  unique_lock<mutex> l(lock_);
  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];
    // Exprs guaranteed to be literals, so can safely be evaluated without a row context
    void* value = value_ctxs[slot_desc->col_pos()]->GetValue(NULL);
    RawValue::Write(value, template_tuple, slot_desc, NULL);
  }
  return template_tuple;
}

Tuple* HdfsScanNode::InitEmptyTemplateTuple() {
  Tuple* template_tuple = NULL;
  {
    unique_lock<mutex> l(lock_);
    template_tuple = Tuple::Create(tuple_desc_->byte_size(), scan_node_pool_.get());
  }
  memset(template_tuple, 0, tuple_desc_->byte_size());
  return template_tuple;
}

void HdfsScanNode::TransferToScanNodePool(MemPool* pool) {
  unique_lock<mutex> l(lock_);
  scan_node_pool_->AcquireData(pool, false);
}

Status HdfsScanNode::Prepare(RuntimeState* state) {
  SCOPED_TIMER(runtime_profile_->total_time_counter());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  runtime_state_ = state;
  RETURN_IF_ERROR(ScanNode::Prepare(state));

  tuple_desc_ = state->desc_tbl().GetTupleDescriptor(tuple_id_);
  DCHECK(tuple_desc_ != NULL);
<<<<<<< HEAD
  current_range_idx_ = 0;

  // One-time initialisation of state that is constant across scan ranges
  DCHECK(tuple_desc_->table_desc() != NULL);

  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
=======

  if (!state->cgroup().empty()) {
    scanner_threads_.SetCgroupsMgr(state->exec_env()->cgroups_mgr());
    scanner_threads_.SetCgroup(state->cgroup());
  }

  // One-time initialisation of state that is constant across scan ranges
  DCHECK(tuple_desc_->table_desc() != NULL);
  hdfs_table_ = static_cast<const HdfsTableDescriptor*>(tuple_desc_->table_desc());
  scan_node_pool_.reset(new MemPool(mem_tracker()));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Create mapping from column index in table to slot index in output tuple.
  // First, initialize all columns to SKIP_COLUMN.
  int num_cols = hdfs_table_->num_cols();
  column_idx_to_materialized_slot_idx_.resize(num_cols);
  for (int i = 0; i < num_cols; ++i) {
    column_idx_to_materialized_slot_idx_[i] = SKIP_COLUMN;
  }

<<<<<<< HEAD
  num_partition_keys_ = hdfs_table_->num_clustering_cols();

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Next, collect all materialized (partition key and not) slots
  vector<SlotDescriptor*> all_materialized_slots;
  all_materialized_slots.resize(num_cols);
  const vector<SlotDescriptor*>& slots = tuple_desc_->slots();
  for (size_t i = 0; i < slots.size(); ++i) {
    if (!slots[i]->is_materialized()) continue;
    int col_idx = slots[i]->col_pos();
<<<<<<< HEAD
=======
    DCHECK_LT(col_idx, column_idx_to_materialized_slot_idx_.size());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    DCHECK_EQ(column_idx_to_materialized_slot_idx_[col_idx], SKIP_COLUMN);
    all_materialized_slots[col_idx] = slots[i];
  }

<<<<<<< HEAD
  // Finally, populate materialized_slots_ and partition_key_slots_ in the order that 
  // the slots appear in the file.  
=======
  // Finally, populate materialized_slots_ and partition_key_slots_ in the order that
  // the slots appear in the file.
  int num_partition_keys = hdfs_table_->num_clustering_cols();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  for (int i = 0; i < num_cols; ++i) {
    SlotDescriptor* slot_desc = all_materialized_slots[i];
    if (slot_desc == NULL) continue;
    if (hdfs_table_->IsClusteringCol(slot_desc)) {
      partition_key_slots_.push_back(slot_desc);
    } else {
<<<<<<< HEAD
      DCHECK_GE(i, num_partition_keys_);
=======
      DCHECK_GE(i, num_partition_keys);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      column_idx_to_materialized_slot_idx_[i] = materialized_slots_.size();
      materialized_slots_.push_back(slot_desc);
    }
  }

<<<<<<< HEAD
  // Codegen scanner specific functions
  if (state->llvm_codegen() != NULL) {
    Function* text_fn = HdfsTextScanner::Codegen(this);
    Function* seq_fn = HdfsSequenceScanner::Codegen(this);
    if (text_fn != NULL) codegend_fn_map_[THdfsFileFormat::TEXT] = text_fn;
    if (seq_fn != NULL) codegend_fn_map_[THdfsFileFormat::SEQUENCE_FILE] = seq_fn;
  }

  return Status::OK;
}

Tuple* HdfsScanNode::InitTemplateTuple(RuntimeState* state,
    const vector<Expr*>& expr_values) {
  if (partition_key_slots_.empty()) return NULL;

  // Look to protect access to partition_key_pool_ and expr_values
  // TODO: we can push the lock to the mempool and exprs_values should not
  // use internal memory.
  unique_lock<recursive_mutex> l(lock_);
  Tuple* template_tuple = 
      reinterpret_cast<Tuple*>(partition_key_pool_->Allocate(tuple_desc_->byte_size()));
  memset(template_tuple, 0, tuple_desc_->byte_size());

  for (int i = 0; i < partition_key_slots_.size(); ++i) {
    const SlotDescriptor* slot_desc = partition_key_slots_[i];

    // Exprs guaranteed to be literals, so can safely be evaluated without a row context
    void* value = expr_values[slot_desc->col_pos()]->GetValue(NULL);
    RawValue::Write(value, template_tuple, slot_desc, NULL);
  }
  return template_tuple;
}

// This function initiates the connection to hdfs and starts up the disk thread.
// Scan ranges are accumulated by file type and the scanner subclasses are passed
// the initial ranges.  Scanners are expected to queue up a non-zero number of
// those ranges to the io mgr (via the ScanNode).
Status HdfsScanNode::Open(RuntimeState* state) {
  if (per_file_scan_ranges_.empty()) {
    done_ = true;
    return Status::OK;
  }

  hdfs_connection_ = state->fs_cache()->GetDefaultConnection();
  if (hdfs_connection_ == NULL) {
    stringstream ss;
    ss << "Failed to connect to HDFS." << "\nError(" << errno << "):" << strerror(errno);
    return Status(ss.str());
  } 

  RETURN_IF_ERROR(runtime_state_->io_mgr()->RegisterReader(
      hdfs_connection_, state->max_io_buffers(), &reader_context_));
  runtime_state_->io_mgr()->set_bytes_read_counter(reader_context_, bytes_read_counter());
  runtime_state_->io_mgr()->set_read_timer(reader_context_, read_timer());

  current_file_scan_ranges_ = per_file_scan_ranges_.begin();

  int total_scan_ranges = 0;
  // Walk all the files on this node and coalesce all the files with the same
  // format.
  // Also, initialize all the exprs for all the partition keys.
  map<THdfsFileFormat::type, vector<HdfsFileDesc*> > per_type_files;
  for (ScanRangeMap::iterator it = per_file_scan_ranges_.begin(); 
       it != per_file_scan_ranges_.end(); ++it) {
    vector<DiskIoMgr::ScanRange*>& ranges = it->second->ranges;
    DCHECK(!ranges.empty());
    
    int64_t partition_id = reinterpret_cast<int64_t>(ranges[0]->meta_data());
    HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
    if (partition == NULL) {
      stringstream ss;
      ss << "Could not find partition with id: " << partition_id;
      return Status(ss.str());
    }

    // TODO: remove when all scanners are updated
    if (partition->file_format() == THdfsFileFormat::TEXT ||
        partition->file_format() == THdfsFileFormat::SEQUENCE_FILE) {
      ++num_unqueued_files_;
      total_scan_ranges += ranges.size();
    } 

    RETURN_IF_ERROR(partition->PrepareExprs(state));
    per_type_files[partition->file_format()].push_back(it->second);
  }

  if (total_scan_ranges == 0) {
    done_ = true;
    return Status::OK;
  }

  stringstream ss;
  ss << "Scan ranges complete (node=" << id() << "):";
  progress_ = ProgressUpdater(ss.str(), total_scan_ranges);

  max_scanner_threads_ = state->num_scanner_threads();
  if (max_scanner_threads_ == 0) {
    max_scanner_threads_ = total_scan_ranges;
  } else {
    max_scanner_threads_ = min(state->num_scanner_threads(), total_scan_ranges);
  }
  VLOG_FILE << "Using " << max_scanner_threads_ << " simultaneous scanner threads.";
  DCHECK_GT(max_scanner_threads_, 0);

  if (FLAGS_randomize_scan_ranges) {
    unsigned int seed = time(NULL);
    srand(seed);
    VLOG_QUERY << "Randomizing scan range order with seed=" << seed;
    map<THdfsFileFormat::type, vector<HdfsFileDesc*> >::iterator it;
    for (it = per_type_files.begin(); it != per_type_files.end(); ++it) {
      vector<HdfsFileDesc*>& file_descs = it->second;
      for (int i = 0; i < file_descs.size(); ++i) {
        random_shuffle(file_descs[i]->ranges.begin(), file_descs[i]->ranges.end());
      }
      random_shuffle(file_descs.begin(), file_descs.end());
    }
  }

  // Issue initial ranges for all file types.
  HdfsTextScanner::IssueInitialRanges(this, per_type_files[THdfsFileFormat::TEXT]);
  HdfsSequenceScanner::IssueInitialRanges(this, 
      per_type_files[THdfsFileFormat::SEQUENCE_FILE]);
  
  // scanners have added their initial ranges, issue the first batch to the io mgr.
  IssueMoreRanges();

  if (ranges_in_flight_ == 0) {
    // This is a temporary hack for scanners not using the io mgr.
    done_ = true;
    return Status::OK;
  }
  
  // Start up disk thread which in turn drives the scanner threads.
  disk_read_thread_.reset(new thread(&HdfsScanNode::DiskThread, this));
  
  return Status::OK;
}

Status HdfsScanNode::Close(RuntimeState* state) {
  done_ = true;
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->CancelReader(reader_context_);
  }

  if (disk_read_thread_ != NULL) disk_read_thread_->join();

  // There are materialized batches that have not been returned to the parent node.
  // Clean those up now.
  for (list<RowBatch*>::iterator it = materialized_row_batches_.begin();
       it != materialized_row_batches_.end(); ++it) {
    delete *it;
  }
  materialized_row_batches_.clear();
  
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->UnregisterReader(reader_context_);
  }

  scanner_pool_.reset(NULL);

  COUNTER_UPDATE(memory_used_counter_, tuple_pool_->peak_allocated_bytes());
  return ExecNode::Close(state);
}

void HdfsScanNode::AddDiskIoRange(DiskIoMgr::ScanRange* range) {
  unique_lock<recursive_mutex> lock(lock_);
  all_ranges_.push_back(range);
}

void HdfsScanNode::AddDiskIoRange(const HdfsFileDesc* desc) {
  const vector<DiskIoMgr::ScanRange*>& ranges = desc->ranges;
  unique_lock<recursive_mutex> lock(lock_);
  for (int j = 0; j < ranges.size(); ++j) {
    all_ranges_.push_back(ranges[j]);
  }
  FileQueued(desc->filename.c_str());
}

void HdfsScanNode::FileQueued(const char* filename) {
  unique_lock<recursive_mutex> lock(lock_);
  // all_ranges_issued_ is only set to true after all_ranges_in_queue_ is set to
  // true.
  DCHECK(!all_ranges_issued_);
  DCHECK_GT(num_unqueued_files_, 0);
  if (--num_unqueued_files_ == 0) {
    all_ranges_in_queue_ = true;
  }
}

// TODO: this is not in the final state.  Currently, each scanner thread only
// works on one scan range and cannot switch back and forth between them.  This
// puts the limitation that we can't have more ranges in flight than the number of
// scanner threads.  There are two ways we can fix this:
// 1. Add the logic to the io mgr to cap the number of in flight contexts.  This is
// yet another resource for the io mgr to deal with.  It's a bit more bookkeeping:
// max 3 contexts across 5 disks, the io mgr needs to make sure 3 of the disks
// are busy for this reader.  Also, as in this example, it is not possible to spin
// up all the disks with 1 scanner thread.
// 2. Update the scanner threads to be able to switch between scan ranges.  This is
// more flexible and can be done with getcontext()/setcontext() (or libtask).  
// ScanRangeContext provides this abstraction already (when GetBytes() blocks, switch
// to another scan range).
//
// Currently, we have implemented a poor man's version of #1 above in the scan node.
// An alternative stop gap would be remove num_scanner_threads and always spin up
// threads = num_buffers_per_disk * num_disks.  This will let us use the disks
// better and closely mimics the final state with solution #2.
Status HdfsScanNode::IssueMoreRanges() {
  unique_lock<recursive_mutex> lock(lock_);

  int num_remaining = all_ranges_.size() - next_range_to_issue_idx_;
  int threads_remaining = max_scanner_threads_ - ranges_in_flight_;
  int ranges_to_issue = min(threads_remaining, num_remaining);

  vector<DiskIoMgr::ScanRange*> ranges;
  if (ranges_to_issue > 0) {
    for (int i = 0; i < ranges_to_issue; ++i, ++next_range_to_issue_idx_) {
      ranges.push_back(all_ranges_[next_range_to_issue_idx_]);
    }
  }
  ranges_in_flight_ += ranges.size();
  
  if (next_range_to_issue_idx_ == all_ranges_.size() && all_ranges_in_queue_) {
    all_ranges_issued_ = true;
  }
    
  if (!ranges.empty()) {
    RETURN_IF_ERROR(runtime_state_->io_mgr()->AddScanRanges(reader_context_, ranges));
  }

  return Status::OK;
}

// lock_ should be taken before calling this.
void HdfsScanNode::StartNewScannerThread(DiskIoMgr::BufferDescriptor* buffer) {
  DiskIoMgr::ScanRange* range = buffer->scan_range();
  int64_t partition_id = reinterpret_cast<int64_t>(range->meta_data());
  HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);

  // TODO: no reason these have to be reallocated.  Reuse them.
  ScanRangeContext* context = runtime_state_->obj_pool()->Add(
      new ScanRangeContext(runtime_state_, this, partition, buffer));
  contexts_[range] = context;
  HdfsScanner* scanner = CreateScanner(partition);

  scanner_threads_.add_thread(new thread(&HdfsScanNode::ScannerThread, this,
        scanner, context)); 
}

// The disk thread continuously reads from the io mgr queuing buffers to the 
// correct scan range context.  Each scan range context maps to a single scan range.
// Each buffer the io mgr returns can be mapped to the scan range it is for.  The
// disk thread is responsible for figuring out the mapping and then queueing the
// new buffer onto the corresponding context.  If no context exists (i.e. it is the
// first buffer for this scan range), a new context object is created as well as a
// new scanner thread to process it.
void HdfsScanNode::DiskThread() {
  while (true) {
    bool eos = false;
    DiskIoMgr::BufferDescriptor* buffer_desc = NULL;
    Status status = 
        runtime_state_->io_mgr()->GetNext(reader_context_, &buffer_desc, &eos);

    unique_lock<recursive_mutex> lock(lock_);

    // done_ will trigger the io mgr to return CANCELLED, we can ignore that error
    // since the scan node triggered it itself.
    if (done_) {
      if (buffer_desc != NULL) buffer_desc->Return();
      break;
    }

    // The disk io mgr is done or error occurred.  Tear everything down.
    if (!status.ok()) {
      done_ = true;
      if (buffer_desc != NULL) buffer_desc->Return();
      status_.AddError(status);
      break;
    }
    
    DCHECK(buffer_desc != NULL);
    {
      ContextMap::iterator context_it = contexts_.find(buffer_desc->scan_range());
      if (context_it == contexts_.end()) {
        // New scan range.  Create a new scanner, context and thread for processing it.
        StartNewScannerThread(buffer_desc);
      } else {
        context_it->second->AddBuffer(buffer_desc);
      }
    }
  }
  runtime_profile()->StopRateCounterUpdates(total_throughput_counter());

  // At this point the disk thread is starting cleanup and will no longer read
  // from the io mgr.  This can happen in one of these conditions:
  //   1. All ranges were returned.  (common case).
  //   2. Limit was reached (done_ and status_ is ok)
  //   3. Error occurred (done_ and status_ not ok).
  DCHECK(done_);

  VLOG_FILE << "Disk thread done (node=" << id() << ")";
  // Wake up all contexts that are still waiting.  done_ indicates that the
  // node is complete (e.g. parse error or limit reached()) and the scanner should
  // terminate immediately.
  for (ContextMap::iterator it = contexts_.begin(); it != contexts_.end(); ++it) {
    it->second->Cancel();
  }

  scanner_threads_.join_all();
  contexts_.clear();
  
  // Wake up thread in GetNext
  {
    unique_lock<mutex> l(row_batches_lock_);
    done_ = true;
  }
  row_batch_added_cv_.notify_one();
}

void HdfsScanNode::AddMaterializedRowBatch(RowBatch* row_batch) {
  {
    unique_lock<mutex> l(row_batches_lock_);
    materialized_row_batches_.push_back(row_batch);
  }
  row_batch_added_cv_.notify_one();
}

void HdfsScanNode::ScannerThread(HdfsScanner* scanner, ScanRangeContext* context) {
  // Call into the scanner to process the range.  From the scanner's perspective,
  // everything is single threaded.
  Status status = scanner->ProcessScanRange(context);
  scanner->Close();

  // Scanner thread completed. Take a look and update the status 
  unique_lock<recursive_mutex> l(lock_);
  
  // If there was already an error, the disk thread will do the cleanup.
  if (!status_.ok()) return;

  if (!status.ok()) {
    if (status.IsCancelled()) {
      // Scan node should be the only thing that initiated scanner threads to see
      // cancelled (i.e. limit reached).  No need to do anything here.
      DCHECK(done_);
      return;
    }
    
    // This thread hit an error, record it and bail
    // TODO: better way to report errors?  Maybe via the thrift interface?
    if (VLOG_QUERY_IS_ON && !runtime_state_->error_log().empty()) {
      stringstream ss;
      ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
         << context->filename() << "(" << context->scan_range()->offset() << ":" 
         << context->scan_range()->len() 
         << ").  Processed " << context->total_bytes_returned() << " bytes." << endl
         << runtime_state_->ErrorLog();
      VLOG_QUERY << ss.str();
    }
  
    status_ = status;
    done_ = true;
    // Notify the disk which will trigger tear down of all threads.
    runtime_state_->io_mgr()->CancelReader(reader_context_);
    // Notify the main thread which reports the error
    row_batch_added_cv_.notify_one();
  }

  --ranges_in_flight_;
  if (progress_.done()) {
    // All ranges are finished.  Indicate we are done.
    {
      unique_lock<mutex> l(row_batches_lock_);
      done_ = true;
    }
    row_batch_added_cv_.notify_one();
  } else {
    IssueMoreRanges();
  }
}

void HdfsScanNode::RangeComplete() {
  scan_ranges_complete_counter()->Update(1);
  progress_.Update(1);
}

void HdfsScanNode::ComputeSlotMaterializationOrder(vector<int>* order) const {
  const vector<Expr*>& conjuncts = ExecNode::conjuncts();
=======
  // Initialize is_materialized_col_
  is_materialized_col_.resize(num_cols);
  for (int i = 0; i < num_cols; ++i) {
    is_materialized_col_[i] = column_idx_to_materialized_slot_idx_[i] != SKIP_COLUMN;
  }
  // TODO: don't assume all partitions are on the same filesystem.
  RETURN_IF_ERROR(HdfsFsCache::instance()->GetConnection(
      hdfs_table_->hdfs_base_dir(), &hdfs_connection_));
  // Convert the TScanRangeParams into per-file DiskIO::ScanRange objects and populate
  // partition_ids_, file_descs_, and per_type_files_.
  DCHECK(scan_range_params_ != NULL)
      << "Must call SetScanRanges() before calling Prepare()";

  int num_ranges_missing_volume_id = 0;
  for (int i = 0; i < scan_range_params_->size(); ++i) {
    DCHECK((*scan_range_params_)[i].scan_range.__isset.hdfs_file_split);
    const THdfsFileSplit& split = (*scan_range_params_)[i].scan_range.hdfs_file_split;
    partition_ids_.insert(split.partition_id);
    HdfsPartitionDescriptor* partition_desc =
        hdfs_table_->GetPartition(split.partition_id);
    filesystem::path file_path(partition_desc->location());
    file_path.append(split.file_name, filesystem::path::codecvt());
    const string& native_file_path = file_path.native();

    HdfsFileDesc* file_desc = NULL;
    FileDescMap::iterator file_desc_it = file_descs_.find(native_file_path);
    if (file_desc_it == file_descs_.end()) {
      // Add new file_desc to file_descs_ and per_type_files_
      file_desc = runtime_state_->obj_pool()->Add(new HdfsFileDesc(native_file_path));
      file_descs_[native_file_path] = file_desc;
      file_desc->file_length = split.file_length;
      file_desc->file_compression = split.file_compression;

      if (partition_desc == NULL) {
        stringstream ss;
        ss << "Could not find partition with id: " << split.partition_id;
        return Status(ss.str());
      }
      ++num_unqueued_files_;
      per_type_files_[partition_desc->file_format()].push_back(file_desc);
    } else {
      // File already processed
      file_desc = file_desc_it->second;
    }

    if ((*scan_range_params_)[i].volume_id == -1) {
      if (!unknown_disk_id_warned_) {
        AddRuntimeExecOption("Missing Volume Id");
        runtime_state()->LogError(
          "Unknown disk id.  This will negatively affect performance. "
          "Check your hdfs settings to enable block location metadata.");
        unknown_disk_id_warned_ = true;
      }
      ++num_ranges_missing_volume_id;
    }

    bool try_cache = (*scan_range_params_)[i].is_cached;
    bool expected_local = (*scan_range_params_)[i].__isset.is_remote &&
                          !(*scan_range_params_)[i].is_remote;
    if (runtime_state_->query_options().disable_cached_reads) {
      DCHECK(!try_cache) << "Params should not have had this set.";
    }
    file_desc->splits.push_back(
        AllocateScanRange(file_desc->filename.c_str(), split.length, split.offset,
                          split.partition_id, (*scan_range_params_)[i].volume_id,
                          try_cache, expected_local));
  }

  // Compute the minimum bytes required to start a new thread. This is based on the
  // file format.
  // The higher the estimate, the less likely it is the query will fail but more likely
  // the query will be throttled when it does not need to be.
  // TODO: how many buffers should we estimate per range. The IoMgr will throttle down to
  // one but if there are already buffers queued before memory pressure was hit, we can't
  // reclaim that memory.
  if (per_type_files_[THdfsFileFormat::PARQUET].size() > 0) {
    // Parquet files require buffers per column
    scanner_thread_bytes_required_ =
        materialized_slots_.size() * 3 * runtime_state_->io_mgr()->max_read_buffer_size();
  } else {
    scanner_thread_bytes_required_ =
        3 * runtime_state_->io_mgr()->max_read_buffer_size();
  }
  // scanner_thread_bytes_required_ now contains the IoBuffer requirement.
  // Next we add in the other memory the scanner thread will use.
  // e.g. decompression buffers, tuple buffers, etc.
  // For compressed text, we estimate this based on the file size (since the whole file
  // will need to be decompressed at once). For all other formats, we use a constant.
  // TODO: can we do something better?
  int64_t scanner_thread_mem_usage = SCANNER_THREAD_MEM_USAGE;
  BOOST_FOREACH(HdfsFileDesc* file, per_type_files_[THdfsFileFormat::TEXT]) {
    if (file->file_compression != THdfsCompression::NONE) {
      int64_t bytes_required = file->file_length * COMPRESSED_TEXT_COMPRESSION_RATIO;
      scanner_thread_mem_usage = ::max(bytes_required, scanner_thread_mem_usage);
    }
  }
  scanner_thread_bytes_required_ += scanner_thread_mem_usage;

  // Prepare all the partitions scanned by the scan node
  BOOST_FOREACH(const int64_t& partition_id, partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL);
    RETURN_IF_ERROR(partition_desc->PrepareExprs(state));
  }

  // Update server wide metrics for number of scan ranges and ranges that have
  // incomplete metadata.
  ImpaladMetrics::NUM_RANGES_PROCESSED->Increment(scan_range_params_->size());
  ImpaladMetrics::NUM_RANGES_MISSING_VOLUME_ID->Increment(num_ranges_missing_volume_id);

  // Add per volume stats to the runtime profile
  PerVolumnStats per_volume_stats;
  stringstream str;
  UpdateHdfsSplitStats(*scan_range_params_, &per_volume_stats);
  PrintHdfsSplitStats(per_volume_stats, &str);
  runtime_profile()->AddInfoString(HDFS_SPLIT_STATS_DESC, str.str());

  // Initialize conjunct exprs
  RETURN_IF_ERROR(Expr::CreateExprTrees(
      runtime_state_->obj_pool(), thrift_plan_node_->conjuncts, &conjunct_ctxs_));
  RETURN_IF_ERROR(
      Expr::Prepare(conjunct_ctxs_, runtime_state_, row_desc(), expr_mem_tracker()));
  AddExprCtxsToFree(conjunct_ctxs_);

  for (int format = THdfsFileFormat::TEXT;
       format <= THdfsFileFormat::PARQUET; ++format) {
    vector<HdfsFileDesc*>& file_descs =
        per_type_files_[static_cast<THdfsFileFormat::type>(format)];

    if (file_descs.empty()) continue;

    // Randomize the order this node processes the files. We want to do this to avoid
    // issuing remote reads to the same DN from different impalads. In file formats such
    // as avro/seq/rc (i.e. splittable with a header), every node first reads the header.
    // If every node goes through the files in the same order, all the remote reads are
    // for the same file meaning a few DN serves a lot of remote reads at the same time.
    random_shuffle(file_descs.begin(), file_descs.end());

    // Create reusable codegen'd functions for each file type type needed
    Function* fn;
    switch (format) {
      case THdfsFileFormat::TEXT:
        fn = HdfsTextScanner::Codegen(this, conjunct_ctxs_);
        break;
      case THdfsFileFormat::SEQUENCE_FILE:
        fn = HdfsSequenceScanner::Codegen(this, conjunct_ctxs_);
        break;
      case THdfsFileFormat::AVRO:
        fn = HdfsAvroScanner::Codegen(this, conjunct_ctxs_);
        break;
      default:
        // No codegen for this format
        fn = NULL;
    }
    if (fn != NULL) {
      LlvmCodeGen* codegen;
      RETURN_IF_ERROR(runtime_state_->GetCodegen(&codegen));
      codegen->AddFunctionToJit(
          fn, &codegend_fn_map_[static_cast<THdfsFileFormat::type>(format)]);
    }
  }

  return Status::OK;
}

// This function initiates the connection to hdfs and starts up the initial scanner
// threads. The scanner subclasses are passed the initial splits.  Scanners are expected
// to queue up a non-zero number of those splits to the io mgr (via the ScanNode).
Status HdfsScanNode::Open(RuntimeState* state) {
  RETURN_IF_ERROR(ExecNode::Open(state));

  // We need at least one scanner thread to make progress. We need to make this
  // reservation before any ranges are issued.
  runtime_state_->resource_pool()->ReserveOptionalTokens(1);
  if (runtime_state_->query_options().num_scanner_threads > 0) {
    runtime_state_->resource_pool()->set_max_quota(
        runtime_state_->query_options().num_scanner_threads);
  }

  runtime_state_->resource_pool()->SetThreadAvailableCb(
      bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this, _1));

  if (runtime_state_->query_resource_mgr() != NULL) {
    rm_callback_id_ = runtime_state_->query_resource_mgr()->AddVcoreAvailableCb(
        bind<void>(mem_fn(&HdfsScanNode::ThreadTokenAvailableCb), this,
            runtime_state_->resource_pool()));
  }

  if (file_descs_.empty()) {
    SetDone();
    return Status::OK;
  }

  // Open all the partition exprs used by the scan node
  BOOST_FOREACH(const int64_t& partition_id, partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL);
    RETURN_IF_ERROR(partition_desc->OpenExprs(state));
  }

  // Open all conjuncts
  Expr::Open(conjunct_ctxs_, state);

  RETURN_IF_ERROR(runtime_state_->io_mgr()->RegisterContext(
      hdfs_connection_, &reader_context_, mem_tracker()));

  // Initialize HdfsScanNode specific counters
  read_timer_ = ADD_TIMER(runtime_profile(), TOTAL_HDFS_READ_TIMER);
  per_read_thread_throughput_counter_ = runtime_profile()->AddDerivedCounter(
      PER_READ_THREAD_THROUGHPUT_COUNTER, TCounterType::BYTES_PER_SECOND,
      bind<int64_t>(&RuntimeProfile::UnitsPerSecond, bytes_read_counter_, read_timer_));
  scan_ranges_complete_counter_ =
      ADD_COUNTER(runtime_profile(), SCAN_RANGES_COMPLETE_COUNTER, TCounterType::UNIT);
  if (DiskInfo::num_disks() < 64) {
    num_disks_accessed_counter_ =
        ADD_COUNTER(runtime_profile(), NUM_DISKS_ACCESSED_COUNTER, TCounterType::UNIT);
  } else {
    num_disks_accessed_counter_ = NULL;
  }
  num_scanner_threads_started_counter_ =
      ADD_COUNTER(runtime_profile(), NUM_SCANNER_THREADS_STARTED, TCounterType::UNIT);

  runtime_state_->io_mgr()->set_bytes_read_counter(reader_context_, bytes_read_counter());
  runtime_state_->io_mgr()->set_read_timer(reader_context_, read_timer());
  runtime_state_->io_mgr()->set_active_read_thread_counter(reader_context_,
      &active_hdfs_read_thread_counter_);
  runtime_state_->io_mgr()->set_disks_access_bitmap(reader_context_,
      &disks_accessed_bitmap_);

  average_scanner_thread_concurrency_ = runtime_profile()->AddSamplingCounter(
      AVERAGE_SCANNER_THREAD_CONCURRENCY, &active_scanner_thread_counter_);
  average_hdfs_read_thread_concurrency_ = runtime_profile()->AddSamplingCounter(
      AVERAGE_HDFS_READ_THREAD_CONCURRENCY, &active_hdfs_read_thread_counter_);

  bytes_read_local_ = ADD_COUNTER(runtime_profile(), "BytesReadLocal",
      TCounterType::BYTES);
  bytes_read_short_circuit_ = ADD_COUNTER(runtime_profile(), "BytesReadShortCircuit",
      TCounterType::BYTES);
  bytes_read_dn_cache_ = ADD_COUNTER(runtime_profile(), "BytesReadDataNodeCache",
      TCounterType::BYTES);
  num_remote_ranges_ = ADD_COUNTER(runtime_profile(), "RemoteScanRanges",
      TCounterType::UNIT);
  unexpected_remote_bytes_ = ADD_COUNTER(runtime_profile(), "BytesReadRemoteUnexpected",
      TCounterType::BYTES);

  max_compressed_text_file_length_ = runtime_profile()->AddHighWaterMarkCounter(
      "MaxCompressedTextFileLength", TCounterType::BYTES);

  // Create num_disks+1 bucket counters
  for (int i = 0; i < state->io_mgr()->num_disks() + 1; ++i) {
    hdfs_read_thread_concurrency_bucket_.push_back(
        pool_->Add(new RuntimeProfile::Counter(TCounterType::DOUBLE_VALUE, 0)));
  }
  runtime_profile()->RegisterBucketingCounters(&active_hdfs_read_thread_counter_,
      &hdfs_read_thread_concurrency_bucket_);

  counters_running_ = true;

  int total_splits = 0;
  for (FileDescMap::iterator it = file_descs_.begin(); it != file_descs_.end(); ++it) {
    total_splits += it->second->splits.size();
  }

  if (total_splits == 0) {
    SetDone();
    return Status::OK;
  }

  stringstream ss;
  ss << "Splits complete (node=" << id() << "):";
  progress_ = ProgressUpdater(ss.str(), total_splits);

  return Status::OK;
}

void HdfsScanNode::Close(RuntimeState* state) {
  if (is_closed()) return;
  SetDone();

  state->resource_pool()->SetThreadAvailableCb(NULL);
  if (state->query_resource_mgr() != NULL && rm_callback_id_ != -1) {
    state->query_resource_mgr()->RemoveVcoreAvailableCb(rm_callback_id_);
  }

  scanner_threads_.JoinAll();

  num_owned_io_buffers_ -= materialized_row_batches_->Cleanup();
  DCHECK_EQ(num_owned_io_buffers_, 0) << "ScanNode has leaked io buffers";

  if (reader_context_ != NULL) {
    // There may still be io buffers used by parent nodes so we can't unregister the
    // reader context yet. The runtime state keeps a list of all the reader contexts and
    // they are unregistered when the fragment is closed.
    state->reader_contexts()->push_back(reader_context_);
    // Need to wait for all the active scanner threads to finish to ensure there is no
    // more memory tracked by this scan node's mem tracker.
    state->io_mgr()->CancelContext(reader_context_, true);
  }

  StopAndFinalizeCounters();

  // There should be no active scanner threads and hdfs read threads.
  DCHECK_EQ(active_scanner_thread_counter_.value(), 0);
  DCHECK_EQ(active_hdfs_read_thread_counter_.value(), 0);

  if (scan_node_pool_.get() != NULL) scan_node_pool_->FreeAll();

  // Close all conjuncts
  Expr::Close(conjunct_ctxs_, state);

  // Close all the partitions scanned by the scan node
  BOOST_FOREACH(const int64_t& partition_id, partition_ids_) {
    HdfsPartitionDescriptor* partition_desc = hdfs_table_->GetPartition(partition_id);
    DCHECK(partition_desc != NULL);
    partition_desc->CloseExprs(state);
  }

  ScanNode::Close(state);
}

Status HdfsScanNode::AddDiskIoRanges(const vector<DiskIoMgr::ScanRange*>& ranges) {
  RETURN_IF_ERROR(
      runtime_state_->io_mgr()->AddScanRanges(reader_context_, ranges));
  ThreadTokenAvailableCb(runtime_state_->resource_pool());
  return Status::OK;
}

Status HdfsScanNode::AddDiskIoRanges(const HdfsFileDesc* desc) {
  const vector<DiskIoMgr::ScanRange*>& ranges = desc->splits;
  RETURN_IF_ERROR(
      runtime_state_->io_mgr()->AddScanRanges(reader_context_, ranges));
  MarkFileDescIssued(desc);
  ThreadTokenAvailableCb(runtime_state_->resource_pool());
  return Status::OK;
}

void HdfsScanNode::MarkFileDescIssued(const HdfsFileDesc* desc) {
  DCHECK_GT(num_unqueued_files_, 0);
  --num_unqueued_files_;
}

void HdfsScanNode::AddMaterializedRowBatch(RowBatch* row_batch) {
  materialized_row_batches_->AddBatch(row_batch);
}

Status HdfsScanNode::GetConjunctCtxs(vector<ExprContext*>* ctxs) {
  return Expr::Clone(conjunct_ctxs_, runtime_state_, ctxs);
}

// For controlling the amount of memory used for scanners, we approximate the
// scanner mem usage based on scanner_thread_bytes_required_, rather than the
// consumption in the scan node's mem tracker. The problem with the scan node
// trackers is that it does not account for the memory the scanner will use.
// For example, if there is 110 MB of memory left (based on the mem tracker)
// and we estimate that a scanner will use 100MB, we want to make sure to only
// start up one additional thread. However, after starting the first thread, the
// mem tracker value will not change immediately (it takes some time before the
// scanner is running and starts using memory). Therefore we just use the estimate
// based on the number of running scanner threads.
bool HdfsScanNode::EnoughMemoryForScannerThread(bool new_thread) {
  int64_t committed_scanner_mem =
      active_scanner_thread_counter_.value() * scanner_thread_bytes_required_;
  int64_t tracker_consumption = mem_tracker()->consumption();
  int64_t est_additional_scanner_mem = committed_scanner_mem - tracker_consumption;
  if (est_additional_scanner_mem < 0) {
    // This is the case where our estimate was too low. Expand the estimate based
    // on the usage.
    int64_t avg_consumption =
        tracker_consumption / active_scanner_thread_counter_.value();
    // Take the average and expand it by 50%. Some scanners will not have hit their
    // steady state mem usage yet.
    // TODO: how can we scale down if we've overestimated.
    // TODO: better heuristic?
    scanner_thread_bytes_required_ = static_cast<int64_t>(avg_consumption * 1.5);
    est_additional_scanner_mem = 0;
  }

  // If we are starting a new thread, take that into account now.
  if (new_thread) est_additional_scanner_mem += scanner_thread_bytes_required_;
  return est_additional_scanner_mem < mem_tracker()->SpareCapacity();
}

void HdfsScanNode::ThreadTokenAvailableCb(ThreadResourceMgr::ResourcePool* pool) {
  // This is called to start up new scanner threads. It's not a big deal if we
  // spin up more than strictly necessary since they will go through and terminate
  // promptly. However, we want to minimize that by checking a conditions.
  //  1. Don't start up if the ScanNode is done
  //  2. Don't start up if all the ranges have been taken by another thread.
  //  3. Don't start up if the number of ranges left is less than the number of
  //     active scanner threads.
  //  4. Don't start up a ScannerThread if materialized_row_batches_ is full since
  //     we are not scanner bound.
  //  5. Don't start up a thread if there isn't enough memory left to run it.
  //  6. Don't start up if there are no thread tokens.
  //  7. Don't start up if we are running too many threads for our vcore allocation
  //  (unless the thread is reserved, in which case it has to run).
  bool started_scanner = false;
  while (true) {
    // The lock must be given up between loops in order to give writers to done_,
    // all_ranges_started_ etc. a chance to grab the lock.
    // TODO: This still leans heavily on starvation-free locks, come up with a more
    // correct way to communicate between this method and ScannerThreadHelper
    unique_lock<mutex> lock(lock_);
    // Cases 1, 2, 3
    if (done_ || all_ranges_started_ ||
      active_scanner_thread_counter_.value() >= progress_.remaining()) {
      break;
    }

    // Cases 4 and 5
    if (active_scanner_thread_counter_.value() > 0 &&
        (materialized_row_batches_->GetSize() >= max_materialized_row_batches_ ||
         !EnoughMemoryForScannerThread(true))) {
      break;
    }

    // Case 6
    bool is_reserved = false;
    if (!pool->TryAcquireThreadToken(&is_reserved)) break;

    // Case 7
    if (!is_reserved) {
      if (runtime_state_->query_resource_mgr() != NULL &&
          runtime_state_->query_resource_mgr()->IsVcoreOverSubscribed()) {
        break;
      }
    }

    COUNTER_ADD(&active_scanner_thread_counter_, 1);
    COUNTER_ADD(num_scanner_threads_started_counter_, 1);
    stringstream ss;
    ss << "scanner-thread(" << num_scanner_threads_started_counter_->value() << ")";
    scanner_threads_.AddThread(
        new Thread("hdfs-scan-node", ss.str(), &HdfsScanNode::ScannerThread, this));
    started_scanner = true;

    if (runtime_state_->query_resource_mgr() != NULL) {
      runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(1);
    }
  }
  if (!started_scanner) ++num_skipped_tokens_;
}

void HdfsScanNode::ScannerThread() {
  SCOPED_THREAD_COUNTER_MEASUREMENT(scanner_thread_counters());
  SCOPED_TIMER(runtime_state_->total_cpu_timer());

  while (!done_) {
    {
      // Check if we have enough resources (thread token and memory) to keep using
      // this thread.
      unique_lock<mutex> l(lock_);
      if (active_scanner_thread_counter_.value() > 1) {
        if (runtime_state_->resource_pool()->optional_exceeded() ||
            !EnoughMemoryForScannerThread(false)) {
          // We can't break here. We need to update the counter with the lock held or else
          // all threads might see active_scanner_thread_counter_.value > 1
          COUNTER_ADD(&active_scanner_thread_counter_, -1);
          // Unlock before releasing the thread token to avoid deadlock in
          // ThreadTokenAvailableCb().
          l.unlock();
          if (runtime_state_->query_resource_mgr() != NULL) {
            runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(-1);
          }
          runtime_state_->resource_pool()->ReleaseThreadToken(false);
          return;
        }
      } else {
        // If this is the only scanner thread, it should keep running regardless
        // of resource constraints.
      }
    }

    DiskIoMgr::ScanRange* scan_range;
    // Take a snapshot of num_unqueued_files_ before calling GetNextRange().
    // We don't want num_unqueued_files_ to go to zero between the return from
    // GetNextRange() and the check for when all ranges are complete.
    int num_unqueued_files = num_unqueued_files_;
    AtomicUtil::MemoryBarrier();
    Status status = runtime_state_->io_mgr()->GetNextRange(reader_context_, &scan_range);

    if (status.ok() && scan_range != NULL) {
      // Got a scan range. Create a new scanner object and process the range
      // end to end (in this thread).
      ScanRangeMetadata* metadata =
          reinterpret_cast<ScanRangeMetadata*>(scan_range->meta_data());
      int64_t partition_id = metadata->partition_id;
      HdfsPartitionDescriptor* partition = hdfs_table_->GetPartition(partition_id);
      DCHECK(partition != NULL);

      ScannerContext* context = runtime_state_->obj_pool()->Add(
          new ScannerContext(runtime_state_, this, partition, scan_range));
      Status scanner_status;
      HdfsScanner* scanner = CreateAndPrepareScanner(partition, context, &scanner_status);
      if (!scanner_status.ok() || scanner == NULL) {
        stringstream ss;
        ss << "Error preparing text scanner for scan range " << scan_range->file() <<
            "(" << scan_range->offset() << ":" << scan_range->len() << ").";
        ss << endl << runtime_state_->ErrorLog();
        VLOG_QUERY << ss.str();
      }

      status = scanner->ProcessSplit();
      if (!status.ok()) {
        // This thread hit an error, record it and bail
        // TODO: better way to report errors?  Maybe via the thrift interface?
        if (VLOG_QUERY_IS_ON && !runtime_state_->error_log().empty()) {
          stringstream ss;
          ss << "Scan node (id=" << id() << ") ran into a parse error for scan range "
             << scan_range->file() << "(" << scan_range->offset() << ":"
             << scan_range->len() << ").";
          if (partition->file_format() != THdfsFileFormat::PARQUET) {
            // Parquet doesn't read the range end to end so the current offset
            // isn't useful.
            // TODO: make sure the parquet reader is outputting as much diagnostic
            // information as possible.
            ScannerContext::Stream* stream = context->GetStream();
            ss << "  Processed " << stream->total_bytes_returned() << " bytes.";
          }
          ss << endl << runtime_state_->ErrorLog();
          VLOG_QUERY << ss.str();
        }
      }
      scanner->Close();
    }

    if (!status.ok()) {
      {
        unique_lock<mutex> l(lock_);
        // If there was already an error, the main thread will do the cleanup
        if (!status_.ok()) break;

        if (status.IsCancelled()) {
          // Scan node should be the only thing that initiated scanner threads to see
          // cancelled (i.e. limit reached).  No need to do anything here.
          DCHECK(done_);
          break;
        }
        status_ = status;
      }

      if (status.IsMemLimitExceeded()) runtime_state_->SetMemLimitExceeded();
      SetDone();
      break;
    }

    // Done with range and it completed successfully
    if (progress_.done()) {
      // All ranges are finished.  Indicate we are done.
      SetDone();
      break;
    }

    if (scan_range == NULL && num_unqueued_files == 0) {
      unique_lock<mutex> l(lock_);
      // All ranges have been queued and GetNextRange() returned NULL. This
      // means that every range is either done or being processed by
      // another thread.
      all_ranges_started_ = true;
      break;
    }
  }

  COUNTER_ADD(&active_scanner_thread_counter_, -1);
  if (runtime_state_->query_resource_mgr() != NULL) {
    runtime_state_->query_resource_mgr()->NotifyThreadUsageChange(-1);
  }
  runtime_state_->resource_pool()->ReleaseThreadToken(false);
}

void HdfsScanNode::RangeComplete(const THdfsFileFormat::type& file_type,
    const THdfsCompression::type& compression_type) {
  vector<THdfsCompression::type> types;
  types.push_back(compression_type);
  RangeComplete(file_type, types);
}

void HdfsScanNode::RangeComplete(const THdfsFileFormat::type& file_type,
    const vector<THdfsCompression::type>& compression_types) {
  scan_ranges_complete_counter()->Add(1);
  progress_.Update(1);

  {
    ScopedSpinLock l(&file_type_counts_lock_);
    for (int i = 0; i < compression_types.size(); ++i) {
      ++file_type_counts_[make_pair(file_type, compression_types[i])];
    }
  }
}

void HdfsScanNode::SetDone() {
  {
    unique_lock<mutex> l(lock_);
    if (done_) return;
    done_ = true;
  }
  if (reader_context_ != NULL) {
    runtime_state_->io_mgr()->CancelContext(reader_context_);
  }
  materialized_row_batches_->Shutdown();
}

void HdfsScanNode::ComputeSlotMaterializationOrder(vector<int>* order) const {
  const vector<ExprContext*>& conjuncts = ExecNode::conjunct_ctxs();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Initialize all order to be conjuncts.size() (after the last conjunct)
  order->insert(order->begin(), materialized_slots().size(), conjuncts.size());

  const DescriptorTbl& desc_tbl = runtime_state_->desc_tbl();

  vector<SlotId> slot_ids;
  for (int conjunct_idx = 0; conjunct_idx < conjuncts.size(); ++conjunct_idx) {
    slot_ids.clear();
<<<<<<< HEAD
    int num_slots = conjuncts[conjunct_idx]->GetSlotIds(&slot_ids);
=======
    int num_slots = conjuncts[conjunct_idx]->root()->GetSlotIds(&slot_ids);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    for (int j = 0; j < num_slots; ++j) {
      SlotDescriptor* slot_desc = desc_tbl.GetSlotDescriptor(slot_ids[j]);
      int slot_idx = GetMaterializedSlotIdx(slot_desc->col_pos());
      // slot_idx == -1 means this was a partition key slot which is always
      // materialized before any slots.
      if (slot_idx == -1) continue;
      // If this slot hasn't been assigned an order, assign it be materialized
      // before evaluating conjuncts[i]
      if ((*order)[slot_idx] == conjuncts.size()) {
        (*order)[slot_idx] = conjunct_idx;
      }
    }
  }
}

<<<<<<< HEAD
=======
void HdfsScanNode::StopAndFinalizeCounters() {
  unique_lock<mutex> l(lock_);
  if (!counters_running_) return;
  counters_running_ = false;

  PeriodicCounterUpdater::StopTimeSeriesCounter(bytes_read_timeseries_counter_);
  PeriodicCounterUpdater::StopRateCounter(total_throughput_counter());
  PeriodicCounterUpdater::StopSamplingCounter(average_scanner_thread_concurrency_);
  PeriodicCounterUpdater::StopSamplingCounter(average_hdfs_read_thread_concurrency_);
  PeriodicCounterUpdater::StopBucketingCounters(&hdfs_read_thread_concurrency_bucket_,
      true);

  // Output hdfs read thread concurrency into info string
  stringstream ss;
  for (int i = 0; i < hdfs_read_thread_concurrency_bucket_.size(); ++i) {
    ss << i << ":" << setprecision(4)
       << hdfs_read_thread_concurrency_bucket_[i]->double_value() << "% ";
  }
  runtime_profile_->AddInfoString("Hdfs Read Thread Concurrency Bucket", ss.str());

  // Convert disk access bitmap to num of disk accessed
  uint64_t num_disk_bitmap = disks_accessed_bitmap_.value();
  int64_t num_disk_accessed = BitUtil::Popcount(num_disk_bitmap);
  if (num_disks_accessed_counter_ != NULL) {
    num_disks_accessed_counter_->Set(num_disk_accessed);
  }

  // output completed file types and counts to info string
  if (!file_type_counts_.empty()) {
    stringstream ss;
    {
      ScopedSpinLock l2(&file_type_counts_lock_);
      for (FileTypeCountsMap::const_iterator it = file_type_counts_.begin();
          it != file_type_counts_.end(); ++it) {
        ss << it->first.first << "/" << it->first.second << ":" << it->second << " ";
      }
    }
    runtime_profile_->AddInfoString("File Formats", ss.str());
  }

  // Output fraction of scanners with codegen enabled
  ss.str(std::string());
  ss << "Codegen enabled: " << num_scanners_codegen_enabled_ << " out of "
     << (num_scanners_codegen_enabled_ + num_scanners_codegen_disabled_);
  AddRuntimeExecOption(ss.str());

  if (reader_context_ != NULL) {
    bytes_read_local_->Set(runtime_state_->io_mgr()->bytes_read_local(reader_context_));
    bytes_read_short_circuit_->Set(
        runtime_state_->io_mgr()->bytes_read_short_circuit(reader_context_));
    bytes_read_dn_cache_->Set(
        runtime_state_->io_mgr()->bytes_read_dn_cache(reader_context_));
    num_remote_ranges_->Set(static_cast<int64_t>(
        runtime_state_->io_mgr()->num_remote_ranges(reader_context_)));
    unexpected_remote_bytes_->Set(
        runtime_state_->io_mgr()->unexpected_remote_bytes(reader_context_));

    if (unexpected_remote_bytes_->value() > 0) {
      runtime_state_->LogError(Substitute(
          "Block locality metadata for table '$0' may be stale. Consider running "
          "\"INVALIDATE METADATA $0\".", hdfs_table_->name()));
    }

    ImpaladMetrics::IO_MGR_BYTES_READ->Increment(bytes_read_counter()->value());
    ImpaladMetrics::IO_MGR_LOCAL_BYTES_READ->Increment(
        bytes_read_local_->value());
    ImpaladMetrics::IO_MGR_SHORT_CIRCUIT_BYTES_READ->Increment(
        bytes_read_short_circuit_->value());
  }
}

void HdfsScanNode::UpdateHdfsSplitStats(
    const vector<TScanRangeParams>& scan_range_params_list,
    PerVolumnStats* per_volume_stats) {
  pair<int, int64_t> init_value(0, 0);
  BOOST_FOREACH(const TScanRangeParams& scan_range_params, scan_range_params_list) {
    const TScanRange& scan_range = scan_range_params.scan_range;
    if (!scan_range.__isset.hdfs_file_split) continue;
    const THdfsFileSplit& split = scan_range.hdfs_file_split;
    pair<int, int64_t>* stats =
        FindOrInsert(per_volume_stats, scan_range_params.volume_id, init_value);
    ++(stats->first);
    stats->second += split.length;
  }
}

void HdfsScanNode::PrintHdfsSplitStats(const PerVolumnStats& per_volume_stats,
    stringstream* ss) {
  for (PerVolumnStats::const_iterator i = per_volume_stats.begin();
       i != per_volume_stats.end(); ++i) {
     (*ss) << i->first << ":" << i->second.first << "/"
         << PrettyPrinter::Print(i->second.second, TCounterType::BYTES) << " ";
  }
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
