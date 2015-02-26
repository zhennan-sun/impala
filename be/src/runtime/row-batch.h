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


#ifndef IMPALA_RUNTIME_ROW_BATCH_H
#define IMPALA_RUNTIME_ROW_BATCH_H

#include <vector>
#include <cstring>
#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "runtime/descriptors.h"
#include "runtime/disk-io-mgr.h"
#include "runtime/mem-pool.h"
<<<<<<< HEAD

namespace impala {

=======
#include "runtime/mem-tracker.h"

namespace impala {

class BufferedTupleStream;
class MemTracker;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class TRowBatch;
class Tuple;
class TupleRow;
class TupleDescriptor;

// A RowBatch encapsulates a batch of rows, each composed of a number of tuples.
<<<<<<< HEAD
// The maximum number of rows is fixed at the time of construction, and the caller
// can add rows up to that capacity.
// The row batch reference a few different sources of memory.
//   1. TupleRow ptrs - this is always owned and managed by the row batch.
//   2. Tuple memory - this is allocated (or transferred to) the row batches tuple pool.
//   3. Auxillary tuple memory (e.g. string data) - this can either be stored externally
//      (don't copy strings) or from the tuple pool (strings are copied).  If external,
//      the data is in an io buffer that may not be attached to this row batch.  The
//      creator of that row batch has to make sure that the io buffer is not recycled
//      until all batches that reference the memory have been consumed.  
// TODO: stick tuple_ptrs_ into a pool?
=======
// The maximum number of rows is fixed at the time of construction.
// The row batch reference a few different sources of memory.
//   1. TupleRow ptrs - this is always owned and managed by the row batch.
//   2. Tuple memory - this is allocated (or transferred to) the row batches tuple pool.
//   3. Auxiliary tuple memory (e.g. string data) - this can either be stored externally
//      (don't copy strings) or from the tuple pool (strings are copied).  If external,
//      the data is in an io buffer that may not be attached to this row batch.  The
//      creator of that row batch has to make sure that the io buffer is not recycled
//      until all batches that reference the memory have been consumed.
// In order to minimize memory allocations, RowBatches and TRowBatches that have been
// serialized and sent over the wire should be reused (this prevents compression_scratch_
// from being needlessly reallocated).
//
// Row batches and memory usage: We attempt to stream row batches through the plan
// tree without copying the data. This means that row batches are often not-compact
// and reference memory outside of the row batch. This results in most row batches
// having a very small memory footprint and in some row batches having a very large
// one (it contains all the memory that other row batches are referencing). An example
// is IoBuffers which are only attached to one row batch. Only when the row batch reaches
// a blocking operator or the root of the fragment is the row batch memory freed.
// This means that in some cases (e.g. very selective queries), we still need to
// pass the row batch through the exec nodes (even if they have no rows) to trigger
// memory deletion. AtCapacity() encapsulates the check that we are not accumulating
// excessive memory.
//
// A row batch is considered at capacity if all the rows are full or it has accumulated
// auxiliary memory up to a soft cap. (See at_capacity_mem_usage_ comment).
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
class RowBatch {
 public:
  // Create RowBatch for a maximum of 'capacity' rows of tuples specified
  // by 'row_desc'.
<<<<<<< HEAD
  RowBatch(const RowDescriptor& row_desc, int capacity)
    : has_in_flight_row_(false),
      is_self_contained_(false),
      num_rows_(0),
      capacity_(capacity),
      num_tuples_per_row_(row_desc.tuple_descriptors().size()),
      row_desc_(row_desc),
      tuple_data_pool_(new MemPool()) {
    tuple_ptrs_size_ = capacity_ * num_tuples_per_row_ * sizeof(Tuple*);
    tuple_ptrs_ = new Tuple*[capacity_ * num_tuples_per_row_];
    DCHECK_GT(capacity, 0);
  }

  // Populate a row batch from input_batch by copying input_batch's
  // tuple_data into the row batch's mempool and converting all offsets
  // in the data back into pointers. The row batch will be self-contained after the call.
  // TODO: figure out how to transfer the data from input_batch to this RowBatch
  // (so that we don't need to make yet another copy)
  RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch);
=======
  // tracker cannot be NULL.
  RowBatch(const RowDescriptor& row_desc, int capacity, MemTracker* tracker);

  // Populate a row batch from input_batch by copying input_batch's
  // tuple_data into the row batch's mempool and converting all offsets
  // in the data back into pointers.
  // TODO: figure out how to transfer the data from input_batch to this RowBatch
  // (so that we don't need to make yet another copy)
  RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch,
      MemTracker* tracker);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Releases all resources accumulated at this row batch.  This includes
  //  - tuple_ptrs
  //  - tuple mem pool data
  //  - buffer handles from the io mgr
  ~RowBatch();

  static const int INVALID_ROW_INDEX = -1;

  // Add n rows of tuple pointers after the last committed row and return its index.
  // The rows are uninitialized and each tuple of the row must be set.
  // Returns INVALID_ROW_INDEX if the row batch cannot fit n rows.
  // Two consecutive AddRow() calls without a CommitLastRow() between them
  // have the same effect as a single call.
  int AddRows(int n) {
    if (num_rows_ + n > capacity_) return INVALID_ROW_INDEX;
    has_in_flight_row_ = true;
    return num_rows_;
  }

  int AddRow() { return AddRows(1); }

  void CommitRows(int n) {
<<<<<<< HEAD
=======
    DCHECK_GE(n, 0);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    DCHECK_LE(num_rows_ + n, capacity_);
    num_rows_ += n;
    has_in_flight_row_ = false;
  }
<<<<<<< HEAD
  
=======

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  void CommitLastRow() { CommitRows(1); }

  // Set function can be used to reduce the number of rows in the batch.  This is only
  // used in the limit case where more rows were added than necessary.
  void set_num_rows(int num_rows) {
    DCHECK_LE(num_rows, num_rows_);
    num_rows_ = num_rows;
  }

<<<<<<< HEAD
  // Returns true if row_batch has reached capacity or there are io buffers attached to
  // this batch, false otherwise
  // IO buffers are a scarce resource and must be recycled as quickly as possible to get
  // the best cpu/disk interleaving.
  bool IsFull() {
    return num_rows_ == capacity_ || !io_buffers_.empty();
  }

  int row_byte_size() {
    return num_tuples_per_row_ * sizeof(Tuple*);
  }

  TupleRow* GetRow(int row_idx) {
=======
  // Returns true if the row batch has filled all the rows or has accumulated
  // enough memory.
  bool AtCapacity() {
    return num_rows_ == capacity_ || auxiliary_mem_usage_ >= AT_CAPACITY_MEM_USAGE ||
      num_tuple_streams() > 0 || need_to_return_;
  }

  // Returns true if the row batch has filled all the rows or has accumulated
  // enough memory. tuple_pool is an intermediate memory pool containing tuple data
  // that will eventually be attached to this row batch. We need to make sure
  // the tuple pool does not accumulate excessive memory.
  bool AtCapacity(MemPool* tuple_pool) {
    DCHECK(tuple_pool != NULL);
    return AtCapacity() ||
        (tuple_pool->total_allocated_bytes() > AT_CAPACITY_MEM_USAGE && num_rows_ > 0);
  }

  // The total size of all data represented in this row batch (tuples and referenced
  // string data). This is the size of the row batch after removing all gaps in the
  // auxiliary (i.e. the smallest footprint for the row batch).
  int TotalByteSize();

  TupleRow* GetRow(int row_idx) {
    DCHECK(tuple_ptrs_ != NULL);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    DCHECK_GE(row_idx, 0);
    DCHECK_LT(row_idx, num_rows_ + (has_in_flight_row_ ? 1 : 0));
    return reinterpret_cast<TupleRow*>(tuple_ptrs_ + row_idx * num_tuples_per_row_);
  }

<<<<<<< HEAD
  void Reset() {
    num_rows_ = 0;
    has_in_flight_row_ = false;
    tuple_data_pool_.reset(new MemPool());
    for (int i = 0; i < io_buffers_.size(); ++i) {
      io_buffers_[i]->Return();
    }
    io_buffers_.clear();
  }

  MemPool* tuple_data_pool() {
    return tuple_data_pool_.get();
  }

  void AddIoBuffer(DiskIoMgr::BufferDescriptor* buffer) {
    io_buffers_.push_back(buffer);
  }

  int num_io_buffers() {
    return io_buffers_.size();
  }

  // Transfer ownership of resources to dest.  This includes tuple data in mem
  // pool and io buffers.
  void TransferResourceOwnership(RowBatch* dest) {
    dest->tuple_data_pool_->AcquireData(tuple_data_pool_.get(), false);
    dest->io_buffers_.insert(dest->io_buffers_.begin(), 
        io_buffers_.begin(), io_buffers_.end());
    io_buffers_.clear();
    // make sure we can't access our tuples after we gave up the pools holding the
    // tuple data
    Reset();
  }
=======
  int row_byte_size() { return num_tuples_per_row_ * sizeof(Tuple*); }
  MemPool* tuple_data_pool() { return tuple_data_pool_.get(); }
  int num_io_buffers() const { return io_buffers_.size(); }
  int num_tuple_streams() const { return tuple_streams_.size(); }

  // Resets the row batch, returning all resources it has accumulated.
  void Reset();

  // Add io buffer to this row batch.
  void AddIoBuffer(DiskIoMgr::BufferDescriptor* buffer);

  // Add tuple stream to this row batch. The row batch must call Close() on the stream
  // when freeing resources.
  void AddTupleStream(BufferedTupleStream* stream);

  // Called to indicate this row batch must be returned up the operator tree.
  // This is used to control memory management for streaming rows.
  // TODO: consider using this mechanism instead of AddIoBuffer/AddTupleStream. This is
  // the property we need rather than meticulously passing resources up so the operator
  // tree.
  void MarkNeedToReturn() { need_to_return_ = true; }

  // Transfer ownership of resources to dest.  This includes tuple data in mem
  // pool and io buffers.
  void TransferResourceOwnership(RowBatch* dest);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  void CopyRow(TupleRow* src, TupleRow* dest) {
    memcpy(dest, src, num_tuples_per_row_ * sizeof(Tuple*));
  }

<<<<<<< HEAD
  void ClearRow(TupleRow* row) {
    memset(row, 0, num_tuples_per_row_ * sizeof(Tuple*));
  }

  void ClearBatch() {
    memset(tuple_ptrs_, 0, capacity_ * num_tuples_per_row_ * sizeof(Tuple*));
  }

  // Swaps all of the row batch state with 'other'.  This is used for scan nodes
  // which produce RowBatches asynchronously.  Typically, an ExecNode is handed
  // a row batch to populate (pull model) but ScanNodes have multiple threads
  // which push row batches.  This function is used to swap the pushed row batch
  // contents with the row batch that's passed from the caller.
  // TODO: this is wasteful and makes a copy that's unnecessary.  Think about cleaning
  // this up.
  void Swap(RowBatch* other);

  // Create a serialized version of this row batch in output_batch, attaching
  // all of the data it references (TRowBatch::tuple_data) to output_batch.tuple_data.
  // If an in-flight row is present in this row batch, it is ignored.
  // If this batch is self-contained, it simply does an in-place conversion of the
  // string pointers contained in the tuple data into offsets and resets the batch
  // after copying the data to TRowBatch.
  void Serialize(TRowBatch* output_batch);

  // utility function: return total tuple data size of 'batch'.
=======
  // Copy 'num_rows' rows from 'src' to 'dest' within the batch. Useful for exec
  // nodes that skip an offset and copied more than necessary.
  void CopyRows(int dest, int src, int num_rows) {
    DCHECK_LE(dest, src);
    DCHECK_LE(src + num_rows, capacity_);
    memmove(tuple_ptrs_ + num_tuples_per_row_ * dest,
        tuple_ptrs_ + num_tuples_per_row_ * src,
        num_rows * num_tuples_per_row_ * sizeof(Tuple*));
  }

  void ClearRow(TupleRow* row) {
    memset(row, 0, num_tuples_per_row_ * sizeof(Tuple*));
  }

  // Acquires state from the 'src' row batch into this row batch. This includes all IO
  // buffers and tuple data.
  // This row batch must be empty and have the same row descriptor as the src batch.
  // This is used for scan nodes which produce RowBatches asynchronously.  Typically,
  // an ExecNode is handed a row batch to populate (pull model) but ScanNodes have
  // multiple threads which push row batches.
  // TODO: this is wasteful and makes a copy that's unnecessary.  Think about cleaning
  // this up.
  // TOOD: rename this or unify with TransferResourceOwnership()
  void AcquireState(RowBatch* src);

  // Create a serialized version of this row batch in output_batch, attaching all of the
  // data it references to output_batch.tuple_data. output_batch.tuple_data will be
  // snappy-compressed unless the compressed data is larger than the uncompressed
  // data. Use output_batch.is_compressed to determine whether tuple_data is compressed.
  // If an in-flight row is present in this row batch, it is ignored.
  // This function does not Reset().
  // Returns the uncompressed serialized size (this will be the true size of output_batch
  // if tuple_data is actually uncompressed).
  int Serialize(TRowBatch* output_batch);

  // Utility function: returns total size of batch.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  static int GetBatchSize(const TRowBatch& batch);

  int num_rows() const { return num_rows_; }
  int capacity() const { return capacity_; }

<<<<<<< HEAD
  // A self-contained row batch contains all of the tuple data it references
  // in tuple_data_pool_. The creator of the row batch needs to decide whether
  // it's self-contained (ie, this is not something that the row batch can
  // ascertain on its own).
  bool is_self_contained() const { return is_self_contained_; }
  void set_is_self_contained(bool v) { is_self_contained_ = v; }

  const RowDescriptor& row_desc() const { return row_desc_; }

 private:
  // All members need to be handled in RowBatch::Swap()

  bool has_in_flight_row_;  // if true, last row hasn't been committed yet
  bool is_self_contained_;  // if true, contains all ref'd data in tuple_data_pool_
=======
  const RowDescriptor& row_desc() const { return row_desc_; }

  // Max memory that this row batch can accumulate in tuple_data_pool_ before it
  // is considered at capacity.
  static const int AT_CAPACITY_MEM_USAGE;

  // Computes the maximum size needed to store tuple data for this row batch.
  int MaxTupleBufferSize();

 private:
  MemTracker* mem_tracker_;  // not owned

  // All members below need to be handled in RowBatch::AcquireState()

  bool has_in_flight_row_;  // if true, last row hasn't been committed yet
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  int num_rows_;  // # of committed rows
  int capacity_;  // maximum # of rows

  int num_tuples_per_row_;
  RowDescriptor row_desc_;

  // array of pointers (w/ capacity_ * num_tuples_per_row_ elements)
  // TODO: replace w/ tr1 array?
  Tuple** tuple_ptrs_;
  int tuple_ptrs_size_;

<<<<<<< HEAD
  // holding (some of the) data referenced by rows
  boost::scoped_ptr<MemPool> tuple_data_pool_;

  std::vector<DiskIoMgr::BufferDescriptor*> io_buffers_;
=======
  // Sum of all auxiliary bytes. This includes IoBuffers and memory from
  // TransferResourceOwnership().
  int64_t auxiliary_mem_usage_;

  // If true, this batch is considered at capacity. This is explicitly set by streaming
  // components that return rows via row batches.
  bool need_to_return_;

  // holding (some of the) data referenced by rows
  boost::scoped_ptr<MemPool> tuple_data_pool_;

  // IO buffers current owned by this row batch. Ownership of IO buffers transfer
  // between row batches. Any IO buffer will be owned by at most one row batch
  // (i.e. they are not ref counted) so most row batches don't own any.
  std::vector<DiskIoMgr::BufferDescriptor*> io_buffers_;

  // Tuple streams currently owned by this row batch.
  std::vector<BufferedTupleStream*> tuple_streams_;

  // String to write compressed tuple data to in Serialize().
  // This is a string so we can swap() with the string in the TRowBatch we're serializing
  // to (we don't compress directly into the TRowBatch in case the compressed data is
  // longer than the uncompressed data). Swapping avoids copying data to the TRowBatch and
  // avoids excess memory allocations: since we reuse RowBatchs and TRowBatchs, and
  // assuming all row batches are roughly the same size, all strings will eventually be
  // allocated to the right size.
  std::string compression_scratch_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

}

#endif
