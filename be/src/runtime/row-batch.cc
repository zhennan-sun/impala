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

#include "runtime/row-batch.h"

#include <stdint.h>  // for intptr_t
<<<<<<< HEAD

#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "gen-cpp/Data_types.h"

=======
#include <boost/scoped_ptr.hpp>

#include "runtime/buffered-tuple-stream.h"
#include "runtime/mem-tracker.h"
#include "runtime/string-value.h"
#include "runtime/tuple-row.h"
#include "util/compress.h"
#include "util/decompress.h"
#include "gen-cpp/Results_types.h"

using namespace boost;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
using namespace std;

namespace impala {

<<<<<<< HEAD
RowBatch::~RowBatch() {
  delete [] tuple_ptrs_;
  for (int i = 0; i < io_buffers_.size(); ++i) {
    io_buffers_[i]->Return();
  }
}

void RowBatch::Serialize(TRowBatch* output_batch) {
  // why does Thrift not generate a Clear() function?
  output_batch->row_tuples.clear();
  output_batch->tuple_offsets.clear();
  output_batch->tuple_data.clear();

  output_batch->num_rows = num_rows_;
  row_desc_.ToThrift(&output_batch->row_tuples);
  output_batch->tuple_offsets.reserve(num_rows_ * num_tuples_per_row_);
  MemPool output_pool;

  // iterate through all tuples;
  // for a self-contained batch, convert Tuple* and string pointers into offsets into
  // our tuple_data_pool_;
  // otherwise, copy the tuple data, including strings, into output_pool (converting
  // string pointers into offset in the process)
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* row = GetRow(i);
    const vector<TupleDescriptor*>& tuple_descs = row_desc_.tuple_descriptors();
    vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
    for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
      if (row->GetTuple(j) == NULL) {
        // NULLs are encoded as -1
        output_batch->tuple_offsets.push_back(-1);
        continue;
      }

      Tuple* t;
      if (is_self_contained_) {
        t = row->GetTuple(j);
        output_batch->tuple_offsets.push_back(
            tuple_data_pool_->GetOffset(reinterpret_cast<uint8_t*>(t)));

        // convert string pointers to offsets
        vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
        for (; slot != (*desc)->string_slots().end(); ++slot) {
          DCHECK_EQ((*slot)->type(), TYPE_STRING);
          StringValue* string_val = t->GetStringSlot((*slot)->tuple_offset());
          string_val->ptr = reinterpret_cast<char*>(tuple_data_pool_->GetOffset(
                  reinterpret_cast<uint8_t*>(string_val->ptr)));
        }
      } else  {
        // record offset before creating copy
        output_batch->tuple_offsets.push_back(output_pool.GetCurrentOffset());
        t = row->GetTuple(j)->DeepCopy(**desc, &output_pool, /* convert_ptrs */ true);
      }
    }
  }

  if (is_self_contained_) {
    output_pool.AcquireData(tuple_data_pool_.get(), false);
    Reset();  // we passed on all data
  }

  vector<pair<uint8_t*, int> > chunk_info;
  output_pool.GetChunkInfo(&chunk_info);
  output_batch->tuple_data.reserve(chunk_info.size());
  for (int i = 0; i < chunk_info.size(); ++i) {
    // TODO: this creates yet another data copy, figure out how to avoid that
    // (Thrift should introduce something like StringPieces, that can hold
    // references to data/length pairs)
    string data(reinterpret_cast<char*>(chunk_info[i].first), chunk_info[i].second);
    output_batch->tuple_data.push_back(string());
    output_batch->tuple_data.back().swap(data);
    DCHECK(data.empty());
  }
=======
const int RowBatch::AT_CAPACITY_MEM_USAGE = 8 * 1024 * 1024;

RowBatch::RowBatch(const RowDescriptor& row_desc, int capacity,
    MemTracker* mem_tracker)
  : mem_tracker_(mem_tracker),
    has_in_flight_row_(false),
    num_rows_(0),
    capacity_(capacity),
    num_tuples_per_row_(row_desc.tuple_descriptors().size()),
    row_desc_(row_desc),
    auxiliary_mem_usage_(0),
    need_to_return_(false),
    tuple_data_pool_(new MemPool(mem_tracker_)) {
  DCHECK(mem_tracker_ != NULL);
  DCHECK_GT(capacity, 0);
  tuple_ptrs_size_ = capacity_ * num_tuples_per_row_ * sizeof(Tuple*);
  tuple_ptrs_ = reinterpret_cast<Tuple**>(tuple_data_pool_->Allocate(tuple_ptrs_size_));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

// TODO: we want our input_batch's tuple_data to come from our (not yet implemented)
// global runtime memory segment; how do we get thrift to allocate it from there?
// maybe change line (in Data_types.cc generated from Data.thrift)
//              xfer += iprot->readString(this->tuple_data[_i9]);
// to allocated string data in special mempool
// (change via python script that runs over Data_types.cc)
<<<<<<< HEAD
RowBatch::RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch)
  : has_in_flight_row_(false),
    is_self_contained_(true),
=======
RowBatch::RowBatch(const RowDescriptor& row_desc, const TRowBatch& input_batch,
    MemTracker* mem_tracker)
  : mem_tracker_(mem_tracker),
    has_in_flight_row_(false),
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    num_rows_(input_batch.num_rows),
    capacity_(num_rows_),
    num_tuples_per_row_(input_batch.row_tuples.size()),
    row_desc_(row_desc),
<<<<<<< HEAD
    tuple_ptrs_(new Tuple*[num_rows_ * input_batch.row_tuples.size()]),
    tuple_data_pool_(new MemPool(input_batch.tuple_data)) {
=======
    auxiliary_mem_usage_(0),
    tuple_data_pool_(new MemPool(mem_tracker)) {
  DCHECK(mem_tracker_ != NULL);
  tuple_ptrs_size_ = num_rows_ * input_batch.row_tuples.size() * sizeof(Tuple*);
  tuple_ptrs_ = reinterpret_cast<Tuple**>(tuple_data_pool_->Allocate(tuple_ptrs_size_));
  if (input_batch.compression_type != THdfsCompression::NONE) {
    // Decompress tuple data into data pool
    uint8_t* compressed_data = (uint8_t*)input_batch.tuple_data.c_str();
    size_t compressed_size = input_batch.tuple_data.size();

    scoped_ptr<Codec> decompressor;
    Status status = Codec::CreateDecompressor(NULL, false, input_batch.compression_type,
        &decompressor);
    DCHECK(status.ok()) << status.GetErrorMsg();

    int64_t uncompressed_size = input_batch.uncompressed_size;
    DCHECK_NE(uncompressed_size, -1) << "RowBatch decompression failed";
    uint8_t* data = tuple_data_pool_->Allocate(uncompressed_size);
    status = decompressor->ProcessBlock(true, compressed_size, compressed_data,
        &uncompressed_size, &data);
    DCHECK(status.ok()) << "RowBatch decompression failed.";
    decompressor->Close();
  } else {
    // Tuple data uncompressed, copy directly into data pool
    uint8_t* data = tuple_data_pool_->Allocate(input_batch.tuple_data.size());
    memcpy(data, input_batch.tuple_data.c_str(), input_batch.tuple_data.size());
  }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // convert input_batch.tuple_offsets into pointers
  int tuple_idx = 0;
  for (vector<int32_t>::const_iterator offset = input_batch.tuple_offsets.begin();
       offset != input_batch.tuple_offsets.end(); ++offset) {
    if (*offset == -1) {
      tuple_ptrs_[tuple_idx++] = NULL;
    } else {
<<<<<<< HEAD
      tuple_ptrs_[tuple_idx++] =
          reinterpret_cast<Tuple*>(tuple_data_pool_->GetDataPtr(*offset));
=======
      tuple_ptrs_[tuple_idx++] = reinterpret_cast<Tuple*>(
          tuple_data_pool_->GetDataPtr(*offset + tuple_ptrs_size_));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }

  // check whether we have string slots
  // TODO: do that during setup (part of RowDescriptor c'tor?)
  bool has_string_slots = false;
  const vector<TupleDescriptor*>& tuple_descs = row_desc_.tuple_descriptors();
  for (int i = 0; i < tuple_descs.size(); ++i) {
    if (!tuple_descs[i]->string_slots().empty()) {
      has_string_slots = true;
      break;
    }
  }
  if (!has_string_slots) return;

  // convert string offsets contained in tuple data into pointers
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* row = GetRow(i);
    vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
    for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
      if ((*desc)->string_slots().empty()) continue;
      Tuple* t = row->GetTuple(j);
      if (t == NULL) continue;

      vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
      for (; slot != (*desc)->string_slots().end(); ++slot) {
<<<<<<< HEAD
        DCHECK_EQ((*slot)->type(), TYPE_STRING);
        StringValue* string_val = t->GetStringSlot((*slot)->tuple_offset());
        string_val->ptr = reinterpret_cast<char*>(
            tuple_data_pool_->GetDataPtr(reinterpret_cast<intptr_t>(string_val->ptr)));
=======
        DCHECK((*slot)->type().IsVarLen());
        StringValue* string_val = t->GetStringSlot((*slot)->tuple_offset());
        int offset = reinterpret_cast<intptr_t>(string_val->ptr) + tuple_ptrs_size_;
        string_val->ptr = reinterpret_cast<char*>(tuple_data_pool_->GetDataPtr(offset));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      }
    }
  }
}

<<<<<<< HEAD
int RowBatch::GetBatchSize(const TRowBatch& batch) {
  int result = 0;
  for (int i = 0; i < batch.tuple_data.size(); ++i) {
    result += batch.tuple_data[i].size();
  }
  return result;
}

void RowBatch::Swap(RowBatch* other) {
  DCHECK(row_desc_.Equals(other->row_desc_));
  DCHECK_EQ(num_tuples_per_row_, other->num_tuples_per_row_);
  DCHECK_EQ(tuple_ptrs_size_, other->tuple_ptrs_size_);

  // The destination row batch should be empty.  
  DCHECK(!has_in_flight_row_);
  DCHECK(!is_self_contained_);
  DCHECK(io_buffers_.empty());
  DCHECK_EQ(tuple_data_pool_->GetTotalChunkSizes(), 0);

  std::swap(has_in_flight_row_, other->has_in_flight_row_);
  std::swap(is_self_contained_, other->is_self_contained_);
  std::swap(num_rows_, other->num_rows_);
  std::swap(capacity_, other->capacity_);
  std::swap(tuple_ptrs_, other->tuple_ptrs_);
  std::swap(io_buffers_, other->io_buffers_);
  tuple_data_pool_.swap(other->tuple_data_pool_);
}

=======
RowBatch::~RowBatch() {
  tuple_data_pool_->FreeAll();
  for (int i = 0; i < io_buffers_.size(); ++i) {
    io_buffers_[i]->Return();
  }
  for (int i = 0; i < tuple_streams_.size(); ++i) {
    tuple_streams_[i]->Close();
  }
}

int RowBatch::Serialize(TRowBatch* output_batch) {
  // why does Thrift not generate a Clear() function?
  output_batch->row_tuples.clear();
  output_batch->tuple_offsets.clear();
  output_batch->compression_type = THdfsCompression::NONE;

  output_batch->num_rows = num_rows_;
  row_desc_.ToThrift(&output_batch->row_tuples);
  output_batch->tuple_offsets.reserve(num_rows_ * num_tuples_per_row_);

  int size = TotalByteSize();
  output_batch->tuple_data.resize(size);
  output_batch->uncompressed_size = size;

  // Copy tuple data, including strings, into output_batch (converting string
  // pointers into offsets in the process)
  int offset = 0; // current offset into output_batch->tuple_data
  char* tuple_data = const_cast<char*>(output_batch->tuple_data.c_str());
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* row = GetRow(i);
    const vector<TupleDescriptor*>& tuple_descs = row_desc_.tuple_descriptors();
    vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
    for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
      if (row->GetTuple(j) == NULL) {
        // NULLs are encoded as -1
        output_batch->tuple_offsets.push_back(-1);
        continue;
      }
      // Record offset before creating copy (which increments offset and tuple_data)
      output_batch->tuple_offsets.push_back(offset);
      row->GetTuple(j)->DeepCopy(**desc, &tuple_data, &offset, /* convert_ptrs */ true);
      DCHECK_LE(offset, size);
    }
  }
  DCHECK_EQ(offset, size);

  if (size > 0) {
    // Try compressing tuple_data to compression_scratch_, swap if compressed data is
    // smaller
    scoped_ptr<Codec> compressor;
    Status status = Codec::CreateCompressor(NULL, false, THdfsCompression::LZ4,
                                            &compressor);
    DCHECK(status.ok()) << status.GetErrorMsg();

    int64_t compressed_size = compressor->MaxOutputLen(size);
    if (compression_scratch_.size() < compressed_size) {
      compression_scratch_.resize(compressed_size);
    }
    uint8_t* input = (uint8_t*)output_batch->tuple_data.c_str();
    uint8_t* compressed_output = (uint8_t*)compression_scratch_.c_str();
    compressor->ProcessBlock(true, size, input, &compressed_size, &compressed_output);
    if (LIKELY(compressed_size < size)) {
      compression_scratch_.resize(compressed_size);
      output_batch->tuple_data.swap(compression_scratch_);
      output_batch->compression_type = THdfsCompression::LZ4;
    }
    VLOG_ROW << "uncompressed size: " << size << ", compressed size: " << compressed_size;
  }

  // The size output_batch would be if we didn't compress tuple_data (will be equal to
  // actual batch size if tuple_data isn't compressed)
  return GetBatchSize(*output_batch) - output_batch->tuple_data.size() + size;
}

void RowBatch::AddIoBuffer(DiskIoMgr::BufferDescriptor* buffer) {
  DCHECK(buffer != NULL);
  io_buffers_.push_back(buffer);
  auxiliary_mem_usage_ += buffer->buffer_len();
  buffer->SetMemTracker(mem_tracker_);
}

void RowBatch::AddTupleStream(BufferedTupleStream* stream) {
  DCHECK(stream != NULL);
  tuple_streams_.push_back(stream);
  auxiliary_mem_usage_ += stream->byte_size();
}

void RowBatch::Reset() {
  DCHECK(tuple_data_pool_.get() != NULL);
  num_rows_ = 0;
  has_in_flight_row_ = false;
  tuple_data_pool_->FreeAll();
  tuple_data_pool_.reset(new MemPool(mem_tracker_));
  for (int i = 0; i < io_buffers_.size(); ++i) {
    io_buffers_[i]->Return();
  }
  io_buffers_.clear();
  for (int i = 0; i < tuple_streams_.size(); ++i) {
    tuple_streams_[i]->Close();
  }
  tuple_streams_.clear();
  auxiliary_mem_usage_ = 0;
  tuple_ptrs_ = reinterpret_cast<Tuple**>(tuple_data_pool_->Allocate(tuple_ptrs_size_));
  need_to_return_ = false;
}

void RowBatch::TransferResourceOwnership(RowBatch* dest) {
  dest->auxiliary_mem_usage_ += tuple_data_pool_->total_allocated_bytes();
  dest->tuple_data_pool_->AcquireData(tuple_data_pool_.get(), false);
  for (int i = 0; i < io_buffers_.size(); ++i) {
    DiskIoMgr::BufferDescriptor* buffer = io_buffers_[i];
    dest->io_buffers_.push_back(buffer);
    dest->auxiliary_mem_usage_ += buffer->buffer_len();
    buffer->SetMemTracker(dest->mem_tracker_);
  }
  io_buffers_.clear();
  for (int i = 0; i < tuple_streams_.size(); ++i) {
    dest->tuple_streams_.push_back(tuple_streams_[i]);
    dest->auxiliary_mem_usage_ += tuple_streams_[i]->byte_size();
  }
  tuple_streams_.clear();
  dest->need_to_return_ |= need_to_return_;
  auxiliary_mem_usage_ = 0;
  tuple_ptrs_ = NULL;
  Reset();
}

int RowBatch::GetBatchSize(const TRowBatch& batch) {
  int result = batch.tuple_data.size();
  result += batch.row_tuples.size() * sizeof(TTupleId);
  result += batch.tuple_offsets.size() * sizeof(int32_t);
  return result;
}

void RowBatch::AcquireState(RowBatch* src) {
  DCHECK(row_desc_.Equals(src->row_desc_));
  DCHECK_EQ(num_tuples_per_row_, src->num_tuples_per_row_);
  DCHECK_EQ(tuple_ptrs_size_, src->tuple_ptrs_size_);
  DCHECK_EQ(capacity_, src->capacity_);
  DCHECK_EQ(auxiliary_mem_usage_, 0);

  // The destination row batch should be empty.
  DCHECK(!has_in_flight_row_);
  DCHECK_EQ(num_rows_, 0);

  for (int i = 0; i < src->io_buffers_.size(); ++i) {
    DiskIoMgr::BufferDescriptor* buffer = src->io_buffers_[i];
    io_buffers_.push_back(buffer);
    auxiliary_mem_usage_ += buffer->buffer_len();
    buffer->SetMemTracker(mem_tracker_);
  }
  src->io_buffers_.clear();
  src->auxiliary_mem_usage_ = 0;

  DCHECK(src->tuple_streams_.empty());

  has_in_flight_row_ = src->has_in_flight_row_;
  num_rows_ = src->num_rows_;
  capacity_ = src->capacity_;
  need_to_return_ = src->need_to_return_;
  std::swap(tuple_ptrs_, src->tuple_ptrs_);
  tuple_data_pool_->AcquireData(src->tuple_data_pool_.get(), false);
  auxiliary_mem_usage_ += src->tuple_data_pool_->total_allocated_bytes();
}

// TODO: consider computing size of batches as they are built up
int RowBatch::TotalByteSize() {
  int result = 0;
  for (int i = 0; i < num_rows_; ++i) {
    TupleRow* row = GetRow(i);
    const vector<TupleDescriptor*>& tuple_descs = row_desc_.tuple_descriptors();
    vector<TupleDescriptor*>::const_iterator desc = tuple_descs.begin();
    for (int j = 0; desc != tuple_descs.end(); ++desc, ++j) {
      Tuple* tuple = row->GetTuple(j);
      if (tuple == NULL) continue;
      result += (*desc)->byte_size();
      vector<SlotDescriptor*>::const_iterator slot = (*desc)->string_slots().begin();
      for (; slot != (*desc)->string_slots().end(); ++slot) {
        DCHECK((*slot)->type().IsVarLen());
        if (tuple->IsNull((*slot)->null_indicator_offset())) continue;
        StringValue* string_val = tuple->GetStringSlot((*slot)->tuple_offset());
        result += string_val->len;
      }
    }
  }
  return result;
}

int RowBatch::MaxTupleBufferSize() {
  int row_size = row_desc_.GetRowSize();
  if (row_size > AT_CAPACITY_MEM_USAGE) return row_size;
  int num_rows = 0;
  if (row_size != 0) {
    num_rows = std::min(capacity_, AT_CAPACITY_MEM_USAGE / row_size);
  }
  int tuple_buffer_size = num_rows * row_size;
  DCHECK_LE(tuple_buffer_size, AT_CAPACITY_MEM_USAGE);
  return tuple_buffer_size;
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
