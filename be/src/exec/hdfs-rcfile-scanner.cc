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

<<<<<<< HEAD
#include <boost/algorithm/string.hpp>
#include "text-converter.h"
=======
#include "exec/hdfs-rcfile-scanner.h"

#include <boost/algorithm/string.hpp>

#include "exec/hdfs-scan-node.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/scanner-context.inline.h"
#include "exec/text-converter.inline.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "exprs/expr.h"
#include "runtime/descriptors.h"
#include "runtime/runtime-state.h"
#include "runtime/mem-pool.h"
#include "runtime/raw-value.h"
<<<<<<< HEAD
#include "runtime/row-batch.h"
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/hdfs-fs-cache.h"
#include "runtime/string-value.h"
#include "util/runtime-profile.h"
#include "common/object-pool.h"
#include "gen-cpp/PlanNodes_types.h"
#include "exec/buffered-byte-stream.h"
#include "exec/hdfs-rcfile-scanner.h"
#include "exec/hdfs-sequence-scanner.h"
#include "exec/hdfs-scan-node.h"
#include "exec/serde-utils.inline.h"
#include "exec/text-converter.inline.h"
=======
#include "runtime/tuple-row.h"
#include "runtime/tuple.h"
#include "runtime/string-value.h"
#include "util/codec.h"
#include "util/string-parser.h"

#include "gen-cpp/PlanNodes_types.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

using namespace std;
using namespace boost;
using namespace impala;

const char* const HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$KeyBuffer";

const char* const HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME =
  "org.apache.hadoop.hive.ql.io.RCFile$ValueBuffer";

const char* const HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS =
  "hive.io.rcfile.column.number";

const uint8_t HdfsRCFileScanner::RCFILE_VERSION_HEADER[4] = {'R', 'C', 'F', 1};

<<<<<<< HEAD
static const uint8_t RC_FILE_RECORD_DELIMITER = 0xff;

HdfsRCFileScanner::HdfsRCFileScanner(HdfsScanNode* scan_node, RuntimeState* state,
    MemPool* mem_pool)
    : HdfsScanner(scan_node, state, mem_pool),
      scan_range_fully_buffered_(false),
      key_buffer_pool_(new MemPool()),
      key_buffer_length_(0),
      compressed_data_buffer_length_(0),
      column_buffer_pool_(new MemPool()) {
  // Initialize the parser to find bytes that are 0xff.
  find_first_parser_.reset(new DelimitedTextParser(scan_node, RC_FILE_RECORD_DELIMITER));
}

HdfsRCFileScanner::~HdfsRCFileScanner() {
  // Collect the maximum amount of memory we used to process this file.
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      column_buffer_pool_->peak_allocated_bytes());
  COUNTER_UPDATE(scan_node_->memory_used_counter(),
      key_buffer_pool_->peak_allocated_bytes());
}

Status HdfsRCFileScanner::Prepare() {
  RETURN_IF_ERROR(HdfsScanner::Prepare());

  text_converter_.reset(new TextConverter(0));

  // Allocate the buffers for the key information that is used to read and decode
  // the column data from its run length encoding.
  int num_part_keys = scan_node_->num_partition_keys();
  num_cols_ = scan_node_->num_cols() - num_part_keys;

  col_buf_len_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_buf_uncompressed_len_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_key_bufs_ = reinterpret_cast<uint8_t**>
      (key_buffer_pool_->Allocate(sizeof(uint8_t*) * num_cols_));

  col_key_bufs_len_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_bufs_off_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  key_buf_pos_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  cur_field_length_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  cur_field_length_rep_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  col_buf_pos_ = reinterpret_cast<int32_t*>(
      key_buffer_pool_->Allocate(sizeof(int32_t) * num_cols_));

  return Status::OK;
}

Status HdfsRCFileScanner::Close() {
  return Status::OK;
}

Status HdfsRCFileScanner::InitCurrentScanRange(HdfsPartitionDescriptor* hdfs_partition, 
    DiskIoMgr::ScanRange* scan_range, Tuple* template_tuple, 
    ByteStream* current_byte_stream_) {
  RETURN_IF_ERROR(HdfsScanner::InitCurrentScanRange(hdfs_partition, scan_range, 
      template_tuple, current_byte_stream_));
  end_of_scan_range_ = scan_range->len() + scan_range->offset();

  // Check the Location (file name) to see if we have changed files.
  // If this a new file then we need to read and process the header.
  if (previous_location_ != current_byte_stream_->GetLocation()) {
    RETURN_IF_ERROR(current_byte_stream_->Seek(0));
    RETURN_IF_ERROR(ReadFileHeader());
    // Save the current file name if we get the same file we can avoid
    // rereading the header information.
    previous_location_ = current_byte_stream_->GetLocation();
    // Save the seek offset of the end of the header information.  If we
    // get a scan range that starts at the beginning of the file we can avoid
    // reading the header but we must seek past it to get to the beginning of the data.
    current_byte_stream_->GetPosition(&header_end_);
  } else if (scan_range->offset() == 0) {
    // If are at the beginning of the file and we previously read the file header
    // we do not have to read it again but we need to seek past the file header.
    // We saved the offset above when we previously read and processed the header.
    RETURN_IF_ERROR(current_byte_stream_->Seek(header_end_));
  }

  // Offset may not point to row group boundary so we need to search for the next
  // sync block.
  if (scan_range->offset() != 0) {
    RETURN_IF_ERROR(current_byte_stream_->Seek(scan_range->offset()));
    RETURN_IF_ERROR(find_first_parser_->FindSyncBlock(end_of_scan_range_,
          SYNC_HASH_SIZE, &(sync_hash_[0]), current_byte_stream_));
  }
  int64_t position;
  RETURN_IF_ERROR(current_byte_stream_->GetPosition(&position));

  ResetRowGroup();

  scan_range_fully_buffered_ = false;
  previous_total_length_ = 0;
  tuple_ = NULL;

=======
// Macro to convert between SerdeUtil errors to Status returns.
#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return parse_status_

HdfsRCFileScanner::HdfsRCFileScanner(HdfsScanNode* scan_node, RuntimeState* state)
    : BaseSequenceScanner(scan_node, state) {
}

HdfsRCFileScanner::~HdfsRCFileScanner() {
}

Status HdfsRCFileScanner::Prepare(ScannerContext* context) {
  RETURN_IF_ERROR(BaseSequenceScanner::Prepare(context));
  text_converter_.reset(
      new TextConverter(0, scan_node_->hdfs_table()->null_column_value()));
  scan_node_->IncNumScannersCodegenDisabled();
  return Status::OK;
}

Status HdfsRCFileScanner::InitNewRange() {
  DCHECK(header_ != NULL);

  only_parsing_header_ = false;
  row_group_buffer_size_ = 0;

  // Can reuse buffer if there are no string columns (since the tuple won't contain
  // ptrs into the decompressed data).
  reuse_row_group_buffer_ = scan_node_->tuple_desc()->string_slots().empty();

  // The scanner currently copies all the column data out of the io buffer so the
  // stream never contains any tuple data.
  stream_->set_contains_tuple_data(false);

  if (header_->is_compressed) {
    RETURN_IF_ERROR(Codec::CreateDecompressor(NULL,
        reuse_row_group_buffer_, header_->codec, &decompressor_));
  }

  // Allocate the buffers for the key information that is used to read and decode
  // the column data.
  columns_.resize(reinterpret_cast<RcFileHeader*>(header_)->num_cols);
  int num_table_cols = scan_node_->num_cols() - scan_node_->num_partition_keys();
  for (int i = 0; i < columns_.size(); ++i) {
    if (i < num_table_cols) {
      int col_idx = i + scan_node_->num_partition_keys();
      columns_[i].materialize_column =
          scan_node_->GetMaterializedSlotIdx(col_idx) != HdfsScanNode::SKIP_COLUMN;
    } else {
      // Treat columns not found in table metadata as extra unmaterialized columns
      columns_[i].materialize_column = false;
    }
  }

  // TODO: Initialize codegen fn here
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

Status HdfsRCFileScanner::ReadFileHeader() {
<<<<<<< HEAD
  vector<uint8_t> head;
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
      sizeof(RCFILE_VERSION_HEADER), &head));
  if (!memcmp(&head[0], HdfsSequenceScanner::SEQFILE_VERSION_HEADER, 
      sizeof(HdfsSequenceScanner::SEQFILE_VERSION_HEADER))) {
    version_ = SEQ6;
  } else if (!memcmp(&head[0], RCFILE_VERSION_HEADER, sizeof(RCFILE_VERSION_HEADER))) {
    version_ = RCF1;
  } else {
    stringstream ss;
    ss << "Invalid RCFILE_VERSION_HEADER: '"
       << SerDeUtils::HexDump(&head[0], sizeof(RCFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }
  
  if (version_ == SEQ6) {
    vector<char> buf;
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &buf));
    if (strncmp(&buf[0], HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME,
        strlen(HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME))) {
      stringstream ss;
      ss << "Invalid RCFILE_KEY_CLASS_NAME: '"
         << string(&buf[0], strlen(HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME))
         << "'";
      return Status(ss.str());
    }

    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &buf));
    if (strncmp(&buf[0], HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME,
        strlen(HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME))) {
      stringstream ss;
      ss << "Invalid RCFILE_VALUE_CLASS_NAME: '"
         << string(&buf[0], strlen(HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME))
         << "'";
=======
  uint8_t* header;

  RcFileHeader* rc_header = reinterpret_cast<RcFileHeader*>(header_);
  // Validate file version
  RETURN_IF_FALSE(stream_->ReadBytes(
      sizeof(RCFILE_VERSION_HEADER), &header, &parse_status_));
  if (!memcmp(header, HdfsSequenceScanner::SEQFILE_VERSION_HEADER,
      sizeof(HdfsSequenceScanner::SEQFILE_VERSION_HEADER))) {
    rc_header->version = SEQ6;
  } else if (!memcmp(header, RCFILE_VERSION_HEADER, sizeof(RCFILE_VERSION_HEADER))) {
    rc_header->version = RCF1;
  } else {
    stringstream ss;
    ss << "Invalid RCFILE_VERSION_HEADER: '"
       << ReadWriteUtil::HexDump(header, sizeof(RCFILE_VERSION_HEADER)) << "'";
    return Status(ss.str());
  }

  if (rc_header->version == SEQ6) {
    // Validate class name key/value
    uint8_t* class_name_key;
    int64_t len;
    RETURN_IF_FALSE(
        stream_->ReadText(&class_name_key, &len, &parse_status_));
    if (len != strlen(HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME) ||
        memcmp(class_name_key, HdfsRCFileScanner::RCFILE_KEY_CLASS_NAME, len)) {
      stringstream ss;
      ss << "Invalid RCFILE_KEY_CLASS_NAME: '"
         << string(reinterpret_cast<char*>(class_name_key), len)
         << "' len=" << len;
      return Status(ss.str());
    }

    uint8_t* class_name_val;
    RETURN_IF_FALSE(
        stream_->ReadText(&class_name_val, &len, &parse_status_));
    if (len != strlen(HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME) ||
        memcmp(class_name_val, HdfsRCFileScanner::RCFILE_VALUE_CLASS_NAME, len)) {
      stringstream ss;
      ss << "Invalid RCFILE_VALUE_CLASS_NAME: '"
         << string(reinterpret_cast<char*>(class_name_val), len)
         << "' len=" << len;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      return Status(ss.str());
    }
  }

<<<<<<< HEAD
  RETURN_IF_ERROR(SerDeUtils::ReadBoolean(current_byte_stream_, &is_compressed_));

  if (version_ == SEQ6) {
    // Read the is_blk_compressed header field. This field should *always*
    // be FALSE, and is the result of a defect in the original RCFile
    // implementation contained in Hive.
    bool is_blk_compressed;
    RETURN_IF_ERROR(SerDeUtils::ReadBoolean(current_byte_stream_, &is_blk_compressed));
    if (is_blk_compressed) {
      stringstream ss;
      ss << "RC files do no support block compression, set in: '"
         << current_byte_stream_->GetLocation() << "'";
      return Status(ss.str());
    }
  }

  vector<char> codec;
  if (is_compressed_) {
    // Read the codec and get the right decompressor class.
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &codec));
    string codec_str(&codec[0], codec.size());
    RETURN_IF_ERROR(Codec::CreateDecompressor(state_,
        column_buffer_pool_.get(), !has_noncompact_strings_, codec_str, &decompressor_));
  }

  VLOG_FILE << current_byte_stream_->GetLocation() << ": "
            << (is_compressed_ ?  "block compressed" : "not compressed");
  if (is_compressed_) VLOG_FILE << string(&codec[0], codec.size());

  RETURN_IF_ERROR(ReadFileHeaderMetadata());

  RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
      HdfsRCFileScanner::SYNC_HASH_SIZE, &sync_hash_[0]));
  return Status::OK;
}

Status HdfsRCFileScanner::ReadFileHeaderMetadata() {
  int map_size = 0;
  vector<char> key;
  vector<char> value;

  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &map_size));

  for (int i = 0; i < map_size; ++i) {
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &key));
    RETURN_IF_ERROR(SerDeUtils::ReadText(current_byte_stream_, &value));

    if (!strncmp(&key[0], HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS,
        strlen(HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS))) {
      string tmp(&value[0], value.size());
      int file_num_cols = atoi(tmp.c_str());
      if (file_num_cols != num_cols_) {
        return Status("Unexpected hive.io.rcfile.column.number value!");
      }
=======
  // Check for compression
  RETURN_IF_FALSE(
      stream_->ReadBoolean(&header_->is_compressed, &parse_status_));
  if (rc_header->version == SEQ6) {
    // Read the is_blk_compressed header field. This field should *always*
    // be FALSE, and is the result of using the sequence file header format in the
    // original RCFile format.
    bool is_blk_compressed;
    RETURN_IF_FALSE(
        stream_->ReadBoolean(&is_blk_compressed, &parse_status_));
    if (is_blk_compressed) {
      stringstream ss;
      ss << "RC files do no support block compression.";
      return Status(ss.str());
    }
  }
  if (header_->is_compressed) {
    uint8_t* codec_ptr;
    int64_t len;
    // Read the codec and get the right decompressor class.
    RETURN_IF_FALSE(stream_->ReadText(&codec_ptr, &len, &parse_status_));
    header_->codec = string(reinterpret_cast<char*>(codec_ptr), len);
    Codec::CodecMap::const_iterator it = Codec::CODEC_MAP.find(header_->codec);
    DCHECK(it != Codec::CODEC_MAP.end());
    header_->compression_type = it->second;
  } else {
    header_->compression_type = THdfsCompression::NONE;
  }
  VLOG_FILE << stream_->filename() << ": "
            << (header_->is_compressed ?  "compressed" : "not compressed");
  if (header_->is_compressed) VLOG_FILE << header_->codec;

  RETURN_IF_ERROR(ReadNumColumnsMetadata());

  // Read file sync marker
  uint8_t* sync;
  RETURN_IF_FALSE(stream_->ReadBytes(SYNC_HASH_SIZE, &sync, &parse_status_));
  memcpy(header_->sync, sync, SYNC_HASH_SIZE);

  header_->header_size = stream_->total_bytes_returned() - SYNC_HASH_SIZE;
  return Status::OK;
}

Status HdfsRCFileScanner::ReadNumColumnsMetadata() {
  int map_size = 0;
  RETURN_IF_FALSE(stream_->ReadInt(&map_size, &parse_status_));

  for (int i = 0; i < map_size; ++i) {
    uint8_t* key, *value;
    int64_t key_len, value_len;
    RETURN_IF_FALSE(stream_->ReadText(&key, &key_len, &parse_status_));
    RETURN_IF_FALSE(stream_->ReadText(&value, &value_len, &parse_status_));

    if (key_len == strlen(RCFILE_METADATA_KEY_NUM_COLS) &&
        !memcmp(key, HdfsRCFileScanner::RCFILE_METADATA_KEY_NUM_COLS, key_len)) {
      string value_str(reinterpret_cast<char*>(value), value_len);
      StringParser::ParseResult result;
      int num_cols =
          StringParser::StringToInt<int>(value_str.c_str(), value_str.size(), &result);
      if (result != StringParser::PARSE_SUCCESS) {
        stringstream ss;
        ss << "Could not parse number of columns in file " << stream_->filename()
           << ": " << value_str;
        if (result == StringParser::PARSE_OVERFLOW) ss << " (result overflowed)";
        return Status(ss.str());
      }
      RcFileHeader* rc_header = reinterpret_cast<RcFileHeader*>(header_);
      rc_header->num_cols = num_cols;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }
  return Status::OK;
}

<<<<<<< HEAD
Status HdfsRCFileScanner::ReadSync() {
  uint8_t hash[HdfsRCFileScanner::SYNC_HASH_SIZE];
  RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
      HdfsRCFileScanner::SYNC_HASH_SIZE, &hash[0]));
  if (memcmp(&hash[0], &sync_hash_[0], HdfsRCFileScanner::SYNC_HASH_SIZE)) {
    if (state_->LogHasSpace()) {
      stringstream ss;
      ss  << "Bad sync hash in current HdfsRCFileScanner: "
          << current_byte_stream_->GetLocation() << "." << endl
          << "Expected: '"
          << SerDeUtils::HexDump(sync_hash_, HdfsRCFileScanner::SYNC_HASH_SIZE)
          << "'" << endl
          << "Actual:   '"
          << SerDeUtils::HexDump(hash, HdfsRCFileScanner::SYNC_HASH_SIZE)
          << "'" << endl;
      state_->LogError(ss.str());
    }
    return Status("bad sync hash block");
  }
  return Status::OK;
=======
BaseSequenceScanner::FileHeader* HdfsRCFileScanner::AllocateFileHeader() {
  return new RcFileHeader;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

void HdfsRCFileScanner::ResetRowGroup() {
  num_rows_ = 0;
  row_pos_ = 0;
  key_length_ = 0;
  compressed_key_length_ = 0;

<<<<<<< HEAD
  memset(col_buf_len_, 0, num_cols_ * sizeof(int32_t));
  memset(col_buf_uncompressed_len_, 0, num_cols_ * sizeof(int32_t));
  memset(col_key_bufs_len_, 0, num_cols_ * sizeof(int32_t));

  memset(key_buf_pos_, 0, num_cols_ * sizeof(int32_t));
  memset(cur_field_length_, 0, num_cols_ * sizeof(int32_t));
  memset(cur_field_length_rep_, 0, num_cols_ * sizeof(int32_t));

  memset(col_buf_pos_, 0, num_cols_ * sizeof(int32_t));
=======
  for (int i = 0; i < columns_.size(); ++i) {
    columns_[i].buffer_len = 0;
    columns_[i].buffer_pos = 0;
    columns_[i].uncompressed_buffer_len = 0;
    columns_[i].key_buffer_len = 0;
    columns_[i].key_buffer_pos = 0;
    columns_[i].current_field_len = 0;
    columns_[i].current_field_len_rep = 0;
  }

  // We are done with this row group, pass along external buffers if necessary.
  if (!reuse_row_group_buffer_) {
    AttachPool(data_buffer_pool_.get(), true);
    row_group_buffer_size_ = 0;
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

Status HdfsRCFileScanner::ReadRowGroup() {
  ResetRowGroup();
<<<<<<< HEAD
  int64_t position;
  bool eof;

  while (num_rows_ == 0) {
    RETURN_IF_ERROR(ReadHeader());
    RETURN_IF_ERROR(ReadKeyBuffers());
    if (has_noncompact_strings_ || previous_total_length_ < total_col_length_) {
      column_buffer_ = column_buffer_pool_->Allocate(total_col_length_);
      previous_total_length_ = total_col_length_;
    }
    RETURN_IF_ERROR(ReadColumnBuffers());
    RETURN_IF_ERROR(current_byte_stream_->GetPosition(&position));
    if (position >= end_of_scan_range_) {
      RETURN_IF_ERROR(current_byte_stream_->Eof(&eof));
      if (eof) {
        scan_range_fully_buffered_ = true;
        break;
      }

      // We must read up to the next sync marker.
      int32_t record_length;
      RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &record_length));
      if (record_length == HdfsRCFileScanner::SYNC_MARKER) {
        // If the marker is there, it's an error not to have a Sync block following the
        // Marker.
        RETURN_IF_ERROR(ReadSync());
        scan_range_fully_buffered_ = true;
        break;
      } else {
        RETURN_IF_ERROR(current_byte_stream_->Seek(position));
      }
    }
=======

  while (num_rows_ == 0) {
    RETURN_IF_ERROR(ReadRowGroupHeader());
    RETURN_IF_ERROR(ReadKeyBuffers());
    if (!reuse_row_group_buffer_ || row_group_buffer_size_ < row_group_length_) {
      // Allocate a new buffer for reading the row group.  Row groups have a
      // fixed number of rows so take a guess at how big it will be based on
      // the previous row group size.
      // The row group length depends on the user data and can be very big. This
      // can cause us to go way over the mem limit so use TryAllocate instead.
      row_group_buffer_ = data_buffer_pool_->TryAllocate(row_group_length_);
      if (row_group_length_ > 0 && row_group_buffer_ == NULL) {
        return state_->SetMemLimitExceeded(
            scan_node_->mem_tracker(), row_group_length_);
      }
      row_group_buffer_size_ = row_group_length_;
    }
    RETURN_IF_ERROR(ReadColumnBuffers());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  return Status::OK;
}

<<<<<<< HEAD
Status HdfsRCFileScanner::ReadHeader() {
  int32_t record_length;
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &record_length));
  // The sync block is marked with a record_length of -1.
  if (record_length == HdfsRCFileScanner::SYNC_MARKER) {
    RETURN_IF_ERROR(ReadSync());
    RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &record_length));
  }
  if (record_length < 0) {
    stringstream ss;
    int64_t position;
    current_byte_stream_->GetPosition(&position);
    position -= sizeof(int32_t);
    ss << "Bad record length: " << record_length << " in file: "
        << current_byte_stream_->GetLocation() << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &key_length_));
  if (key_length_ < 0) {
    stringstream ss;
    int64_t position;
    current_byte_stream_->GetPosition(&position);
    position -= sizeof(int32_t);
    ss << "Bad key length: " << key_length_ << " in file: "
        << current_byte_stream_->GetLocation() << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_ERROR(SerDeUtils::ReadInt(current_byte_stream_, &compressed_key_length_));
  if (compressed_key_length_ < 0) {
    stringstream ss;
    int64_t position;
    current_byte_stream_->GetPosition(&position);
    position -= sizeof(int32_t);
    ss << "Bad compressed key length: " << compressed_key_length_ << " in file: "
        << current_byte_stream_->GetLocation() << " at offset: " << position;
=======
Status HdfsRCFileScanner::ReadRowGroupHeader() {
  int32_t record_length;
  RETURN_IF_FALSE(stream_->ReadInt(&record_length, &parse_status_));
  if (record_length < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad record length: " << record_length << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_FALSE(stream_->ReadInt(&key_length_, &parse_status_));
  if (key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad key length: " << key_length_ << " at offset: " << position;
    return Status(ss.str());
  }
  RETURN_IF_FALSE(stream_->ReadInt(&compressed_key_length_, &parse_status_));
  if (compressed_key_length_ < 0) {
    stringstream ss;
    int64_t position = stream_->file_offset();
    position -= sizeof(int32_t);
    ss << "Bad compressed key length: " << compressed_key_length_
       << " at offset: " << position;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    return Status(ss.str());
  }
  return Status::OK;
}

Status HdfsRCFileScanner::ReadKeyBuffers() {
<<<<<<< HEAD
  if (key_buffer_length_ < key_length_) {
    key_buffer_ = key_buffer_pool_->Allocate(key_length_);
    key_buffer_length_ = key_length_;
  }
  if (is_compressed_) {
    if (compressed_data_buffer_length_ < compressed_key_length_) {
      compressed_data_buffer_ = key_buffer_pool_->Allocate(compressed_key_length_);
      compressed_data_buffer_length_ = compressed_key_length_;
    }
    RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
        compressed_key_length_, compressed_data_buffer_));
    RETURN_IF_ERROR(decompressor_->ProcessBlock(compressed_key_length_,
        compressed_data_buffer_, &key_length_, &key_buffer_));
  } else {
    RETURN_IF_ERROR(
        SerDeUtils::ReadBytes(current_byte_stream_, key_length_, key_buffer_));
  }

  total_col_length_ = 0;
  uint8_t* key_buf_ptr = key_buffer_;
  int bytes_read = SerDeUtils::GetVInt(key_buf_ptr, &num_rows_);
  key_buf_ptr += bytes_read;

  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    GetCurrentKeyBuffer(col_idx, !ReadColumn(col_idx), &key_buf_ptr);
    DCHECK_LE(key_buf_ptr, key_buffer_ + key_length_);
  }
  DCHECK_EQ(key_buf_ptr, key_buffer_ + key_length_);
=======
  if (key_buffer_.size() < key_length_) key_buffer_.resize(key_length_);
  uint8_t* key_buffer = &key_buffer_[0];

  if (header_->is_compressed) {
    uint8_t* compressed_buffer;
    RETURN_IF_FALSE(stream_->ReadBytes(
        compressed_key_length_, &compressed_buffer, &parse_status_));
    {
      SCOPED_TIMER(decompress_timer_);
      RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, compressed_key_length_,
          compressed_buffer, &key_length_, &key_buffer));
      VLOG_FILE << "Decompressed " << compressed_key_length_ << " to " << key_length_;
    }
  } else {
    uint8_t* buffer;
    RETURN_IF_FALSE(
        stream_->ReadBytes(key_length_, &buffer, &parse_status_));
    // Make a copy of this buffer.  The underlying IO buffer will get recycled
    memcpy(key_buffer, buffer, key_length_);
  }

  row_group_length_ = 0;
  uint8_t* key_buf_ptr = key_buffer;
  int bytes_read = ReadWriteUtil::GetVInt(key_buf_ptr, &num_rows_);
  key_buf_ptr += bytes_read;

  for (int col_idx = 0; col_idx < columns_.size(); ++col_idx) {
    GetCurrentKeyBuffer(col_idx, !columns_[col_idx].materialize_column, &key_buf_ptr);
    DCHECK_LE(key_buf_ptr, key_buffer + key_length_);
  }
  DCHECK_EQ(key_buf_ptr, key_buffer + key_length_);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  return Status::OK;
}

void HdfsRCFileScanner::GetCurrentKeyBuffer(int col_idx, bool skip_col_data,
                                            uint8_t** key_buf_ptr) {
<<<<<<< HEAD
  int bytes_read = SerDeUtils::GetVInt(*key_buf_ptr, &(col_buf_len_[col_idx]));
  *key_buf_ptr += bytes_read;

  bytes_read = SerDeUtils::GetVInt(*key_buf_ptr, &(col_buf_uncompressed_len_[col_idx]));
  *key_buf_ptr += bytes_read;

  int col_key_buf_len;
  bytes_read = SerDeUtils::GetVInt(*key_buf_ptr , &col_key_buf_len);
  *key_buf_ptr += bytes_read;

  if (!skip_col_data) {
    col_key_bufs_[col_idx] = *key_buf_ptr;

    // Set the offset for the start of the data for this column in the allocated buffer.
    col_bufs_off_[col_idx] = total_col_length_;
    total_col_length_ += col_buf_uncompressed_len_[col_idx];
=======
  ColumnInfo& col_info = columns_[col_idx];

  int bytes_read = ReadWriteUtil::GetVInt(*key_buf_ptr, &col_info.buffer_len);
  *key_buf_ptr += bytes_read;

  bytes_read = ReadWriteUtil::GetVInt(*key_buf_ptr, &col_info.uncompressed_buffer_len);
  *key_buf_ptr += bytes_read;

  int col_key_buf_len;
  bytes_read = ReadWriteUtil::GetVInt(*key_buf_ptr , &col_key_buf_len);
  *key_buf_ptr += bytes_read;

  if (!skip_col_data) {
    col_info.key_buffer = *key_buf_ptr;

    // Set the offset for the start of the data for this column in the allocated buffer.
    col_info.start_offset = row_group_length_;
    row_group_length_ += col_info.uncompressed_buffer_len;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  *key_buf_ptr += col_key_buf_len;
}

inline Status HdfsRCFileScanner::NextField(int col_idx) {
<<<<<<< HEAD
  col_buf_pos_[col_idx] += cur_field_length_[col_idx];

  if (cur_field_length_rep_[col_idx] > 0) {
    // repeat the previous length
    --cur_field_length_rep_[col_idx];
  } else {
    DCHECK_GE(cur_field_length_rep_[col_idx], 0);
    // Get the next column length or repeat count
    int64_t length = 0;
    uint8_t* col_key_buf = &col_key_bufs_[col_idx][0];
    int bytes_read = SerDeUtils::GetVLong(col_key_buf, key_buf_pos_[col_idx], &length);
    if (bytes_read == -1) {
        int64_t position;
        current_byte_stream_->GetPosition(&position);
        stringstream ss;
        ss << "Invalid column length in file: "
           << current_byte_stream_->GetLocation() << " at offset: " << position;
        if (state_->LogHasSpace()) state_->LogError(ss.str());
        return Status(ss.str());
    }
    key_buf_pos_[col_idx] += bytes_read;
=======
  ColumnInfo& col_info = columns_[col_idx];
  col_info.buffer_pos += col_info.current_field_len;

  if (col_info.current_field_len_rep > 0) {
    // repeat the previous length
    --col_info.current_field_len_rep;
  } else {
    // Get the next column length or repeat count
    int64_t length = 0;
    uint8_t* col_key_buf = col_info.key_buffer;
    int bytes_read = ReadWriteUtil::GetVLong(
        col_key_buf, col_info.key_buffer_pos, &length);
    if (bytes_read == -1) {
        int64_t position = stream_->file_offset();
        stringstream ss;
        ss << "Invalid column length at offset: " << position;
        return Status(ss.str());
    }
    col_info.key_buffer_pos += bytes_read;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

    if (length < 0) {
      // The repeat count is stored as the logical negation of the number of repetitions.
      // See the column-key-buffer comment in hdfs-rcfile-scanner.h.
<<<<<<< HEAD
      cur_field_length_rep_[col_idx] = ~length - 1;
    } else {
      cur_field_length_[col_idx] = length;
=======
      col_info.current_field_len_rep = ~length - 1;
    } else {
      col_info.current_field_len = length;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }
  return Status::OK;
}

<<<<<<< HEAD
inline Status HdfsRCFileScanner::NextRow(bool* eorg) {
  // TODO: Wrap this in an iterator and prevent people from alternating
  // calls to NextField()/NextRow()
  if (row_pos_ >= num_rows_) {
    *eorg = true;
    return Status::OK;
  }
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    if (ReadColumn(col_idx)) {
=======
inline Status HdfsRCFileScanner::NextRow() {
  // TODO: Wrap this in an iterator and prevent people from alternating
  // calls to NextField()/NextRow()
  DCHECK_LT(row_pos_, num_rows_);
  for (int col_idx = 0; col_idx < columns_.size(); ++col_idx) {
    if (columns_[col_idx].materialize_column) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      RETURN_IF_ERROR(NextField(col_idx));
    }
  }
  ++row_pos_;
<<<<<<< HEAD
  *eorg = false;
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

Status HdfsRCFileScanner::ReadColumnBuffers() {
<<<<<<< HEAD
  for (int col_idx = 0; col_idx < num_cols_; ++col_idx) {
    if (!ReadColumn(col_idx)) {
      RETURN_IF_ERROR(SerDeUtils::SkipBytes(current_byte_stream_, col_buf_len_[col_idx]));
    } else {
      // TODO: Stream through these column buffers instead of reading everything
      // in at once.
      DCHECK_LE(
          col_buf_uncompressed_len_[col_idx] + col_bufs_off_[col_idx], total_col_length_);
      if (is_compressed_) {
        if (compressed_data_buffer_length_ < col_buf_len_[col_idx]) {
          compressed_data_buffer_ = key_buffer_pool_->Allocate(col_buf_len_[col_idx]);
          compressed_data_buffer_length_ = col_buf_len_[col_idx];
        }
        RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
            col_buf_len_[col_idx], compressed_data_buffer_));
        uint8_t* compressed_output = column_buffer_ + col_bufs_off_[col_idx];
        RETURN_IF_ERROR(decompressor_->ProcessBlock(col_buf_len_[col_idx],
            compressed_data_buffer_, &col_buf_uncompressed_len_[col_idx],
            &compressed_output));
      } else {
        RETURN_IF_ERROR(SerDeUtils::ReadBytes(current_byte_stream_,
            col_buf_len_[col_idx], column_buffer_ + col_bufs_off_[col_idx]));
      }
=======
  for (int col_idx = 0; col_idx < columns_.size(); ++col_idx) {
    ColumnInfo& column = columns_[col_idx];
    if (!columns_[col_idx].materialize_column) {
      // Not materializing this column, just skip it.
      RETURN_IF_FALSE(
          stream_->SkipBytes(column.buffer_len, &parse_status_));
      continue;
    }

    // TODO: Stream through these column buffers instead of reading everything
    // in at once.
    DCHECK_LE(column.uncompressed_buffer_len + column.start_offset, row_group_length_);
    if (header_->is_compressed) {
      uint8_t* compressed_input;
      RETURN_IF_FALSE(stream_->ReadBytes(
          column.buffer_len, &compressed_input, &parse_status_));
      uint8_t* compressed_output = row_group_buffer_ + column.start_offset;
      {
        SCOPED_TIMER(decompress_timer_);
        RETURN_IF_ERROR(decompressor_->ProcessBlock32(true, column.buffer_len,
            compressed_input, &column.uncompressed_buffer_len,
            &compressed_output));
        VLOG_FILE << "Decompressed " << column.buffer_len << " to "
                  << column.uncompressed_buffer_len;
      }
    } else {
      uint8_t* uncompressed_data;
      RETURN_IF_FALSE(stream_->ReadBytes(
          column.buffer_len, &uncompressed_data, &parse_status_));
      // TODO: this is bad.  Remove this copy.
      memcpy(row_group_buffer_ + column.start_offset,
          uncompressed_data, column.buffer_len);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    }
  }
  return Status::OK;
}

<<<<<<< HEAD
// TODO: We should be able to skip over badly fomrated data and move to the
//       next sync block to restart the scan.
Status HdfsRCFileScanner::GetNext(RowBatch* row_batch, bool* eosr) {
  AllocateTupleBuffer(row_batch);
  // Indicates whether the current row has errors.
  bool error_in_row = false;

  if (scan_node_->ReachedLimit()) {
    tuple_ = NULL;
    *eosr = true;
    return Status::OK;
  }


  // HdfsRCFileScanner effectively does buffered IO, in that it reads the
  // rcfile into a column buffer, and sets an eof marker at that
  // point, before the buffer is drained.
  // In the following loop, eosr tracks the end of stream flag that
  // tells the scan node to move onto the next scan range.
  // scan_range_fully_buffered_ is set once the RC file has been fully
  // read into RCRowGroups.
  while (!scan_node_->ReachedLimit() && !row_batch->IsFull() && !(*eosr)) {
    if (num_rows_ == row_pos_) {
      // scan_range_fully_buffered_ is set iff the scan range has been
      // exhausted due to a previous read.
      if (scan_range_fully_buffered_) {
        *eosr = true;
        break;
      }
      RETURN_IF_ERROR(ReadRowGroup());

      if (num_rows_ == 0) break;
    }

    SCOPED_TIMER(scan_node_->materialize_tuple_timer());
    // Copy rows out of the current row group into the row_batch
    while (!scan_node_->ReachedLimit() && !row_batch->IsFull()) {
      bool eorg = false;
      RETURN_IF_ERROR(NextRow(&eorg));
      if (eorg) break;

      // Index into current row in row_batch.
      int row_idx = row_batch->AddRow();
      DCHECK(row_idx != RowBatch::INVALID_ROW_INDEX);
      TupleRow* current_row = row_batch->GetRow(row_idx);
      current_row->SetTuple(scan_node_->tuple_idx(), tuple_);
      // Initialize tuple_ from the partition key template tuple before writing the
      // slots
      InitTuple(template_tuple_, tuple_);

      const vector<SlotDescriptor*>& materialized_slots = 
          scan_node_->materialized_slots();
      vector<SlotDescriptor*>::const_iterator it;

      for (it = materialized_slots.begin(); it != materialized_slots.end(); ++it) {
        const SlotDescriptor* slot_desc = *it;
        int rc_column_idx = slot_desc->col_pos() - scan_node_->num_partition_keys();

        const char* col_start = reinterpret_cast<const char*>(&((column_buffer_ +
            col_bufs_off_[rc_column_idx])[col_buf_pos_[rc_column_idx]]));
        int field_len = cur_field_length_[rc_column_idx];
        DCHECK_LE(col_start + field_len,
            reinterpret_cast<const char*>(column_buffer_ + total_col_length_));

        if (!text_converter_->WriteSlot(slot_desc, tuple_, 
              col_start, field_len, !has_noncompact_strings_, false, tuple_pool_)) {
          ReportColumnParseError(slot_desc, col_start, field_len);
          error_in_row = true;
        }
      }

      if (error_in_row) {
        error_in_row = false;
        if (state_->LogHasSpace()) {
          stringstream ss;
          ss << "file: " << current_byte_stream_->GetLocation();
          state_->LogError(ss.str());
        }
        if (state_->abort_on_error()) {
          state_->ReportFileErrors(current_byte_stream_->GetLocation(), 1);
          return Status(state_->ErrorLog());
        }
      }

      // Evaluate the conjuncts and add the row to the batch
      if (ExecNode::EvalConjuncts(conjuncts_, num_conjuncts_, current_row)) {
        row_batch->CommitLastRow();
        if (scan_node_->ReachedLimit() || row_batch->IsFull()) {
          tuple_ = NULL;
          return Status::OK;
        }
        char* new_tuple = reinterpret_cast<char*>(tuple_);
        new_tuple += tuple_byte_size_;
        tuple_ = reinterpret_cast<Tuple*>(new_tuple);
      }
    }
  }

  // Note that number of rows remaining == 0 -> scan_range_fully_buffered_
  // at this point, hence we don't need to explicitly check that
  // scan_range_fully_buffered_ == true.
  if (scan_node_->ReachedLimit() || num_rows_ == 0 || num_rows_ == row_pos_) {
    // We reached the limit, drained the row group or hit the end of the table.
    // No more work to be done. Clean up all pools with the last row batch.
    *eosr = true;
  } else {
    DCHECK(row_batch->IsFull());
    // The current row_batch is full, but we haven't yet reached our limit.
    // Hang on to the last chunks. We'll continue from there in the next
    // call to GetNext().
    *eosr = false;
  }
  // Maintian ownership of last memory chunk if not at the end of the scan range.
  if (has_noncompact_strings_) {
    row_batch->tuple_data_pool()->AcquireData(column_buffer_pool_.get(), !*eosr);
  }
  row_batch->tuple_data_pool()->AcquireData(tuple_pool_, !*eosr);

=======
Status HdfsRCFileScanner::ProcessRange() {
  ResetRowGroup();

  // HdfsRCFileScanner effectively does buffered IO, in that it reads all the
  // materialized columns into a row group buffer.
  // It will then materialize tuples from the row group buffer.  When the row
  // group is complete, it will move onto the next row group.
  while (!finished()) {
    DCHECK_EQ(num_rows_, row_pos_);
    // Finished materializing this row group, read the next one.
    RETURN_IF_ERROR(ReadRowGroup());
    if (num_rows_ == 0) break;

    while (num_rows_ != row_pos_) {
      SCOPED_TIMER(scan_node_->materialize_tuple_timer());

      // Indicates whether the current row has errors.
      bool error_in_row = false;
      const vector<SlotDescriptor*>& materialized_slots =
          scan_node_->materialized_slots();
      vector<SlotDescriptor*>::const_iterator it;

      // Materialize rows from this row group in row batch sizes
      MemPool* pool;
      Tuple* tuple;
      TupleRow* current_row;
      int max_tuples = GetMemory(&pool, &tuple, &current_row);
      max_tuples = min(max_tuples, num_rows_ - row_pos_);

      if (materialized_slots.empty()) {
        // If there are no materialized slots (e.g. count(*) or just partition cols)
        // we can shortcircuit the parse loop
        row_pos_ += max_tuples;
        int num_to_commit = WriteEmptyTuples(context_, current_row, max_tuples);
        COUNTER_ADD(scan_node_->rows_read_counter(), max_tuples);
        RETURN_IF_ERROR(CommitRows(num_to_commit));
        continue;
      }

      int num_to_commit = 0;
      for (int i = 0; i < max_tuples; ++i) {
        RETURN_IF_ERROR(NextRow());

        // Initialize tuple from the partition key template tuple before writing the
        // slots
        InitTuple(template_tuple_, tuple);

        for (it = materialized_slots.begin(); it != materialized_slots.end(); ++it) {
          const SlotDescriptor* slot_desc = *it;
          int file_column_idx = slot_desc->col_pos() - scan_node_->num_partition_keys();

          // Set columns missing in this file to NULL
          if (file_column_idx >= columns_.size()) {
            tuple->SetNull(slot_desc->null_indicator_offset());
            continue;
          }

          ColumnInfo& column = columns_[file_column_idx];
          DCHECK(column.materialize_column);

          const char* col_start = reinterpret_cast<const char*>(
              row_group_buffer_ + column.start_offset + column.buffer_pos);
          int field_len = column.current_field_len;
          DCHECK_LE(col_start + field_len,
              reinterpret_cast<const char*>(row_group_buffer_ + row_group_length_));

          if (!text_converter_->WriteSlot(slot_desc, tuple, col_start, field_len,
              false, false, pool)) {
            ReportColumnParseError(slot_desc, col_start, field_len);
            error_in_row = true;
          }
        }

        if (error_in_row) {
          error_in_row = false;
          if (state_->LogHasSpace()) {
            stringstream ss;
            ss << "file: " << stream_->filename();
            state_->LogError(ss.str());
          }
          if (state_->abort_on_error()) {
            state_->ReportFileErrors(stream_->filename(), 1);
            return Status(state_->ErrorLog());
          }
        }

        current_row->SetTuple(scan_node_->tuple_idx(), tuple);
        // Evaluate the conjuncts and add the row to the batch
        if (EvalConjuncts(current_row)) {
          ++num_to_commit;
          current_row = next_row(current_row);
          tuple = next_tuple(tuple);
        }
      }
      COUNTER_ADD(scan_node_->rows_read_counter(), max_tuples);
      RETURN_IF_ERROR(CommitRows(num_to_commit));
      if (scan_node_->ReachedLimit()) return Status::OK;
    }

    // RCFiles don't end with syncs
    if (stream_->eof()) return Status::OK;

    // Check for sync by looking for the marker that precedes syncs.
    int marker;
    RETURN_IF_FALSE(stream_->ReadInt(&marker, &parse_status_, /* peek */ true));
    if (marker == HdfsRCFileScanner::SYNC_MARKER) {
      RETURN_IF_FALSE(stream_->ReadInt(&marker, &parse_status_, /* peek */ false));
      RETURN_IF_ERROR(ReadSync());
    }
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

void HdfsRCFileScanner::DebugString(int indentation_level, stringstream* out) const {
  // TODO: Add more details of internal state.
<<<<<<< HEAD
  *out << string(indentation_level * 2, ' ');
  *out << "HdfsRCFileScanner(tupleid=" << scan_node_->tuple_idx() <<
    " file=" << current_byte_stream_->GetLocation();
=======
  *out << string(indentation_level * 2, ' ')
       << "HdfsRCFileScanner(tupleid=" << scan_node_->tuple_idx()
       << " file=" << stream_->filename();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // TODO: Scanner::DebugString
  //  ExecNode::DebugString(indentation_level, out);
  *out << "])" << endl;
}
