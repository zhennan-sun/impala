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


#ifndef IMPALA_UTIL_DECOMPRESS_H
#define IMPALA_UTIL_DECOMPRESS_H

// We need zlib.h here to declare stream_ below.
#include <zlib.h>

#include "util/codec.h"
#include "exec/hdfs-scanner.h"
#include "runtime/mem-pool.h"

namespace impala {

class GzipDecompressor : public Codec {
 public:
<<<<<<< HEAD
  GzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~GzipDecompressor();

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Initialize the decompressor.
  virtual Status Init();

 private:
=======
  virtual ~GzipDecompressor();
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual Status ProcessBlockStreaming(int64_t input_length, const uint8_t* input,
      int64_t* input_bytes_read, int64_t* output_length, uint8_t** output, bool* eos);
  virtual std::string file_extension() const { return "gz"; }

 private:
  friend class Codec;
  GzipDecompressor(
      MemPool* mem_pool = NULL, bool reuse_buffer = false, bool is_deflate = false);
  virtual Status Init();
  std::string DebugStreamState() const;

  // If set assume deflate format, otherwise zlib or gzip
  bool is_deflate_;

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  z_stream stream_;

  // These are magic numbers from zlib.h.  Not clear why they are not defined there.
  const static int WINDOW_BITS = 15;    // Maximum window size
  const static int DETECT_CODEC = 32;   // Determine if this is libz or gzip from header.
};

class BzipDecompressor : public Codec {
 public:
<<<<<<< HEAD
  BzipDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~BzipDecompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);
 protected:
  // Bzip does not need initialization
  virtual Status Init() { return Status::OK;  }

=======
  virtual ~BzipDecompressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated,
                              int64_t input_length, const uint8_t* input,
                              int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "bz2"; }
 private:
  friend class Codec;
  BzipDecompressor(MemPool* mem_pool, bool reuse_buffer);

  virtual Status Init() { return Status::OK; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

class SnappyDecompressor : public Codec {
 public:
<<<<<<< HEAD
  SnappyDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyDecompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

};
class SnappyBlockDecompressor : public Codec {
 public:
  SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual ~SnappyBlockDecompressor() { }

  //Process a block of data.
  virtual Status ProcessBlock(int input_length, uint8_t* input,
                              int* output_length, uint8_t** output);

 protected:
  // Snappy does not need initialization
  virtual Status Init() { return Status::OK; }

=======
  // Snappy-compressed data block includes trailing 4-byte checksum. Decompressor
  // doesn't expect this.
  static const uint TRAILING_CHECKSUM_LEN = 4;

  virtual ~SnappyDecompressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "snappy"; }

 private:
  friend class Codec;
  SnappyDecompressor(MemPool* mem_pool = NULL, bool reuse_buffer = false);
  virtual Status Init() { return Status::OK; }
};

// Lz4 is a compression codec with similar compression ratios as snappy but much faster
// decompression. This decompressor is not able to decompress unless the output buffer
// is allocated and will cause an error if asked to do so.
class Lz4Decompressor : public Codec {
 public:
  virtual ~Lz4Decompressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "lz4"; }

 private:
  friend class Codec;
  Lz4Decompressor(MemPool* mem_pool = NULL, bool reuse_buffer = false);
  virtual Status Init() { return Status::OK; }
};

class SnappyBlockDecompressor : public Codec {
 public:
  virtual ~SnappyBlockDecompressor() { }
  virtual int64_t MaxOutputLen(int64_t input_len, const uint8_t* input = NULL);
  virtual Status ProcessBlock(bool output_preallocated, int64_t input_length,
      const uint8_t* input, int64_t* output_length, uint8_t** output);
  virtual std::string file_extension() const { return "snappy"; }

 private:
  friend class Codec;
  SnappyBlockDecompressor(MemPool* mem_pool, bool reuse_buffer);
  virtual Status Init() { return Status::OK; }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

}
#endif
