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

#include "exec/read-write-util.h"
<<<<<<< HEAD
#include "exec/byte-stream.h"
=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

using namespace std;
using namespace impala;

<<<<<<< HEAD
Status ReadWriteUtil::ReadZLong(ByteStream* byte_stream, int64_t* value) {
  uint8_t buf[MAX_ZLONG_LEN];
  int64_t nread;

  RETURN_IF_ERROR(byte_stream->Read(buf, MAX_ZLONG_LEN, &nread));

  int len = GetZLong(buf, value);
  if (len > nread) {
    return Status("Bad integer format");
  } else if (len < nread) {
    RETURN_IF_ERROR(byte_stream->SeekRelative(len - nread));
  }
  
  return Status::OK;
}

Status ReadWriteUtil::ReadZInt(ByteStream* byte_stream, int32_t* integer) {
  uint8_t buf[MAX_ZINT_LEN];
  int64_t nread;

  RETURN_IF_ERROR(byte_stream->Read(buf, MAX_ZINT_LEN, &nread));

  int len = GetZInt(buf, integer);
  if (len > nread) {
    return Status("Bad integer format");
  } else if (len < nread) {
    RETURN_IF_ERROR(byte_stream->SeekRelative(len - nread));
  }
  
  return Status::OK;
}

Status ReadWriteUtil::ReadString(ByteStream* byte_stream, string* str) {
  int64_t len;
  RETURN_IF_ERROR(ReadZLong(byte_stream, &len));
  str->reserve(len);
  str->resize(len);
  int64_t bytes_read;
  RETURN_IF_ERROR(byte_stream->Read(
      reinterpret_cast<uint8_t*>(&(*str)[0]), len, &bytes_read));
  if (len != bytes_read) {
    return Status("Short read in ReadString");
  }
  return Status::OK;
}

Status ReadWriteUtil::ReadBytes(ByteStream* byte_stream, vector<uint8_t>* buf) {
  int64_t len;
  RETURN_IF_ERROR(ReadZLong(byte_stream, &len));
  buf->reserve(len);
  buf->resize(len);
  int64_t bytes_read;
  RETURN_IF_ERROR(byte_stream->Read(&(*buf)[0], len, &bytes_read));
  if (len != bytes_read) {
    return Status("Short read in ReadBytes");
  }
  return Status::OK;
}

Status ReadWriteUtil::SkipBytes(ByteStream* byte_stream) {
  int64_t len;
  RETURN_IF_ERROR(ReadZLong(byte_stream, &len));
  return byte_stream->SeekRelative(len);
=======
// This function is not inlined because it can potentially cause LLVM to crash (see
// http://llvm.org/bugs/show_bug.cgi?id=19315), and inlining does not appear to have any
// performance impact.
int64_t ReadWriteUtil::ReadZLong(uint8_t** buf) {
  uint64_t zlong = 0;
  int shift = 0;
  bool more;
  do {
    DCHECK_LE(shift, 64);
    zlong |= static_cast<uint64_t>(**buf & 0x7f) << shift;
    shift += 7;
    more = (**buf & 0x80) != 0;
    ++(*buf);
  } while (more);
  return (zlong >> 1) ^ -(zlong & 1);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

int ReadWriteUtil::PutZInt(int32_t integer, uint8_t* buf) {
  // Move the sign bit to the first bit.
  uint32_t uinteger = (integer << 1) ^ (integer >> 31);
  const int mask = 0x7f;
  const int cont = 0x80;
  buf[0] = uinteger & mask;
  int len = 1;
  while ((uinteger >>= 7) != 0) {
    // Set the continuation bit.
    buf[len - 1] |= cont;
    buf[len] = uinteger & mask;
    ++len;
  }

  return len;
}

int ReadWriteUtil::PutZLong(int64_t longint, uint8_t* buf) {
  // Move the sign bit to the first bit.
  uint64_t ulongint = (longint << 1) ^ (longint >> 63);
  const int mask = 0x7f;
  const int cont = 0x80;
  buf[0] = ulongint & mask;
  int len = 1;
  while ((ulongint >>= 7) != 0) {
    // Set the continuation bit.
    buf[len - 1] |= cont;
    buf[len] = ulongint & mask;
    ++len;
  }

  return len;
}

<<<<<<< HEAD
=======
string ReadWriteUtil::HexDump(const uint8_t* buf, int64_t length) {
  stringstream ss;
  ss << std::hex;
  for (int i = 0; i < length; ++i) {
    ss << static_cast<int>(buf[i]) << " ";
  }
  return ss.str();
}

string ReadWriteUtil::HexDump(const char* buf, int64_t length) {
  return HexDump(reinterpret_cast<const uint8_t*>(buf), length);
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
