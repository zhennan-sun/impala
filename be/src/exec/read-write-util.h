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


#ifndef IMPALA_EXEC_READ_WRITE_UTIL_H
#define IMPALA_EXEC_READ_WRITE_UTIL_H

<<<<<<< HEAD
#include "exec/hdfs-byte-stream.h"

class ByteStream;

namespace impala {

// Class for reading and writing various data types supported by Trevni and Avro.
class ReadWriteUtil {
 public:
=======
#include <boost/cstdint.hpp>
#include <sstream>
#include "common/logging.h"
#include "common/status.h"
#include "util/bit-util.h"

namespace impala {

#define RETURN_IF_FALSE(x) if (UNLIKELY(!(x))) return false

// Class for reading and writing various data types.
// Note: be very careful using *signed* ints.  Casting from a signed int to
// an unsigned is not a problem.  However, bit shifts will do sign extension
// on unsigned ints, which is rarely the right thing to do for byte level
// operations.
class ReadWriteUtil {
 public:
  // Maximum length for Writeable VInt
  static const int MAX_VINT_LEN = 9;

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Maximum lengths for Zigzag encodings.
  const static int MAX_ZINT_LEN = 5;
  const static int MAX_ZLONG_LEN = 10;

<<<<<<< HEAD
  // Read a Trevni string.
  static Status ReadString(ByteStream* byte_stream, std::string* str);

  // Read Trevni bytes.
  static Status ReadBytes(ByteStream* byte_stream, std::vector<uint8_t>* buf);

  // Read the length of the byte or string value and skip over it.
  static Status SkipBytes(ByteStream* byte_stream);

  // Return an integer from a buffer, stored little endian.
  static int32_t GetInt(uint8_t* buf) {
    return buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24);
  }

  // Return a long from a buffer, stored little endian.
  static int64_t GetLong(uint8_t* buf) {
    return buf[0] | (buf[1] << 8) | (buf[2] << 16) | (buf[3] << 24) |
        (static_cast<int64_t>(buf[4]) << 32) |
        (static_cast<int64_t>(buf[5]) << 40) |
        (static_cast<int64_t>(buf[6]) << 48) |
        (static_cast<int64_t>(buf[7]) << 56);
  }

  // Read an integer or long from the current byte stream.  Stored
  // little endian.
  template <typename T>
  static Status ReadInt(ByteStream* byte_stream, T* integer) {
    uint8_t buf[sizeof(T)];
    int64_t bytes_read;
    RETURN_IF_ERROR(byte_stream->Read(buf, sizeof(T), &bytes_read));
    if (bytes_read != sizeof(T)) {
      return Status("Short read in ReadInt");
    }
    if (sizeof(T) == sizeof(int32_t)) {
      *integer = GetInt(buf);
    } else {
      *integer = GetLong(buf);
    }
    return Status::OK;
  }

  // Get a zigzag encoded long integer from a buffer and return its length.
  // This is the integer encoding defined by google.com protocol-buffers:
  // https://developers.google.com/protocol-buffers/docs/encoding
  static int GetZLong(uint8_t* buf, int64_t* value) {
    uint64_t zlong = 0;
    int shift = 0;
    uint8_t* bp = buf;

    do {
      DCHECK_LE(shift, 64);
      zlong |= static_cast<uint64_t>(*bp & 0x7f) << shift;
      shift += 7;
    } while ((*(bp++) & 0x80) != 0);

    *value = (zlong >> 1) ^ -(zlong & 1);
    return bp - buf;
  }

  // Get a zigzag encoded integer from a buffer and return its length.
  static int GetZInt(uint8_t* buf, int32_t* integer) {
    uint32_t zint = 0;
    int shift = 0;
    uint8_t* bp = buf;

    do {
      DCHECK_LE(shift, 32);
      zint |= static_cast<uint32_t>(*bp & 0x7f) << shift;
      shift += 7;
    } while (*(bp++) & 0x80);

    *integer = (zint >> 1) ^ -(zint & 1);
    return bp - buf;
  }

  // Read a zigzag encoded integer from the current byte stream.
  static Status ReadZInt(ByteStream* byte_stream, int32_t* integer);

  // Read a zigzag encoded long integer from the current byte stream.
  static Status ReadZLong(ByteStream* byte_stream, int64_t* longint);

  // Put a zigzag encoded integer into a buffer and return its length.
  static int PutZInt(int32_t integer, uint8_t* buf);

  // Put a zigzag encoded long integer into a buffer and return its length.
  static int PutZLong(int64_t longint, uint8_t* buf);

  // Write a zigzag encoded integer to the output file.
  static Status WriteZInt(ByteStream* byte_stream, int32_t integer);

  // Write a zigzag encoded long integer to the output file.
  static Status WriteZLong(ByteStream* byte_stream, int64_t longint);

  // Put an integer into a buffer in little endian order.
  static void PutInt(int32_t integer, uint8_t* buf) {
    buf[0] = integer;
    buf[1] = integer >> 8;
    buf[2] = integer >> 16;
    buf[3] = integer >> 24;
  }

  // Put a long integer into a buffer in little endian order.
  static void PutLong(int64_t longint, uint8_t* buf) {
    buf[0] = longint;
    buf[1] = longint >> 8;
    buf[2] = longint >> 16;
    buf[3] = longint >> 24;
    buf[4] = longint >> 32;
    buf[5] = longint >> 40;
    buf[6] = longint >> 48;
    buf[7] = longint >> 56;
  }
};
=======
  // Put a zigzag encoded integer into a buffer and return its length.
  static int PutZInt(int32_t integer, uint8_t* buf);

  // Put a zigzag encoded long integer into a buffer and return its length.
  static int PutZLong(int64_t longint, uint8_t* buf);

  // Get a big endian integer from a buffer.  The buffer does not have to be word aligned.
  template<typename T>
  static T GetInt(const uint8_t* buffer);

  // Get a variable-length Long or int value from a byte buffer.
  // Returns the length of the long/int
  // If the size byte is corrupted then return -1;
  static int GetVLong(uint8_t* buf, int64_t* vlong);
  static int GetVInt(uint8_t* buf, int32_t* vint);

  // Writes a variable-length Long or int value to a byte buffer.
  // Returns the number of bytes written
  static int64_t PutVLong(int64_t val, uint8_t* buf);
  static int64_t PutVInt(int32_t val, uint8_t* buf);

  // returns size of the encoded long value, not including the 1 byte for length
  static int VLongRequiredBytes(int64_t val);

  // Read a variable-length Long value from a byte buffer starting at the specified
  // byte offset.
  static int GetVLong(uint8_t* buf, int64_t offset, int64_t* vlong);

  // Put an Integer into a buffer in big endian order.  The buffer must be big
  // enough.
  static void PutInt(uint8_t* buf, uint16_t integer);
  static void PutInt(uint8_t* buf, uint32_t integer);
  static void PutInt(uint8_t* buf, uint64_t integer);

  // Dump the first length bytes of buf to a Hex string.
  static std::string HexDump(const uint8_t* buf, int64_t length);
  static std::string HexDump(const char* buf, int64_t length);

  // Determines the sign of a VInt/VLong from the first byte.
  static bool IsNegativeVInt(int8_t byte);

  // Determines the total length in bytes of a Writable VInt/VLong from the first byte.
  static int DecodeVIntSize(int8_t byte);

  // Read a zig-zag encoded long. This is the integer encoding defined by google.com
  // protocol-buffers: https://developers.google.com/protocol-buffers/docs/encoding
  // *buf is incremented past the encoded long.
  static int64_t ReadZLong(uint8_t** buf);

  // Read a zig-zag encoded int.
  static int32_t ReadZInt(uint8_t** buf);

  // The following methods read data from a buffer without assuming the buffer is long
  // enough. If the buffer isn't long enough or another error occurs, they return false
  // and update the status with the error. Otherwise they return true. buffer is advanced
  // past the data read and buf_len is decremented appropriately.

  // Read a native type T (e.g. bool, float) directly into output (i.e. input is cast
  // directly to T and incremented by sizeof(T)).
  template <class T>
  static bool Read(uint8_t** buf, int* buf_len, T* val, Status* status);

  // Skip the next num_bytes bytes.
  static bool SkipBytes(uint8_t** buf, int* buf_len, int num_bytes, Status* status);
};

template<>
inline uint16_t ReadWriteUtil::GetInt(const uint8_t* buf) {
  return (buf[0] << 8) | buf[1];
}

template<>
inline uint32_t ReadWriteUtil::GetInt(const uint8_t* buf) {
  return (buf[0] << 24) | (buf[1] << 16) | (buf[2] << 8) | buf[3];
}

template<>
inline uint64_t ReadWriteUtil::GetInt(const uint8_t* buf) {
  uint64_t upper_half = GetInt<uint32_t>(buf);
  uint64_t lower_half = GetInt<uint32_t>(buf + 4);
  return lower_half | upper_half << 32;
}

inline void ReadWriteUtil::PutInt(uint8_t* buf, uint16_t integer) {
  buf[0] = integer >> 8;
  buf[1] = integer;
}

inline void ReadWriteUtil::PutInt(uint8_t* buf, uint32_t integer) {
  uint32_t big_endian = BitUtil::ByteSwap(integer);
  memcpy(buf, &big_endian, sizeof(uint32_t));
}

inline void ReadWriteUtil::PutInt(uint8_t* buf, uint64_t integer) {
  uint64_t big_endian = BitUtil::ByteSwap(integer);
  memcpy(buf, &big_endian, sizeof(uint64_t));
}

inline int ReadWriteUtil::GetVInt(uint8_t* buf, int32_t* vint) {
  int64_t vlong = 0;
  int len = GetVLong(buf, &vlong);
  *vint = static_cast<int32_t>(vlong);
  return len;
}

inline int ReadWriteUtil::GetVLong(uint8_t* buf, int64_t* vlong) {
  return GetVLong(buf, 0, vlong);
}

inline int ReadWriteUtil::GetVLong(uint8_t* buf, int64_t offset, int64_t* vlong) {
  int8_t firstbyte = (int8_t) buf[0 + offset];

  int len = DecodeVIntSize(firstbyte);
  if (len > MAX_VINT_LEN) return -1;
  if (len == 1) {
    *vlong = static_cast<int64_t>(firstbyte);
    return len;
  }

  *vlong &= ~*vlong;

  for (int i = 1; i < len; i++) {
    *vlong = (*vlong << 8) | buf[i+offset];
  }

  if (IsNegativeVInt(firstbyte)) {
    *vlong = *vlong ^ ((int64_t) - 1);
  }

  return len;
}

inline int ReadWriteUtil::VLongRequiredBytes(int64_t val) {
  // returns size of the encoded long value, not including the 1 byte for length
  if (val & 0xFF00000000000000llu) return 8;
  if (val & 0x00FF000000000000llu) return 7;
  if (val & 0x0000FF0000000000llu) return 6;
  if (val & 0x000000FF00000000llu) return 5;
  if (val & 0x00000000FF000000llu) return 4;
  if (val & 0x0000000000FF0000llu) return 3;
  if (val & 0x000000000000FF00llu) return 2;
  // Values between -112 and 127 are stored using 1 byte,
  // values between -127 and -112 are stored using 2 bytes
  // See ReadWriteUtil::DecodeVIntSize for this case
  if (val < -112) return 2;
  return 1;
}

inline int64_t ReadWriteUtil::PutVLong(int64_t val, uint8_t* buf) {
  int64_t num_bytes = VLongRequiredBytes(val);

  if (num_bytes == 1) {
    // store the value itself instead of the length
    buf[0] = static_cast<int8_t>(val);
    return 1;
  }

  // This is how we encode the length for a length less than or equal to 8
  buf[0] = -119 + num_bytes;

  // write to buffer in reversed endianness
  for (int i = 0; i < num_bytes; ++i) {
    buf[i+1] = (val >> (8 * (num_bytes - i - 1))) & 0xFF;
  }

  // +1 for the length byte
  return num_bytes + 1;
}

inline int64_t ReadWriteUtil::PutVInt(int32_t val, uint8_t* buf) {
  return PutVLong(val, buf);
}

inline int32_t ReadWriteUtil::ReadZInt(uint8_t** buf) {
  int64_t zlong = ReadZLong(buf);
  return static_cast<int32_t>(zlong);
}

template <class T>
inline bool ReadWriteUtil::Read(uint8_t** buf, int* buf_len, T* val, Status* status) {
  int val_len = sizeof(T);
  if (UNLIKELY(val_len > *buf_len)) {
    std::stringstream ss;
    ss << "Cannot read " << val_len << " bytes, buffer length is " << *buf_len;
    *status = Status(ss.str());
    return false;
  }
  *val = *reinterpret_cast<T*>(*buf);
  *buf += val_len;
  *buf_len -= val_len;
  return true;
}

inline bool ReadWriteUtil::SkipBytes(uint8_t** buf, int* buf_len, int num_bytes,
                                     Status* status) {
  DCHECK_GE(*buf_len, 0);
  if (UNLIKELY(num_bytes > *buf_len)) {
    std::stringstream ss;
    ss << "Cannot skip " << num_bytes << " bytes, buffer length is " << *buf_len;
    *status = Status(ss.str());
    return false;
  }
  *buf += num_bytes;
  *buf_len -= num_bytes;
  return true;
}

inline bool ReadWriteUtil::IsNegativeVInt(int8_t byte) {
  return byte < -120 || (byte >= -112 && byte < 0);
}

inline int ReadWriteUtil::DecodeVIntSize(int8_t byte) {
  if (byte >= -112) {
    return 1;
  } else if (byte < -120) {
    return -119 - byte;
  }
  return -111 - byte;
}
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

}
#endif
