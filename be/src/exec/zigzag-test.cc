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

#include <stdlib.h>
#include <stdio.h>
#include <iostream>
#include <limits.h>
#include <gtest/gtest.h>
<<<<<<< HEAD
=======
#include "common/status.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "exec/read-write-util.h"
#include "util/cpu-info.h"
#include "util/hash-util.h"

using namespace std;

namespace impala {

void TestZInt(int32_t value) {
  uint8_t buf[ReadWriteUtil::MAX_ZINT_LEN];
  int plen = ReadWriteUtil::PutZInt(value, static_cast<uint8_t*>(buf));
  EXPECT_TRUE(plen <= ReadWriteUtil::MAX_ZINT_LEN);
<<<<<<< HEAD
  int32_t output;
  int glen = ReadWriteUtil::GetZInt(static_cast<uint8_t*>(buf), &output);
  EXPECT_EQ(plen, glen);
  EXPECT_EQ(value, output);
=======

  uint8_t* buf_ptr = static_cast<uint8_t*>(buf);
  int32_t val = ReadWriteUtil::ReadZInt(&buf_ptr);
  EXPECT_EQ(value, val);
  int len = buf_ptr - buf;
  EXPECT_GT(len, 0);
  EXPECT_LE(len, sizeof(buf));
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

void TestZLong(int64_t value) {
  uint8_t buf[ReadWriteUtil::MAX_ZLONG_LEN];
  int plen = ReadWriteUtil::PutZLong(value, static_cast<uint8_t*>(buf));
  EXPECT_TRUE(plen <= ReadWriteUtil::MAX_ZLONG_LEN);
<<<<<<< HEAD
  int64_t output;
  int glen = ReadWriteUtil::GetZLong(static_cast<uint8_t*>(buf), &output);
  EXPECT_EQ(plen, glen);
  EXPECT_EQ(value, output);
}


// Test put and get of zigzag integers and longs.
TEST(ZigzagTest, Basic) { 
=======

  uint8_t* buf_ptr = static_cast<uint8_t*>(buf);
  int64_t val = ReadWriteUtil::ReadZLong(&buf_ptr);
  EXPECT_EQ(value, val);
  int len = buf_ptr - buf;
  EXPECT_GT(len, 0);
  EXPECT_LE(len, sizeof(buf));
}

// Test put and get of zigzag integers and longs.
TEST(ZigzagTest, Basic) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Test min/max of all sizes.
  TestZInt(0);
  TestZInt(INT_MAX);
  TestZInt(INT_MIN);
  TestZInt(SHRT_MIN);
  TestZInt(SHRT_MAX);
  TestZInt(0);
  TestZLong(LONG_MAX);
  TestZLong(LONG_MIN);
  TestZLong(INT_MAX);
  TestZLong(INT_MIN);
  TestZLong(SHRT_MIN);
  TestZLong(SHRT_MAX);
  TestZLong(SCHAR_MIN);
  TestZLong(SCHAR_MAX);
  // Test somewhat random bit patterns.
  int32_t value = 0xa2a2a2a2;
  for (int i = 0; i < 1000; ++i) {
<<<<<<< HEAD
    value = HashUtil::CrcHash(&value, sizeof (value), i);
=======
    value = HashUtil::Hash(&value, sizeof (value), i);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    TestZInt(value);
    TestZLong(value);
    TestZLong((static_cast<int64_t>(value) << 32) | value);
  }
<<<<<<< HEAD

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
}

int main(int argc, char **argv) {
  impala::CpuInfo::Init();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
<<<<<<< HEAD

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
