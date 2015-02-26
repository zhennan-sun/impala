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
#include <gtest/gtest.h>
<<<<<<< HEAD
#include "util/cpu-info.h"
=======
#include "common/init.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "util/disk-info.h"
#include "util/perf-counters.h"

using namespace std;

namespace impala {

<<<<<<< HEAD
TEST(PerfCounterTest, Basic) { 
=======
TEST(PerfCounterTest, Basic) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  PerfCounters counters;
  EXPECT_TRUE(counters.AddDefaultCounters());

  counters.Snapshot("Before");
<<<<<<< HEAD

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  double result = 0;
  for (int i = 0; i < 1000000; i++) {
    double d1 = rand() / (double) RAND_MAX;
    double d2 = rand() / (double) RAND_MAX;
    result = d1*d1 + d2*d2;
  }
  counters.Snapshot("After");

  for (int i = 0; i < 1000000; i++) {
    double d1 = rand() / (double) RAND_MAX;
    double d2 = rand() / (double) RAND_MAX;
    result = d1*d1 + d2*d2;
  }
  counters.Snapshot("After2");
  counters.PrettyPrint(&cout);
}

TEST(CpuInfoTest, Basic) {
  cout << CpuInfo::DebugString();
}

TEST(DiskInfoTest, Basic) {
  cout << DiskInfo::DebugString();
  cout << "Device name for disk 0: " << DiskInfo::device_name(0) << endl;

  int disk_id_home_dir = DiskInfo::disk_id("/home");
  cout << "Device name for '/home': " << DiskInfo::device_name(disk_id_home_dir) << endl;
}

}

int main(int argc, char **argv) {
<<<<<<< HEAD
  ::testing::InitGoogleTest(&argc, argv);
  impala::CpuInfo::Init();
  impala::DiskInfo::Init();
=======
  InitCommonRuntime(argc, argv, false);
  ::testing::InitGoogleTest(&argc, argv);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return RUN_ALL_TESTS();
}

