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

#include "runtime/disk-io-mgr-stress.h"

<<<<<<< HEAD
=======
#include "util/time.h"

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
using namespace boost;
using namespace impala;
using namespace std;

static const float ABORT_CHANCE = .10f;
static const int MIN_READ_LEN = 1;
static const int MAX_READ_LEN = 20;

static const int MIN_FILE_LEN = 10;
static const int MAX_FILE_LEN = 1024;

<<<<<<< HEAD
static const int NUM_BUFFERS_PER_DISK = 3;

// Make sure this is between MIN/MAX FILE_LEN to test more cases
static const int READ_BUFFER_SIZE = 128;
=======
static const int MAX_BUFFERS = 12;

// Make sure this is between MIN/MAX FILE_LEN to test more cases
static const int MIN_READ_BUFFER_SIZE = 64;
static const int MAX_READ_BUFFER_SIZE = 128;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

static const int CANCEL_READER_PERIOD_MS = 20;  // in ms

static void CreateTempFile(const char* filename, const char* data) {
  FILE* file = fopen(filename, "w");
  CHECK(file != NULL);
  fwrite(data, 1, strlen(data), file);
  fclose(file);
}

string GenerateRandomData() {
  int rand_len = rand() % (MAX_FILE_LEN - MIN_FILE_LEN) + MIN_FILE_LEN;
  stringstream ss;
  for (int i = 0; i < rand_len; ++i) {
    char c = rand() % 26 + 'a';
    ss << c;
  }
  return ss.str();
}

struct DiskIoMgrStress::Client {
  boost::mutex lock;
<<<<<<< HEAD
  DiskIoMgr::ReaderContext* reader;
=======
  DiskIoMgr::RequestContext* reader;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  int file_idx;
  vector<DiskIoMgr::ScanRange*> scan_ranges;
  int abort_at_byte;
  int files_processed;
};

<<<<<<< HEAD
DiskIoMgrStress::DiskIoMgrStress(int num_disks, int num_threads_per_disk, int num_clients, 
      bool includes_cancellation) :
    num_clients_(num_clients),
    includes_cancellation_(includes_cancellation) {
  
=======
DiskIoMgrStress::DiskIoMgrStress(int num_disks, int num_threads_per_disk,
     int num_clients, bool includes_cancellation) :
    num_clients_(num_clients),
    includes_cancellation_(includes_cancellation) {

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  time_t rand_seed = time(NULL);
  LOG(INFO) << "Running with rand seed: " << rand_seed;
  srand(rand_seed);

<<<<<<< HEAD
  io_mgr_.reset(new DiskIoMgr(num_disks, num_threads_per_disk, READ_BUFFER_SIZE));
  Status status = io_mgr_->Init();
  CHECK(status.ok());
  
=======
  io_mgr_.reset(new DiskIoMgr(
      num_disks, num_threads_per_disk, MIN_READ_BUFFER_SIZE, MAX_READ_BUFFER_SIZE));
  Status status = io_mgr_->Init(&dummy_tracker_);
  CHECK(status.ok());

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Initialize some data files.  It doesn't really matter how many there are.
  files_.resize(num_clients * 2);
  for (int i = 0; i < files_.size(); ++i) {
    stringstream ss;
    ss << "/tmp/disk_io_mgr_stress_file" << i;
    files_[i].filename = ss.str();
    files_[i].data = GenerateRandomData();
    CreateTempFile(files_[i].filename.c_str(), files_[i].data.c_str());
  }

  clients_ = new Client[num_clients_];
  for (int i = 0; i < num_clients_; ++i) {
    NewClient(i);
  }
}

void DiskIoMgrStress::ClientThread(int client_id) {
  Client* client = &clients_[client_id];
  Status status;
  char read_buffer[MAX_FILE_LEN];
<<<<<<< HEAD
  int bytes_read = 0;

  while (!shutdown_) {
    bool eos;
    DiskIoMgr::BufferDescriptor* buffer;
    Status status = io_mgr_->GetNext(client->reader, &buffer, &eos);

    CHECK(status.ok() || status.IsCancelled());

    const string& expected = files_[client->file_idx].data;

    if (buffer != NULL && buffer->buffer() != NULL) {
      DiskIoMgr::ScanRange* range = buffer->scan_range();
      int64_t scan_range_offset = buffer->scan_range_offset();
      int len = buffer->len();
      
      CHECK(range != NULL);
      CHECK_GE(scan_range_offset, 0);
      CHECK_LT(scan_range_offset, expected.size());
      CHECK_GT(len, 0);

      // We get scan ranges back in arbitrary order so the scan range to the file
      // offset.
      int64_t file_offset = scan_range_offset + range->offset();

      // Validate the bytes read
      CHECK_LE(file_offset + len, expected.size());
      CHECK_EQ(strncmp(buffer->buffer(), &expected.c_str()[file_offset], len), 0);

      // Copy the bytes from this read into the result buffer.  
      memcpy(read_buffer + file_offset, buffer->buffer(), buffer->len());
      buffer->Return();
      buffer = NULL;
      bytes_read += len;
    }
    CHECK_GE(bytes_read, 0);
    CHECK_LE(bytes_read, expected.size());

    // If the entire file (i.e. all scan ranges) are finished or the client is
    // aborted, validate the bytes, unregister the current reader and create
    // a new one.
    if ((bytes_read > client->abort_at_byte) || eos || !status.ok()) {
      if (bytes_read == expected.size()) {
        CHECK(status.ok());
        // This entire file was read without being cancelled, validate the entire result
        CHECK_EQ(strncmp(read_buffer, expected.c_str(), bytes_read), 0);
      }
      
      // Unregister the old client and get a new one
      unique_lock<mutex> lock(client->lock);
      io_mgr_->UnregisterReader(client->reader);
      NewClient(client_id);
      bytes_read = 0;
    }
  }
  unique_lock<mutex> lock(client->lock);
  io_mgr_->UnregisterReader(client->reader);
=======

  while (!shutdown_) {
    bool eos = false;
    int bytes_read = 0;

    const string& expected = files_[client->file_idx].data;

    while (!eos) {
      DiskIoMgr::ScanRange* range;
      Status status = io_mgr_->GetNextRange(client->reader, &range);
      CHECK(status.ok() || status.IsCancelled());
      if (range == NULL) break;

      while (true) {
        DiskIoMgr::BufferDescriptor* buffer;
        status = range->GetNext(&buffer);
        CHECK(status.ok() || status.IsCancelled());
        if (buffer == NULL) break;

        int64_t scan_range_offset = buffer->scan_range_offset();
        int len = buffer->len();
        CHECK_GE(scan_range_offset, 0);
        CHECK_LT(scan_range_offset, expected.size());
        CHECK_GT(len, 0);

        // We get scan ranges back in arbitrary order so the scan range to the file
        // offset.
        int64_t file_offset = scan_range_offset + range->offset();

        // Validate the bytes read
        CHECK_LE(file_offset + len, expected.size());
        CHECK_EQ(strncmp(buffer->buffer(), &expected.c_str()[file_offset], len), 0);

        // Copy the bytes from this read into the result buffer.
        memcpy(read_buffer + file_offset, buffer->buffer(), buffer->len());
        buffer->Return();
        buffer = NULL;
        bytes_read += len;

        CHECK_GE(bytes_read, 0);
        CHECK_LE(bytes_read, expected.size());

        if (bytes_read > client->abort_at_byte) {
          eos = true;
          break;
        }
      } // End of buffer
    } // End of scan range

    if (bytes_read == expected.size()) {
      // This entire file was read without being cancelled, validate the entire result
      CHECK(status.ok());
      CHECK_EQ(strncmp(read_buffer, expected.c_str(), bytes_read), 0);
    }

    // Unregister the old client and get a new one
    unique_lock<mutex> lock(client->lock);
    io_mgr_->UnregisterContext(client->reader);
    NewClient(client_id);
  }

  unique_lock<mutex> lock(client->lock);
  io_mgr_->UnregisterContext(client->reader);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  client->reader = NULL;
}

// Cancel a random reader
void DiskIoMgrStress::CancelRandomReader() {
  if (!includes_cancellation_) return;

  int rand_client = rand() % num_clients_;
<<<<<<< HEAD
  
  unique_lock<mutex> lock(clients_[rand_client].lock);
  io_mgr_->CancelReader(clients_[rand_client].reader);
=======

  unique_lock<mutex> lock(clients_[rand_client].lock);
  io_mgr_->CancelContext(clients_[rand_client].reader);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

void DiskIoMgrStress::Run(int sec) {
  shutdown_ = false;
  for (int i = 0; i < num_clients_; ++i) {
    readers_.add_thread(
        new thread(&DiskIoMgrStress::ClientThread, this, i));
  }
<<<<<<< HEAD
  
=======

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Sleep and let the clients do their thing for 'sec'
  for (int loop_count = 1; sec == 0 || loop_count <= sec; ++loop_count) {
    int iter = (1000) / CANCEL_READER_PERIOD_MS;
    for (int i = 0; i < iter; ++i) {
<<<<<<< HEAD
      usleep(CANCEL_READER_PERIOD_MS * 1000);
=======
      SleepForMs(CANCEL_READER_PERIOD_MS);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      CancelRandomReader();
    }
    LOG(ERROR) << "Finished iteration: " << loop_count;
  }
<<<<<<< HEAD
  
=======

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // Signal shutdown for the client threads
  shutdown_ = true;

  for (int i = 0; i < num_clients_; ++i) {
    unique_lock<mutex> lock(clients_[i].lock);
<<<<<<< HEAD
    if (clients_[i].reader != NULL) io_mgr_->CancelReader(clients_[i].reader);
=======
    if (clients_[i].reader != NULL) io_mgr_->CancelContext(clients_[i].reader);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  readers_.join_all();
}

// Initialize a client to read one of the files at random.  The scan ranges are
// assigned randomly.
void DiskIoMgrStress::NewClient(int i) {
  Client& client = clients_[i];
  ++client.files_processed;
  client.file_idx = rand() % files_.size();
  int file_len = files_[client.file_idx].data.size();

  client.abort_at_byte = file_len;

  if (includes_cancellation_) {
    float rand_value = rand() / (float)RAND_MAX;
    if (rand_value < ABORT_CHANCE) {
      // Abort at a random byte inside the file
      client.abort_at_byte = rand() % file_len;
<<<<<<< HEAD
    } 
=======
    }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  for (int i = 0; i < client.scan_ranges.size(); ++i) {
    delete client.scan_ranges[i];
  }
  client.scan_ranges.clear();

  int assigned_len = 0;
  while (assigned_len < file_len) {
    int range_len = rand() % (MAX_READ_LEN - MIN_READ_LEN) + MIN_READ_LEN;
    range_len = min(range_len, file_len - assigned_len);
<<<<<<< HEAD
    
    DiskIoMgr::ScanRange* range = new DiskIoMgr::ScanRange();;
    range->Reset(files_[client.file_idx].filename.c_str(), range_len, assigned_len, 0);
    client.scan_ranges.push_back(range);
    assigned_len += range_len;
  }
  Status status = io_mgr_->RegisterReader(NULL, NUM_BUFFERS_PER_DISK, &client.reader);
=======

    DiskIoMgr::ScanRange* range = new DiskIoMgr::ScanRange();;
    range->Reset(files_[client.file_idx].filename.c_str(), range_len, assigned_len, 0,
                 false, false);
    client.scan_ranges.push_back(range);
    assigned_len += range_len;
  }
  Status status = io_mgr_->RegisterContext(NULL, &client.reader, NULL);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  CHECK(status.ok());
  status = io_mgr_->AddScanRanges(client.reader, client.scan_ranges);
  CHECK(status.ok());
}
