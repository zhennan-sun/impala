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

#include "runtime/disk-io-mgr.h"
<<<<<<< HEAD

#include <queue>
#include <boost/functional/hash.hpp>
#include <boost/scoped_ptr.hpp>
#include <boost/thread/locks.hpp>

#include "common/logging.h"
#include "util/disk-info.h"
#include "util/hdfs-util.h"

// Control the number of disks on the machine.  If 0, this comes from the system 
// settings.
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
// Default IO Mgr configs.
=======
#include "runtime/disk-io-mgr-internal.h"

#include <gutil/strings/substitute.h>
#include <boost/algorithm/string.hpp>

DECLARE_bool(disable_mem_pools);

using namespace boost;
using namespace impala;
using namespace std;
using namespace strings;

// Control the number of disks on the machine.  If 0, this comes from the system
// settings.
DEFINE_int32(num_disks, 0, "Number of disks on data node.");
// Default IoMgr configs.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
// The maximum number of the threads per disk is also the max queue depth per disk.
// The read size is the size of the reads sent to hdfs/os.
// There is a trade off of latency and throughout, trying to keep disks busy but
// not introduce seeks.  The literature seems to agree that with 8 MB reads, random
// io and sequential io perform similarly.
<<<<<<< HEAD
DEFINE_int32(num_threads_per_disk, 1, "number of threads per disk");
DEFINE_int32(read_size, 8 * 1024 * 1024, "Read Size (in bytes)");

using namespace boost;
using namespace impala;
using namespace std;

// Internal per reader state
struct DiskIoMgr::ReaderContext {
  enum State {
    // Reader is initialized and maps to a client
    Active,

    // Reader is in the process of being cancelled.  Cancellation is coordinated between
    // different threads and when they are all complete, the reader is moved to the
    // inactive state.
    Cancelled,

    // Reader does not map to a client.  Accessing memory in a reader in this state
    // is invalid (i.e. it is equivalent to a dangling pointer).
    Inactive,
  };
  
  friend class DiskIoMgr;
  
  // Total bytes read for this reader
  RuntimeProfile::Counter* bytes_read_counter_;

  // Total time spent in hdfs reading
  RuntimeProfile::Counter* read_timer_;

  // hdfsFS connection handle.  This is set once and never changed for the duration 
  // of the reader.  NULL if this is a local reader.
  hdfsFS hdfs_connection_;

  // The number of io buffers this reader has per disk.  This is constant for a single
  // reader.
  int num_buffers_per_disk_;

  // Condition variable for GetNext
  condition_variable buffer_ready_cond_var_;

  // Condition variable for UnregisterReader to wait for all disks to complete
  condition_variable disks_complete_cond_var_;

  // All fields below are accessed by multiple threads and the lock needs to be
  // taken before accessing them.
  mutex lock_;
  
  // Current state of the reader
  State state_;

  // The number of buffers allocated for this reader that are empty.  
  int num_empty_buffers_;

  // The number of buffers currently owned by the reader
  int num_outstanding_buffers_;

  // The total number of scan ranges that have not been returned via GetNext().
  int num_remaining_scan_ranges_;

  // list of buffers that have been read from disk but not returned to the reader.
  // Also contains dummy buffers (just an error code)  signaling failed reads.  
  // In the non-error state, ready_buffers_.size() + num_empty_buffers_ 
  // will be the total number of buffers allocated for this reader.
  list<BufferDescriptor*> ready_buffers_;
  
  // The number of disks with scan ranges remaining (always equal to the sum of
  // non-empty ranges in per disk states).
  int num_disks_with_ranges_;

  // If true, this is a reader from Read()
  bool sync_reader_;

  // Struct containing state per disk. See comments in the disk read loop on how 
  // they are used.
  struct PerDiskState {
    // For each disks, the number of scan ranges that have not been fully read.
    int num_scan_ranges;
  
    // Queue of scan ranges that can be scheduled.  A scan range that is currently being
    // read by one thread, cannot be picked up by another thread and is temporarily
    // removed from the queue.  The size of this queue is always less than or equal
    // to num_scan_ranges. 
    // TODO: this is simpler to get right but not optimal.  We do not parallelize a
    // read from a single scan range.  Parallelizing a scan range requires more logic
    // to make sure the reader gets the scan range buffers in order.  We should do this.
    list<ScanRange*> ranges;

    // For each disk, keeps track if the reader is on that disk's queue.  This saves
    // us from having to walk the consumer list on the common path.
    // TODO: we should instead maintain the per queue double-linked links within the 
    // ReaderContext struct instead of using the stl.
    bool is_on_queue;

    // For each disk, the number of threads issuing the underlying read on behalf of
    // this reader. These threads do not have any locks taken and need to be accounted 
    // for during cancellation.  We need to make sure that their resources are not 
    // cleaned up while they are still in progress.  Only the thread that sees the count 
    // at 0 should do the final cleanup.
    int num_threads_in_read;

    // The number of empty buffers on this disk
    int num_empty_buffers;

    PerDiskState() {
      Reset(0);
    }

    void Reset(int per_disk_buffers) {
      num_scan_ranges = 0;
      ranges.clear();
      is_on_queue = false;
      num_threads_in_read = 0;
      num_empty_buffers = per_disk_buffers;
    }
  };

  // Per disk states to synchronize multiple disk threads accessing the same reader.
  vector<PerDiskState> disk_states_;

  ReaderContext(int num_disks) 
    : bytes_read_counter_(NULL),
      read_timer_(NULL),
      state_(Inactive),
      disk_states_(num_disks) {
  }

  // Resets this object for a new reader
  void Reset(hdfsFS hdfs_connection, int per_disk_buffers) {
    DCHECK_EQ(state_, Inactive);

    bytes_read_counter_ = NULL;
    read_timer_ = NULL;

    state_ = Active;
    num_buffers_per_disk_ = per_disk_buffers;
    hdfs_connection_ = hdfs_connection;
    num_remaining_scan_ranges_ = 0;
    num_disks_with_ranges_ = 0;

    for (int i = 0; i < disk_states_.size(); ++i) {
      disk_states_[i].Reset(per_disk_buffers);
    }

    num_empty_buffers_ = per_disk_buffers * disk_states_.size();
    num_outstanding_buffers_ = 0;
    sync_reader_ = false;
  }

  // Validates invariants of reader.  Reader lock should be taken before hand.
  bool Validate() {
    if (state_ == ReaderContext::Inactive) return false;

    int num_disks_with_ranges = 0;
    int num_empty_buffers = 0;
    int num_reading_threads = 0;

    if (num_remaining_scan_ranges_ == 0 && !ready_buffers_.empty()) return false;
    
    for (int i = 0; i < disk_states_.size(); ++i) {
      PerDiskState& state = disk_states_[i];
      num_empty_buffers += state.num_empty_buffers;
      num_reading_threads += state.num_threads_in_read;

      if (state.num_scan_ranges > 0) ++num_disks_with_ranges;
      if (state.num_threads_in_read < 0) return false;
      if (state.num_empty_buffers < 0) return false;
      if (state.num_empty_buffers > num_buffers_per_disk_) return false;
      
      // If there are no more ranges, no threads should be reading
      if (num_disks_with_ranges_ == 0 && state.num_threads_in_read != 0) return false;
    }
    int num_ok_ready_buffers = 0;

    for (list<BufferDescriptor*>::iterator it = ready_buffers_.begin();
        it != ready_buffers_.end(); ++it) {
      if ((*it)->buffer_ != NULL) ++num_ok_ready_buffers;
    }
      
    // Buffers either need to be unused (empty) or read (num_ok_ready_buffers) or
    // being read (num_reading_threads) or owned by the reader (unaccounted)
    // TODO: keep track of that as well
    // That should never be more than the allocated empty buffers for the reader
    if (num_empty_buffers_ + num_ok_ready_buffers + num_reading_threads > 
          num_buffers_per_disk_ * disk_states_.size()) {
      return false;
    }
    
    if (num_empty_buffers != num_empty_buffers_) return false;
    if (num_disks_with_ranges_ == 0 && num_reading_threads > 0) return false;
    if (num_disks_with_ranges != num_disks_with_ranges_) return false;

    return true;
  }

  // Dumps out reader information.  Lock should be taken by caller
  string DebugString() const {
    stringstream ss;
    ss << "  Reader: " << (void*)this << " (state=";
    if (state_ == ReaderContext::Inactive) ss << "Inactive";
    if (state_ == ReaderContext::Cancelled) ss << "Cancelled";
    if (state_ == ReaderContext::Active) ss << "Active";
    if (state_ != ReaderContext::Inactive) {
      ss << " #buffers_per_disk=" << num_buffers_per_disk_;
      ss << " #ready_buffers=" << ready_buffers_.size();
      ss << " #scan_ranges=" << num_remaining_scan_ranges_;
      ss << " #empty_buffers=" << num_empty_buffers_;
      ss << " #outstanding_buffers=" << num_outstanding_buffers_;
      ss << " #remaining_scan_ranges=" << num_remaining_scan_ranges_;
      ss << " #disk_with_ranges=" << num_disks_with_ranges_;
      ss << " #disks=" << num_disks_with_ranges_;
      for (int i = 0; i < disk_states_.size(); ++i) {
        ss << endl << "    " << i << ": ";
        ss << "is_on_queue=" << disk_states_[i].is_on_queue
            << " #scan_ranges=" << disk_states_[i].num_scan_ranges
            << " #reading_threads=" << disk_states_[i].num_threads_in_read
            << " #empty_buffers=" << disk_states_[i].num_empty_buffers;
      }
    }
    ss << ")";
    return ss.str();
  }
};

// This class provides a cache of ReaderContext objects.  ReaderContexts are recycled.
// This is good for locality as well as lock contention.  The cache has the property that 
// regardless of how many clients get added/removed, the memory locations for 
// existing clients do not change (not the case with std::vector) minimizing the locks we
// have to take across all readers.
// All functions on this object are thread safe
class DiskIoMgr::ReaderCache {
 public:
  ReaderCache(DiskIoMgr* io_mgr) : io_mgr_(io_mgr) {}
  
  // Returns reader to the cache.  This reader object can now be reused.
  void ReturnReader(ReaderContext* reader) {
    DCHECK(reader->state_ != ReaderContext::Inactive);
    reader->state_ = ReaderContext::Inactive;
    lock_guard<mutex> l(lock_);
    inactive_readers_.push_back(reader);
  }

  // Returns a new reader object.  Allocates a new reader context if necessary.
  ReaderContext* GetNewReader() {
    lock_guard<mutex> l(lock_);
    if (!inactive_readers_.empty()) {
      ReaderContext* reader = inactive_readers_.front();
      inactive_readers_.pop_front();
      return reader;
    } else {
      ReaderContext* reader = new ReaderContext(io_mgr_->num_disks());
      all_readers_.push_back(reader);
      return reader;
    }
  }
  
  // This object has the same lifetime as the diskiomgr.
  ~ReaderCache() {
    for (list<ReaderContext*>::iterator it = all_readers_.begin();
        it != all_readers_.end(); ++it) {
=======
DEFINE_int32(num_threads_per_disk, 0, "number of threads per disk");
DEFINE_int32(read_size, 8 * 1024 * 1024, "Read Size (in bytes)");
DEFINE_int32(min_buffer_size, 1024, "The minimum read buffer size (in bytes)");

// With 1024B through 8MB buffers, this is up to ~2GB of buffers.
DEFINE_int32(max_free_io_buffers, 128,
    "For each io buffer size, the maximum number of buffers the IoMgr will hold onto");

// Rotational disks should have 1 thread per disk to minimize seeks.  Non-rotational
// don't have this penalty and benefit from multiple concurrent IO requests.
static const int THREADS_PER_ROTATIONAL_DISK = 1;
static const int THREADS_PER_FLASH_DISK = 8;

// The IoMgr is able to run with a wide range of memory usage. If a query has memory
// remaining less than this value, the IoMgr will stop all buffering regardless of the
// current queue size.
static const int LOW_MEMORY = 64 * 1024 * 1024;

const int DiskIoMgr::DEFAULT_QUEUE_CAPACITY = 2;

// This class provides a cache of RequestContext objects.  RequestContexts are recycled.
// This is good for locality as well as lock contention.  The cache has the property that
// regardless of how many clients get added/removed, the memory locations for
// existing clients do not change (not the case with std::vector) minimizing the locks we
// have to take across all readers.
// All functions on this object are thread safe
class DiskIoMgr::RequestContextCache {
 public:
  RequestContextCache(DiskIoMgr* io_mgr) : io_mgr_(io_mgr) {}

  // Returns a context to the cache.  This object can now be reused.
  void ReturnContext(RequestContext* reader) {
    DCHECK(reader->state_ != RequestContext::Inactive);
    reader->state_ = RequestContext::Inactive;
    lock_guard<mutex> l(lock_);
    inactive_contexts_.push_back(reader);
  }

  // Returns a new RequestContext object.  Allocates a new object if necessary.
  RequestContext* GetNewContext() {
    lock_guard<mutex> l(lock_);
    if (!inactive_contexts_.empty()) {
      RequestContext* reader = inactive_contexts_.front();
      inactive_contexts_.pop_front();
      return reader;
    } else {
      RequestContext* reader = new RequestContext(io_mgr_, io_mgr_->num_disks());
      all_contexts_.push_back(reader);
      return reader;
    }
  }

  // This object has the same lifetime as the disk IoMgr.
  ~RequestContextCache() {
    for (list<RequestContext*>::iterator it = all_contexts_.begin();
        it != all_contexts_.end(); ++it) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      delete *it;
    }
  }

  // Validates that all readers are cleaned up and in the inactive state.  No locks
<<<<<<< HEAD
  // are taken since this is only called from the disk io mgr destructor.
  bool ValidateAllInactive() {
    for (list<ReaderContext*>::iterator it = all_readers_.begin();
        it != all_readers_.end(); ++it) {
      if ((*it)->state_ != ReaderContext::Inactive) {
        return false;
      } 
    }
    DCHECK_EQ(all_readers_.size(), inactive_readers_.size());
    return all_readers_.size() == inactive_readers_.size();
  }

  string DebugString() {
    lock_guard<mutex> l(lock_);
    stringstream ss;
    for (list<ReaderContext*>::iterator it = all_readers_.begin();
        it != all_readers_.end(); ++it) {
      unique_lock<mutex> lock((*it)->lock_);
      ss << (*it)->DebugString() << endl;
    }
    return ss.str();
  }
=======
  // are taken since this is only called from the disk IoMgr destructor.
  bool ValidateAllInactive() {
    for (list<RequestContext*>::iterator it = all_contexts_.begin();
        it != all_contexts_.end(); ++it) {
      if ((*it)->state_ != RequestContext::Inactive) {
        return false;
      }
    }
    DCHECK_EQ(all_contexts_.size(), inactive_contexts_.size());
    return all_contexts_.size() == inactive_contexts_.size();
  }

  string DebugString();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

 private:
  DiskIoMgr* io_mgr_;

  // lock to protect all members below
  mutex lock_;

<<<<<<< HEAD
  // List of all reader created.  Used for debugging
  list<ReaderContext*> all_readers_;

  // List of inactive readers.  These objects can be used for a new reader.
  list<ReaderContext*> inactive_readers_;
};

// Per disk state
struct DiskIoMgr::DiskQueue {
  // Disk id (0-based)
  int disk_id;

  // Condition variable to signal the disk threads that there is work to do or the
  // thread should shut down.  A disk thread will be woken up when there is a reader
  // added to the queue.  Readers are only on the queue when they have both work
  // (scan range added) and an available buffer.
  condition_variable work_available;
  
  // Lock that protects access to 'readers'
  mutex lock;

  // list of all readers that have work queued on this disk and empty buffers to read
  // into.
  list<ReaderContext*> readers;

  // The reader at the head of the queue that will get their io issued next
  list<ReaderContext*>::iterator next_reader;

  DiskQueue(int id) : disk_id(id) {
    next_reader = readers.end();
  }
};

DiskIoMgr::ScanRange::ScanRange() {
  Reset(NULL, -1, -1, -1);
}

void DiskIoMgr::ScanRange::Reset(const char* file, int64_t len, int64_t offset, 
    int disk_id, void* meta_data) {
  file_ = file;
  len_ = len;
  offset_ = offset;
  disk_id_ = disk_id;
  meta_data_ = meta_data;
}
    
void DiskIoMgr::ScanRange::InitInternal(ReaderContext* reader) {
  reader_ = reader;
  local_file_ = NULL;
  hdfs_file_ = NULL;
  bytes_read_ = 0;
}

string DiskIoMgr::ScanRange::DebugString() const{
  stringstream ss;
  ss << "file=" << file_ << " disk_id=" << disk_id_ << " offset=" << offset_
     << " len=" << len_ << " bytes_read=" << bytes_read_;
=======
  // List of all request contexts created.  Used for debugging
  list<RequestContext*> all_contexts_;

  // List of inactive readers.  These objects can be used for a new reader.
  list<RequestContext*> inactive_contexts_;
};

string DiskIoMgr::RequestContextCache::DebugString() {
  lock_guard<mutex> l(lock_);
  stringstream ss;
  for (list<RequestContext*>::iterator it = all_contexts_.begin();
      it != all_contexts_.end(); ++it) {
    unique_lock<mutex> lock((*it)->lock_);
    ss << (*it)->DebugString() << endl;
  }
  return ss.str();
}

string DiskIoMgr::DebugString() {
  stringstream ss;
  ss << "RequestContexts: " << endl << request_context_cache_->DebugString() << endl;

  ss << "Disks: " << endl;
  for (int i = 0; i < disk_queues_.size(); ++i) {
    unique_lock<mutex> lock(disk_queues_[i]->lock);
    ss << "  " << (void*) disk_queues_[i] << ":" ;
    if (!disk_queues_[i]->request_contexts.empty()) {
      ss << " Readers: ";
      BOOST_FOREACH(RequestContext* req_context, disk_queues_[i]->request_contexts) {
        ss << (void*)req_context;
      }
    }
    ss << endl;
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return ss.str();
}

DiskIoMgr::BufferDescriptor::BufferDescriptor(DiskIoMgr* io_mgr) :
  io_mgr_(io_mgr), reader_(NULL), buffer_(NULL) {
}

<<<<<<< HEAD
void DiskIoMgr::BufferDescriptor::Reset(ReaderContext* reader, 
      ScanRange* range, char* buffer) {
  DCHECK(io_mgr_ != NULL);
  DCHECK(buffer_ == NULL);
  reader_ = reader;
  scan_range_ = range;
  buffer_ = buffer;
  len_ = 0;
  eosr_ = false;
  status_ = Status::OK;
=======
void DiskIoMgr::BufferDescriptor::Reset(RequestContext* reader,
      ScanRange* range, char* buffer, int64_t buffer_len) {
  DCHECK(io_mgr_ != NULL);
  DCHECK(buffer_ == NULL);
  DCHECK(range != NULL);
  DCHECK(buffer != NULL);
  DCHECK_GE(buffer_len, 0);
  reader_ = reader;
  scan_range_ = range;
  buffer_ = buffer;
  buffer_len_ = buffer_len;
  len_ = 0;
  eosr_ = false;
  status_ = Status::OK;
  mem_tracker_ = NULL;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

void DiskIoMgr::BufferDescriptor::Return() {
  DCHECK(io_mgr_ != NULL);
  io_mgr_->ReturnBuffer(this);
}

<<<<<<< HEAD
DiskIoMgr::DiskIoMgr() :
    num_threads_per_disk_(FLAGS_num_threads_per_disk),
    max_read_size_(FLAGS_read_size),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::CPU_TICKS),
    num_allocated_buffers_(0) {
  int num_disks = FLAGS_num_disks;
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
}

DiskIoMgr::DiskIoMgr(int num_disks, int threads_per_disk, int max_read_size) :
    num_threads_per_disk_(threads_per_disk),
    max_read_size_(max_read_size),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::CPU_TICKS),
    num_allocated_buffers_(0) {
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
=======
void DiskIoMgr::BufferDescriptor::SetMemTracker(MemTracker* tracker) {
  // Cached buffers don't count towards mem usage.
  if (scan_range_->cached_buffer_ != NULL) return;
  if (mem_tracker_ == tracker) return;
  if (mem_tracker_ != NULL) mem_tracker_->Release(buffer_len_);
  mem_tracker_ = tracker;
  if (mem_tracker_ != NULL) mem_tracker_->Consume(buffer_len_);
}

DiskIoMgr::WriteRange::WriteRange(const string& file, int64_t file_offset, int disk_id,
    WriteDoneCallback callback) {
  file_ = file;
  offset_ = file_offset;
  disk_id_ = disk_id;
  callback_ = callback;
  request_type_ = RequestType::WRITE;
}

void DiskIoMgr::WriteRange::SetData(const uint8_t* buffer, int64_t len) {
  data_ = buffer;
  len_ = len;
}

static void CheckSseSupport() {
  if (!CpuInfo::IsSupported(CpuInfo::SSE4_2)) {
    LOG(WARNING) << "This machine does not support sse4_2.  The default IO system "
                    "configurations are suboptimal for this hardware.  Consider "
                    "increasing the number of threads per disk by restarting impalad "
                    "using the --num_threads_per_disk flag with a higher value";
  }
}

DiskIoMgr::DiskIoMgr() :
    num_threads_per_disk_(FLAGS_num_threads_per_disk),
    max_buffer_size_(FLAGS_read_size),
    min_buffer_size_(FLAGS_min_buffer_size),
    cached_read_options_(NULL),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::TIME_NS) {
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2(max_buffer_size_scaled) + 1);
  int num_disks = FLAGS_num_disks;
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
  CheckSseSupport();
}

DiskIoMgr::DiskIoMgr(int num_disks, int threads_per_disk, int min_buffer_size,
                     int max_buffer_size) :
    num_threads_per_disk_(threads_per_disk),
    max_buffer_size_(max_buffer_size),
    min_buffer_size_(min_buffer_size),
    cached_read_options_(NULL),
    shut_down_(false),
    total_bytes_read_counter_(TCounterType::BYTES),
    read_timer_(TCounterType::TIME_NS) {
  int64_t max_buffer_size_scaled = BitUtil::Ceil(max_buffer_size_, min_buffer_size_);
  free_buffers_.resize(BitUtil::Log2(max_buffer_size_scaled) + 1);
  if (num_disks == 0) num_disks = DiskInfo::num_disks();
  disk_queues_.resize(num_disks);
  CheckSseSupport();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

DiskIoMgr::~DiskIoMgr() {
  shut_down_ = true;
<<<<<<< HEAD
  // Notify all worker threads and shut them down. 
  for (int i = 0; i < disk_queues_.size(); ++i) {
=======
  // Notify all worker threads and shut them down.
  for (int i = 0; i < disk_queues_.size(); ++i) {
    if (disk_queues_[i] == NULL) continue;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    {
      // This lock is necessary to properly use the condition var to notify
      // the disk worker threads.  The readers also grab this lock so updates
      // to shut_down_ are protected.
<<<<<<< HEAD
      unique_lock<mutex> lock(disk_queues_[i]->lock);
    }
    disk_queues_[i]->work_available.notify_all();
  }
  disk_thread_group_.join_all();

  for (int i = 0; i < disk_queues_.size(); ++i) {
    int disk_id = disk_queues_[i]->disk_id;
    for (list<ReaderContext*>::iterator reader = disk_queues_[i]->readers.begin(); 
      reader != disk_queues_[i]->readers.end(); ++reader) { 
      DCHECK_EQ((*reader)->disk_states_[disk_id].num_threads_in_read, 0);
      DecrementDiskRefCount(*reader);
    }
  } 

  DCHECK(reader_cache_->ValidateAllInactive()) << endl << DebugString();
  
  // Delete all allocated buffers
  DCHECK_EQ(num_allocated_buffers_, free_buffers_.size());
  for (list<char*>::iterator iter = free_buffers_.begin();
      iter != free_buffers_.end(); ++iter) {
    delete *iter;
  }
=======
      unique_lock<mutex> disk_lock(disk_queues_[i]->lock);
    }
    disk_queues_[i]->work_available.notify_all();
  }
  disk_thread_group_.JoinAll();

  for (int i = 0; i < disk_queues_.size(); ++i) {
    if (disk_queues_[i] == NULL) continue;
    int disk_id = disk_queues_[i]->disk_id;
    for (list<RequestContext*>::iterator it = disk_queues_[i]->request_contexts.begin();
        it != disk_queues_[i]->request_contexts.end(); ++it) {
      DCHECK_EQ((*it)->disk_states_[disk_id].num_threads_in_op(), 0);
      DCHECK((*it)->disk_states_[disk_id].done());
      (*it)->DecrementDiskRefCount();
    }
  }

  DCHECK(request_context_cache_.get() == NULL ||
      request_context_cache_->ValidateAllInactive())
      << endl << DebugString();
  DCHECK_EQ(num_buffers_in_readers_, 0);

  // Delete all allocated buffers
  int num_free_buffers = 0;
  for (int idx = 0; idx < free_buffers_.size(); ++idx) {
    num_free_buffers += free_buffers_[idx].size();
  }
  DCHECK_EQ(num_allocated_buffers_, num_free_buffers);
  GcIoBuffers();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  for (int i = 0; i < disk_queues_.size(); ++i) {
    delete disk_queues_[i];
  }
}

<<<<<<< HEAD
Status DiskIoMgr::Init() {
  for (int i = 0; i < disk_queues_.size(); ++i) {
    disk_queues_[i] = new DiskQueue(i);
    for (int j = 0; j < num_threads_per_disk_; ++j) {
      disk_thread_group_.add_thread(
          new thread(&DiskIoMgr::ReadLoop, this, disk_queues_[i]));
    }
  }
  reader_cache_.reset(new ReaderCache(this));
  return Status::OK;
}

Status DiskIoMgr::RegisterReader(hdfsFS hdfs, int num_buffers_per_disk, 
    ReaderContext** reader) {
  DCHECK(reader_cache_.get() != NULL) << "Must call Init() first.";
  *reader = reader_cache_->GetNewReader();
  (*reader)->Reset(hdfs, num_buffers_per_disk);
  return Status::OK;
}

void DiskIoMgr::UnregisterReader(ReaderContext* reader) {
  // First cancel the reader.  This is more or less a no-op if the reader is
  // complete (common case).
  CancelReader(reader);
  
  unique_lock<mutex> lock(reader->lock_);
  DCHECK_EQ(reader->num_outstanding_buffers_, 0);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK(reader->ready_buffers_.empty());
  while (reader->num_disks_with_ranges_ > 0) {
    reader->disks_complete_cond_var_.wait(lock);
  }
  if (!reader->sync_reader_) {
    DCHECK_EQ(reader->num_empty_buffers_, 
        reader->num_buffers_per_disk_ * disk_queues_.size());
  } 
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  reader_cache_->ReturnReader(reader);
}

// Cancellation requires coordination from multiple threads.  Each thread that currently
// has a reference to the reader must notice the cancel and remove it from its tracking
// structures.  The last thread to touch the reader should deallocate (aka recycle) the
// reader context object.  Potential threads are:
//  1. Disk threads that are currently reading for this reader.  
//  2. Caller thread that is waiting in GetNext. 
//
// The steps are:
// 1. Cancel will immediately set the reader in the Cancelled state.  This prevents any 
// other thread from adding more ready buffers to this reader (they all take a lock and 
// check the state before doing so).
// 2. Return all ready buffers that are not returned from GetNext.  This also triggers 
// waking up the disks that have work queued for this reader.
// 3. Wake up all threads waiting in GetNext().  They will notice the cancelled state and
// return cancelled statuses.
// 4. Disk threads notice the reader is cancelled either when picking the next reader 
// to read for or when they try to enqueue a ready buffer.  Upon noticing the cancelled 
// state, removes the reader from the disk queue.  The last thread per disk with an 
// outstanding reference to the reader decrements the number of disk queues the reader 
// is on.  
void DiskIoMgr::CancelReader(ReaderContext* reader) {
  // copy of ready but unreturned buffers that need to be cleaned up.
  list<BufferDescriptor*> ready_buffers_copy;
  {
    unique_lock<mutex> lock(reader->lock_);
    DCHECK(reader->Validate()) << endl << reader->DebugString();

    // Already being cancelled
    if (reader->state_ == ReaderContext::Cancelled) {
      return;
    }
    
    // The reader will be put into a cancelled state until call cleanup is complete.
    reader->state_ = ReaderContext::Cancelled;
    
    ready_buffers_copy.swap(reader->ready_buffers_);
    reader->num_outstanding_buffers_ += ready_buffers_copy.size();
  }
  
  // Signal reader and unblock the GetNext/Read thread.  That read will fail with
  // a cancelled status.
  reader->buffer_ready_cond_var_.notify_all();

  // Clean up already read blocks.  We can safely work on a local copy of the ready
  // buffer.  Before releasing the reader lock, we set the reader state to Cancelled.  
  // All other threads first check the reader state before modifying ready_buffers_.
  for (list<BufferDescriptor*>::iterator it = ready_buffers_copy.begin();
       it != ready_buffers_copy.end(); ++it) {
    ReturnBuffer(*it);
  }
}

int DiskIoMgr::num_empty_buffers(ReaderContext* reader) {
  unique_lock<mutex> (reader->lock_);
  return reader->num_empty_buffers_;
}

void DiskIoMgr::set_read_timer(ReaderContext* r, RuntimeProfile::Counter* c) {
  r->read_timer_ = c;
}

void DiskIoMgr::set_bytes_read_counter(ReaderContext* r, RuntimeProfile::Counter* c) {
  r->bytes_read_counter_ = c;
}

=======
Status DiskIoMgr::Init(MemTracker* process_mem_tracker) {
  DCHECK(process_mem_tracker != NULL);
  process_mem_tracker_ = process_mem_tracker;
  // If we hit the process limit, see if we can reclaim some memory by removing
  // previously allocated (but unused) io buffers.
  process_mem_tracker->AddGcFunction(bind(&DiskIoMgr::GcIoBuffers, this));

  for (int i = 0; i < disk_queues_.size(); ++i) {
    disk_queues_[i] = new DiskQueue(i);
    int num_threads_per_disk = num_threads_per_disk_;
    if (num_threads_per_disk == 0) {
      if (DiskInfo::is_rotational(i)) {
        num_threads_per_disk = THREADS_PER_ROTATIONAL_DISK;
      } else {
        num_threads_per_disk = THREADS_PER_FLASH_DISK;
      }
    }
    for (int j = 0; j < num_threads_per_disk; ++j) {
      stringstream ss;
      ss << "work-loop(Disk: " << i << ", Thread: " << j << ")";
      disk_thread_group_.AddThread(new Thread("disk-io-mgr", ss.str(),
          &DiskIoMgr::WorkLoop, this, disk_queues_[i]));
    }
  }
  request_context_cache_.reset(new RequestContextCache(this));
  return Status::OK;
}

Status DiskIoMgr::RegisterContext(hdfsFS hdfs, RequestContext** request_context,
    MemTracker* mem_tracker) {
  DCHECK(request_context_cache_.get() != NULL) << "Must call Init() first.";
  *request_context = request_context_cache_->GetNewContext();
  (*request_context)->Reset(hdfs, mem_tracker);
  return Status::OK;
}

void DiskIoMgr::UnregisterContext(RequestContext* reader) {
  CancelContext(reader, true);

  // All the disks are done with clean, validate nothing is leaking.
  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK_EQ(reader->num_buffers_in_reader_, 0) << endl << reader->DebugString();
  DCHECK_EQ(reader->num_used_buffers_, 0) << endl << reader->DebugString();

  DCHECK(reader->Validate()) << endl << reader->DebugString();
  request_context_cache_->ReturnContext(reader);
}

// Cancellation requires coordination from multiple threads.  Each thread that currently
// has a reference to the request context must notice the cancel and remove it from its
// tracking structures.  The last thread to touch the context should deallocate (aka
// recycle) the request context object.  Potential threads are:
//  1. Disk threads that are currently reading for this reader.
//  2. Caller threads that are waiting in GetNext.
//
// The steps are:
// 1. Cancel will immediately set the context in the Cancelled state.  This prevents any
// other thread from adding more ready buffers to the context (they all take a lock and
// check the state before doing so), or any write ranges to the context.
// 2. Cancel will call cancel on each ScanRange that is not yet complete, unblocking
// any threads in GetNext(). The reader will see the cancelled Status returned. Cancel
// also invokes the callback for the WriteRanges with the cancelled state.
// 3. Disk threads notice the context is cancelled either when picking the next context
// to process or when they try to enqueue a ready buffer.  Upon noticing the cancelled
// state, removes the context from the disk queue.  The last thread per disk with an
// outstanding reference to the context decrements the number of disk queues the context
// is on.
// If wait_for_disks_completion is true, wait for the number of active disks to become 0.
void DiskIoMgr::CancelContext(RequestContext* context, bool wait_for_disks_completion) {
  context->Cancel(Status::CANCELLED);

  if (wait_for_disks_completion) {
    unique_lock<mutex> lock(context->lock_);
    DCHECK(context->Validate()) << endl << context->DebugString();
    while (context->num_disks_with_ranges_ > 0) {
      context->disks_complete_cond_var_.wait(lock);
    }
  }
}

void DiskIoMgr::set_read_timer(RequestContext* r, RuntimeProfile::Counter* c) {
  r->read_timer_ = c;
}

void DiskIoMgr::set_bytes_read_counter(RequestContext* r, RuntimeProfile::Counter* c) {
  r->bytes_read_counter_ = c;
}

void DiskIoMgr::set_active_read_thread_counter(RequestContext* r,
    RuntimeProfile::Counter* c) {
  r->active_read_thread_counter_ = c;
}

void DiskIoMgr::set_disks_access_bitmap(RequestContext* r,
    RuntimeProfile::Counter* c) {
  r->disks_accessed_bitmap_ = c;
}

int64_t DiskIoMgr::queue_size(RequestContext* reader) const {
  return reader->num_ready_buffers_;
}

Status DiskIoMgr::context_status(RequestContext* context) const {
  unique_lock<mutex> lock(context->lock_);
  return context->status_;
}

int DiskIoMgr::num_unstarted_ranges(RequestContext* reader) const {
  return reader->num_unstarted_scan_ranges_;
}

int64_t DiskIoMgr::bytes_read_local(RequestContext* reader) const {
  return reader->bytes_read_local_;
}

int64_t DiskIoMgr::bytes_read_short_circuit(RequestContext* reader) const {
  return reader->bytes_read_short_circuit_;
}

int64_t DiskIoMgr::bytes_read_dn_cache(RequestContext* reader) const {
  return reader->bytes_read_dn_cache_;
}

int DiskIoMgr::num_remote_ranges(RequestContext* reader) const {
  return reader->num_remote_ranges_;
}

int64_t DiskIoMgr::unexpected_remote_bytes(RequestContext* reader) const {
  return reader->unexpected_remote_bytes_;
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
int64_t DiskIoMgr::GetReadThroughput() {
  return RuntimeProfile::UnitsPerSecond(&total_bytes_read_counter_, &read_timer_);
}

<<<<<<< HEAD
Status DiskIoMgr::AddScanRanges(ReaderContext* reader, const vector<ScanRange*>& ranges) {
  DCHECK(!ranges.empty());

  // Validate all ranges before adding any
  for (int i = 0; i < ranges.size(); ++i) {
    int disk_id = ranges[i]->disk_id_;
    if (disk_id < 0 || disk_id >= disk_queues_.size()) {
      stringstream ss;
      ss << "Invalid scan range.  Bad disk id: " << disk_id;
      DCHECK(false) << ss.str();
      return Status(ss.str());
    }
  }

  // disks that this reader needs to be scheduled on.
  vector<DiskQueue*> disks_to_add;

  {
    unique_lock<mutex> lock(reader->lock_);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    
    if (reader->state_ == ReaderContext::Cancelled) return Status::CANCELLED;

    // Add each range to the queue of the disk the range is on
    for (int i = 0; i < ranges.size(); ++i) {
      // Don't add empty ranges.
      DCHECK_NE(ranges[i]->len(), 0);
      ScanRange* range = ranges[i];
      range->InitInternal(reader);
      if (reader->disk_states_[range->disk_id_].num_scan_ranges == 0) {
        ++reader->num_disks_with_ranges_;
        DCHECK(!reader->disk_states_[range->disk_id_].is_on_queue);
        if (reader->disk_states_[range->disk_id_].num_empty_buffers > 0) {
          reader->disk_states_[range->disk_id_].is_on_queue = true;
          disks_to_add.push_back(disk_queues_[range->disk_id_]);
        }
      }
      reader->disk_states_[range->disk_id_].ranges.push_back(range);
      ++reader->disk_states_[range->disk_id_].num_scan_ranges;
    }
    reader->num_remaining_scan_ranges_ += ranges.size();
    DCHECK(reader->Validate()) << endl << reader->DebugString();
  }

  // This requires taking a disk lock, reader lock must not be taken
  for (int i = 0; i < disks_to_add.size(); ++i) {
    {
      unique_lock<mutex> lock(disks_to_add[i]->lock);
      disks_to_add[i]->readers.push_back(reader);
    }
    disks_to_add[i]->work_available.notify_all();
  }
=======
Status DiskIoMgr::ValidateScanRange(ScanRange* range) {
  int disk_id = range->disk_id_;
  if (disk_id < 0 || disk_id >= disk_queues_.size()) {
    stringstream ss;
    ss << "Invalid scan range.  Bad disk id: " << disk_id;
    DCHECK(false) << ss.str();
    return Status(ss.str());
  }
  return Status::OK;
}

Status DiskIoMgr::AddScanRanges(RequestContext* reader,
    const vector<ScanRange*>& ranges, bool schedule_immediately) {
  if (ranges.empty()) return Status::OK;

  // Validate and initialize all ranges
  for (int i = 0; i < ranges.size(); ++i) {
    RETURN_IF_ERROR(ValidateScanRange(ranges[i]));
    ranges[i]->InitInternal(this, reader);
  }

  // disks that this reader needs to be scheduled on.
  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  if (reader->state_ == RequestContext::Cancelled) {
    DCHECK(!reader->status_.ok());
    return reader->status_;
  }

  // Add each range to the queue of the disk the range is on
  for (int i = 0; i < ranges.size(); ++i) {
    // Don't add empty ranges.
    DCHECK_NE(ranges[i]->len(), 0);
    ScanRange* range = ranges[i];

    if (range->try_cache_) {
      if (schedule_immediately) {
        bool cached_read_succeeded;
        RETURN_IF_ERROR(range->ReadFromCache(&cached_read_succeeded));
        if (cached_read_succeeded) continue;
        // Cached read failed, fall back to AddRequestRange() below.
      } else {
        reader->cached_ranges_.Enqueue(range);
        continue;
      }
    }
    reader->AddRequestRange(range, schedule_immediately);
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  return Status::OK;
}

<<<<<<< HEAD
Status DiskIoMgr::Read(hdfsFS hdfs, ScanRange* range, BufferDescriptor** buffer) {
  ReaderContext* local_context = NULL;
  // Make a local context for doing the synchronous read
  // Local reader always has one buffer.  Since this is synchronous, it can't use
  // the same resource pool as the parent reader.
  // TODO: more careful resource management?  We're okay as long as one 
  // sync reader is able to get through (otherwise we can deadlock).
  RETURN_IF_ERROR(RegisterReader(hdfs, 1, &local_context));
  local_context->sync_reader_ = true;

  vector<ScanRange*> ranges;
  ranges.push_back(range);
  Status status = AddScanRanges(local_context, ranges);
  if (status.ok()) {
    bool eos;
    status = GetNext(local_context, buffer, &eos);
    DCHECK(eos);
  }

  // Reassign the buffer's reader to the external context and clean up the temp context
  if (*buffer != NULL) (*buffer)->reader_ = NULL;
  
  // The local context doesn't track its resources the same way.
  local_context->num_outstanding_buffers_ = 0;

  UnregisterReader(local_context);
  return status;
}

Status DiskIoMgr::GetNext(ReaderContext* reader, BufferDescriptor** buffer, bool* eos) {
  unique_lock<mutex> lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  
  *buffer = NULL;
  *eos = true;

  if (reader->state_ == ReaderContext::Cancelled) return Status::CANCELLED;

  VLOG_FILE << "GetNext(): reader=" << reader->DebugString();

  // Wait until a block is read, all blocks have been read and returned or 
  // reader is cancelled
  while (reader->ready_buffers_.empty() && reader->state_ == ReaderContext::Active) {
    reader->buffer_ready_cond_var_.wait(lock);
  }
  
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  if (reader->state_ == ReaderContext::Cancelled) return Status::CANCELLED;

  // Remove the first read block from the queue and return it
  DCHECK(!reader->ready_buffers_.empty());
  *buffer = reader->ready_buffers_.front();
  reader->ready_buffers_.pop_front();

  RETURN_IF_ERROR((*buffer)->status_);

  *eos = false;
  
  if ((*buffer)->eosr_) {
    --reader->num_remaining_scan_ranges_;
    DCHECK_GE(reader->num_remaining_scan_ranges_, 0);
    if (reader->num_remaining_scan_ranges_ == 0) {
      *eos = true;
      // All scan ranges complete, notify all other threads on this reader currently
      // in GetNext()  
      // TODO: is it clearer if the disk thread does this?
      reader->buffer_ready_cond_var_.notify_all();
    }
  }

  ++reader->num_outstanding_buffers_;
  DCHECK((*buffer)->buffer_ != NULL);
=======
// This function returns the next scan range the reader should work on, checking
// for eos and error cases. If there isn't already a cached scan range or a scan
// range prepared by the disk threads, the caller waits on the disk threads.
Status DiskIoMgr::GetNextRange(RequestContext* reader, ScanRange** range) {
  DCHECK(reader != NULL);
  DCHECK(range != NULL);
  *range = NULL;

  Status status;

  unique_lock<mutex> reader_lock(reader->lock_);
  DCHECK(reader->Validate()) << endl << reader->DebugString();

  while (true) {
    if (reader->state_ == RequestContext::Cancelled) {
      DCHECK(!reader->status_.ok());
      status = reader->status_;
      break;
    }

    if (reader->num_unstarted_scan_ranges_ == 0 &&
        reader->ready_to_start_ranges_.empty() && reader->cached_ranges_.empty()) {
      // All ranges are done, just return.
      break;
    }

    if (!reader->cached_ranges_.empty()) {
      // We have a cached range.
      *range = reader->cached_ranges_.Dequeue();
      DCHECK((*range)->try_cache_);
      bool cached_read_succeeded;
      RETURN_IF_ERROR((*range)->ReadFromCache(&cached_read_succeeded));
      if (cached_read_succeeded) return Status::OK;

      // This range ended up not being cached. Loop again and pick up a new range.
      reader->AddRequestRange(*range, false);
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      *range = NULL;
      continue;
    }

    if (reader->ready_to_start_ranges_.empty()) {
      reader->ready_to_start_ranges_cv_.wait(reader_lock);
    } else {
      *range = reader->ready_to_start_ranges_.Dequeue();
      DCHECK(*range != NULL);
      int disk_id = (*range)->disk_id();
      DCHECK_EQ(*range, reader->disk_states_[disk_id].next_scan_range_to_start());
      // Set this to NULL, the next time this disk runs for this reader, it will
      // get another range ready.
      reader->disk_states_[disk_id].set_next_scan_range_to_start(NULL);
      reader->ScheduleScanRange(*range);
      break;
    }
  }

  return status;
}

Status DiskIoMgr::Read(RequestContext* reader,
    ScanRange* range, BufferDescriptor** buffer) {
  DCHECK(range != NULL);
  DCHECK(buffer != NULL);
  *buffer = NULL;

  if (range->len() > max_buffer_size_) {
    stringstream ss;
    ss << "Cannot perform sync read larger than " << max_buffer_size_
       << ". Request was " << range->len();
    return Status(ss.str());
  }

  vector<DiskIoMgr::ScanRange*> ranges;
  ranges.push_back(range);
  RETURN_IF_ERROR(AddScanRanges(reader, ranges, true));
  RETURN_IF_ERROR(range->GetNext(buffer));
  DCHECK((*buffer) != NULL);
  DCHECK((*buffer)->eosr());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  return Status::OK;
}

void DiskIoMgr::ReturnBuffer(BufferDescriptor* buffer_desc) {
  DCHECK(buffer_desc != NULL);
<<<<<<< HEAD
  if (!buffer_desc->status_.ok()) {
    DCHECK(buffer_desc->buffer_ == NULL);
  }

  // Null buffer meant there was an error or GetNext was called after eos.  Protect
  // against returning those buffers.
  if (buffer_desc->buffer_ != NULL) {
    ReturnFreeBuffer(buffer_desc->buffer_);
    buffer_desc->buffer_ = NULL;

    ReaderContext* reader = buffer_desc->reader_;
    if (reader != NULL) {
      DCHECK(buffer_desc->reader_ != NULL);
      int disk_id = buffer_desc->scan_range_->disk_id();
      DiskQueue* disk_queue = disk_queues_[disk_id];
      
      unique_lock<mutex> disk_lock(disk_queue->lock);
      unique_lock<mutex> reader_lock(reader->lock_);

      DCHECK(reader->Validate()) << endl << reader->DebugString();
      ReaderContext::PerDiskState& state = reader->disk_states_[disk_id];

      ++reader->num_empty_buffers_;
      ++state.num_empty_buffers;
      --reader->num_outstanding_buffers_;
      
      // Add the reader back on the disk queue if necessary.  We want to add the reader
      // on the queue if these conditions are true:
      //  1. it is not already on the queue
      //  2. it is not been completely processed for this disk 
      //  3a. it is cancelled and there are no threads currently processing it
      //  3b. or, it has schedulable scan ranges.
      if (!state.is_on_queue && state.num_scan_ranges > 0 &&
          ((reader->state_ == ReaderContext::Cancelled && state.num_threads_in_read == 0) 
          || !state.ranges.empty())) {
        reader->disk_states_[disk_id].is_on_queue = true;
        disk_queue->readers.push_back(reader);
        disk_queue->work_available.notify_one();
      }
    } 
  }

=======
  if (!buffer_desc->status_.ok()) DCHECK(buffer_desc->buffer_ == NULL);

  RequestContext* reader = buffer_desc->reader_;
  if (buffer_desc->buffer_ != NULL) {
    if (buffer_desc->scan_range_->cached_buffer_ == NULL) {
      // Not a cached buffer. Return the io buffer and update mem tracking.
      ReturnFreeBuffer(buffer_desc);
    }
    buffer_desc->buffer_ = NULL;
    --num_buffers_in_readers_;
    --reader->num_buffers_in_reader_;
  } else {
    // A NULL buffer means there was an error in which case there is no buffer
    // to return.
  }

  if (buffer_desc->eosr_ || buffer_desc->scan_range_->is_cancelled_) {
    // Need to close the scan range if returning the last buffer or the scan range
    // has been cancelled (and the caller might never get the last buffer).
    // Close() is idempotent so multiple cancelled buffers is okay.
    buffer_desc->scan_range_->Close();
  }
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  ReturnBufferDesc(buffer_desc);
}

void DiskIoMgr::ReturnBufferDesc(BufferDescriptor* desc) {
  DCHECK(desc != NULL);
  unique_lock<mutex> lock(free_buffers_lock_);
<<<<<<< HEAD
=======
  DCHECK(find(free_buffer_descs_.begin(), free_buffer_descs_.end(), desc)
         == free_buffer_descs_.end());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  free_buffer_descs_.push_back(desc);
}

DiskIoMgr::BufferDescriptor* DiskIoMgr::GetBufferDesc(
<<<<<<< HEAD
    ReaderContext* reader, ScanRange* range, char* buffer) {
=======
    RequestContext* reader, ScanRange* range, char* buffer, int64_t buffer_size) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  BufferDescriptor* buffer_desc;
  {
    unique_lock<mutex> lock(free_buffers_lock_);
    if (free_buffer_descs_.empty()) {
      buffer_desc = pool_.Add(new BufferDescriptor(this));
    } else {
      buffer_desc = free_buffer_descs_.front();
      free_buffer_descs_.pop_front();
    }
  }
<<<<<<< HEAD
  buffer_desc->Reset(reader, range, buffer);
  return buffer_desc;
}

char* DiskIoMgr::GetFreeBuffer() {
  unique_lock<mutex> lock(free_buffers_lock_);
  if (free_buffers_.empty()) {
    ++num_allocated_buffers_;
    return new char[max_read_size_];
  } else {
    char* buffer = free_buffers_.front();
    free_buffers_.pop_front();
    return buffer;
  }
}

void DiskIoMgr::ReturnFreeBuffer(char* buffer) {
  DCHECK(buffer != NULL);
  unique_lock<mutex> lock(free_buffers_lock_);
  free_buffers_.push_back(buffer);
}

string DiskIoMgr::DebugString() {
  stringstream ss;
  ss << "Readers: " << endl << reader_cache_->DebugString() << endl;

  ss << "Disks: " << endl;
  for (int i = 0; i < disk_queues_.size(); ++i) {
    unique_lock<mutex> lock(disk_queues_[i]->lock);
    ss << "  " << (void*) disk_queues_[i] << ":" ;
    if (!disk_queues_[i]->readers.empty()) {
      ss << " Readers: ";
      for (list<ReaderContext*>::iterator it = disk_queues_[i]->readers.begin();
          it != disk_queues_[i]->readers.end(); ++it) {
        ss << (void*)*it;
      }
    }
    ss << endl;
  }
  return ss.str();
}

void DiskIoMgr::DecrementDiskRefCount(ReaderContext* reader) {
  // boost doesn't let us dcheck that the the reader lock is taken
  DCHECK_GT(reader->num_disks_with_ranges_, 0);
  if (--reader->num_disks_with_ranges_ == 0) {
    reader->disks_complete_cond_var_.notify_one(); 
  }
  DCHECK(reader->Validate()) << endl << reader->DebugString();
}

Status DiskIoMgr::OpenScanRange(hdfsFS hdfs_connection, ScanRange* range) const {
  if (hdfs_connection != NULL) {
    if (range->hdfs_file_ != NULL) return Status::OK;

    // TODO: is there much overhead opening hdfs files?  Should we try to preserve
    // the handle across multiple scan ranges of a file?
    range->hdfs_file_ = 
        hdfsOpenFile(hdfs_connection, range->file_, O_RDONLY, 0, 0, 0);
    if (range->hdfs_file_ == NULL) {
      return Status(AppendHdfsErrorMessage("Failed to open HDFS file ", range->file_));
    }

    if (hdfsSeek(hdfs_connection, range->hdfs_file_, range->offset_) != 0) {
      stringstream ss;
      ss << "Error seeking to " << range->offset_ << " in file: " << range->file_;
      return Status(AppendHdfsErrorMessage(ss.str()));
    }
  } else {
    if (range->local_file_ != NULL) return Status::OK;

    range->local_file_ = fopen(range->file_, "r");
    if (range->local_file_ == NULL) {
      stringstream ss;
      ss << "Could not open file: " << range->file_ << ": " << strerror(errno);
      return Status(ss.str());
    }
    if (fseek(range->local_file_, range->offset_, SEEK_SET) == -1) {
      stringstream ss;
      ss << "Could not seek to " << range->offset_ << " for file: " << range->file_
         << ": " << strerror(errno);
      return Status(ss.str());
    }
  } 
  return Status::OK;
}

void DiskIoMgr::CloseScanRange(hdfsFS hdfs_connection, ScanRange* range) const {
  if (range == NULL) return;
 
  if (hdfs_connection != NULL) {
    if (range->hdfs_file_ == NULL) return;
    hdfsCloseFile(hdfs_connection, range->hdfs_file_);
    range->hdfs_file_ = NULL;
  } else {
    if (range->local_file_ == NULL) return;
    fclose(range->local_file_);
    range->local_file_ = NULL;
  }
}

// TODO: how do we best use the disk here.  e.g. is it good to break up a
// 1MB read into 8 128K reads?
// TODO: look at linux disk scheduling
Status DiskIoMgr::ReadFromScanRange(hdfsFS hdfs_connection, ScanRange* range, 
    char* buffer, int64_t* bytes_read, bool* eosr) {
  *eosr = false;
  *bytes_read = 0;
  int bytes_to_read = min(static_cast<int64_t>(max_read_size_), 
      range->len_ - range->bytes_read_);

  if (hdfs_connection != NULL) {
    DCHECK(range->hdfs_file_ != NULL);
    // TODO: why is this loop necessary? Can hdfs reads come up short?
    while (*bytes_read < bytes_to_read) {
      int last_read = hdfsRead(hdfs_connection, range->hdfs_file_,
          buffer + *bytes_read, bytes_to_read - *bytes_read);
      if (last_read == -1) {
        return Status(
            AppendHdfsErrorMessage("Error reading from HDFS file: ", range->file_));
      } else if (last_read == 0) {
        // No more bytes in the file.  The scan range went past the end
        *eosr = true;
        break;
      }
      *bytes_read += last_read;
    }
  } else {
    DCHECK(range->local_file_ != NULL);
    *bytes_read = fread(buffer, 1, bytes_to_read, range->local_file_);
    if (*bytes_read < 0) {
      stringstream ss;
      ss << "Could not read from " << range->file_ << " at byte offset: " 
         << range->bytes_read_ << ": " << strerror(errno);
      return Status(ss.str());
    }
  }
  range->bytes_read_ += *bytes_read;
  DCHECK_LE(range->bytes_read_, range->len_);
  if (range->bytes_read_ == range->len_) {
    *eosr = true;
  }
  return Status::OK;
}

void DiskIoMgr::RemoveReaderFromDiskQueue(DiskQueue* disk_queue, ReaderContext* reader) {
  DCHECK(reader->disk_states_[disk_queue->disk_id].is_on_queue);
  reader->disk_states_[disk_queue->disk_id].is_on_queue = false;
  list<ReaderContext*>::iterator it = disk_queue->readers.begin();
  for (; it != disk_queue->readers.end(); ++it) {
    if (*it == reader) {
      if (it == disk_queue->next_reader) ++disk_queue->next_reader;
      disk_queue->readers.erase(it);
      return;
    }
  }
  DCHECK(false) << "Reader was not on the queue";
}

// This functions gets the next scan range to work on. 
//  - wait until there is a reader with work and available buffer or the thread should
//    terminate.  Note: the disk's reader queue only contains readers that have both work
//    and buffers so this thread does not have to busy spin on readers.
//  - Remove the scan range, available buffer and cycle to the next reader
// There are a few guarantees this makes which causes some complications.
//  1) Readers are round-robined. 
//  2) Multiple threads (including per disk) can work on the same reader.
//  3) Scan ranges within a reader are round-robined.
bool DiskIoMgr::GetNextScanRange(DiskQueue* disk_queue, ScanRange** range, 
    ReaderContext** reader, char** buffer) {
  // This loops returns either with work to do or when the disk io mgr shuts down.
  while (true) {
    unique_lock<mutex> disk_lock(disk_queue->lock);

    while (!shut_down_ && disk_queue->readers.empty()) {
      // wait if there are no readers on the queue
      disk_queue->work_available.wait(disk_lock);
    }
    if (shut_down_) break;
    DCHECK(!disk_queue->readers.empty());

    // Get reader and advance to the next reader
    if (disk_queue->next_reader == disk_queue->readers.end()) {
      disk_queue->next_reader = disk_queue->readers.begin();
    }
    DCHECK(disk_queue->next_reader != disk_queue->readers.end());

    list<ReaderContext*>::iterator reader_it = disk_queue->next_reader;
    ++disk_queue->next_reader;
    *reader = *reader_it;
    
    // Grab reader lock, both locks are held now
    unique_lock<mutex> reader_lock((*reader)->lock_);
    DCHECK((*reader)->Validate()) << endl << (*reader)->DebugString();
  
    ReaderContext::PerDiskState& state = (*reader)->disk_states_[disk_queue->disk_id];
    
    // Check if reader has been cancelled
    if ((*reader)->state_ == ReaderContext::Cancelled) {
      DCHECK(disk_queue->next_reader != reader_it);
      disk_queue->readers.erase(reader_it);
      state.is_on_queue = false;
      if (state.num_threads_in_read == 0) {
        state.num_scan_ranges = 0;
        DecrementDiskRefCount(*reader);
      }
      continue;
    }

    // Validate invariants.  The reader should be active, it should have
    // a buffer available on this disk and it should have a scan range to read.
    DCHECK(state.is_on_queue);
    DCHECK_GT(state.num_empty_buffers, 0);
    DCHECK_GT((*reader)->num_empty_buffers_, 0);
    DCHECK(!state.ranges.empty());
    
    // Increment the ref count on reader.  We need to track the number of threads per
    // reader per disk that is the in unlocked hdfs read code section.
    ++state.num_threads_in_read;
    
    // Get a free buffer from the disk io mgr.  It's a global pool for all readers but
    // they are lazily allocated.  Each reader is guaranteed its share.
    *buffer = GetFreeBuffer();
    --(*reader)->num_empty_buffers_;
    --state.num_empty_buffers;
    DCHECK(*buffer != NULL);

    // Round robin ranges 
    *range = *state.ranges.begin();
    state.ranges.pop_front();

    if (state.ranges.empty() || state.num_empty_buffers == 0) {
      // If this is the last scan range for this reader on this disk or the
      // last buffer on this disk, temporarily remove
      // the reader from the disk queue.  We don't want another disk thread to
      // pick up this reader when it can't do work.  The reader is added back
      // on the queue when the scan range is read or when the buffer is returned.
      DCHECK(disk_queue->next_reader != reader_it);
      disk_queue->readers.erase(reader_it);
      state.is_on_queue = false;
    }
    DCHECK_LT((*range)->bytes_read_, (*range)->len_);
    return true;
  }

  DCHECK(shut_down_);
  return false;
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, ReaderContext* reader,
    BufferDescriptor* buffer) {
  {
    unique_lock<mutex> disk_lock(disk_queue->lock);
    unique_lock<mutex> reader_lock(reader->lock_);

    ReaderContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
    
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    DCHECK_GT(state.num_threads_in_read, 0);
    DCHECK(buffer->buffer_ != NULL);

    --state.num_threads_in_read;

    if (reader->state_ == ReaderContext::Cancelled) {
      // For a cancelled reader, remove it from the disk queue right away.  We don't
      // do the final cleanup though unless we are the last thread.
      if (state.is_on_queue) {
        RemoveReaderFromDiskQueue(disk_queue, reader);
      }
      CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      ++reader->num_empty_buffers_;
      ++state.num_empty_buffers;
      ReturnFreeBuffer(buffer->buffer_);
      buffer->buffer_ = NULL;
      ReturnBufferDesc(buffer);
      if (state.num_threads_in_read == 0) {
        state.num_scan_ranges = 0;
        DecrementDiskRefCount(reader);
      }
      return;
    }
        
    DCHECK_EQ(reader->state_, ReaderContext::Active);
    DCHECK(buffer->buffer_ != NULL);

    // Update the reader's scan ranges.  There are a three cases here:
    //  1. Read error
    //  2. End of scan range
    //  3. Middle of scan range
    if (!buffer->status_.ok()) {
      CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      ++reader->num_empty_buffers_;
      ++state.num_empty_buffers;
      ReturnFreeBuffer(buffer->buffer_);
      buffer->buffer_ = NULL;
      buffer->eosr_ = true;
    } else {
      if (!buffer->eosr_) {
        DCHECK_LT(buffer->scan_range_->bytes_read_, buffer->scan_range_->len_);
        // The order in which scan ranges on a single disk are processed has a significant
        // effect on performance.  We'd like to keep the minimum number of ranges in
        // flight while being able to saturate all the cpus.  This is made more 
        // complicated by the fact that the cpu usage varies tremendously from format
        // to format and this needs to dynamically deal with that.
        //
        // We will round robin through the first N scan ranges queued.  For example,
        // if N was 3 and the total number of ranges on this disk was 50, we'd
        // round robin through 1,2,3 until one of them (e.g. 2) finished, and then
        // round robin between 1, 3, 4.
        // N is the number of buffers for this reader (per disk).
        int queue_location = 
            reader->num_buffers_per_disk_ - 1 - state.num_threads_in_read;
        DCHECK_GE(queue_location, 0);
        list<ScanRange*>::iterator it = state.ranges.begin();
        for (int i = 0; i < queue_location && it != state.ranges.end(); ++i) {
          ++it;
        }
        state.ranges.insert(it, buffer->scan_range_);
      } else {
        CloseScanRange(reader->hdfs_connection_, buffer->scan_range_);
      }
    }

    if (buffer->eosr_) --state.num_scan_ranges;

    // We now update the disk state, adding or removing the reader from the disk
    // queue if necessary.  If all the scan ranges on this disk for this reader
    // are complete, remove the reader.  Otherwise, add the reader.
    if (state.num_scan_ranges == 0) {
      // All scan ranges are finished
      if (state.is_on_queue) {
        RemoveReaderFromDiskQueue(disk_queue, reader);
      }
      DecrementDiskRefCount(reader);
    } else if (!state.is_on_queue) {
      if (!state.ranges.empty() && state.num_empty_buffers > 0) {
        // The reader could have been removed by the thread working on the last 
        // scan range or last buffer.  In that case, we need to add the reader back 
        // on the disk queue since there is still more work to do.
        disk_queue->readers.push_back(reader);
        state.is_on_queue = true;
      }
    }

    // Add the result to the reader's queue and notify the reader
    reader->ready_buffers_.push_back(buffer);
  }
  reader->buffer_ready_cond_var_.notify_one();
}

// The thread waits until there is both work (which is guaranteed to show up with an
// empty buffer) or the entire system is being shut down.  If there is work, it reads the 
// next chunk of the next scan range for the first reader in the queue and round robins 
// across the readers.
// Locks are not taken when reading from disk.  The main loop has three parts:
//   1. GetNextScanRange(): Take locks and figure out what the next scan range to read is
//   2. Open/Read the scan range.  No locks are taken
//   3. HandleReadFinished(): Take locks and update the disk and reader with the 
//      results of the io.
// Cancellation checking needs to happen in both steps 1 and 3.
void DiskIoMgr::ReadLoop(DiskQueue* disk_queue) {
  while (true) {
    char* buffer = NULL;
    ReaderContext* reader = NULL;;
    ScanRange* range = NULL;
    
    // Get the next scan range to read
    if (!GetNextScanRange(disk_queue, &range, &reader, &buffer)) {
      DCHECK(shut_down_);
      break;
    }
    DCHECK(range != NULL);
    DCHECK(reader != NULL);
    DCHECK(buffer != NULL);

    BufferDescriptor* buffer_desc = GetBufferDesc(reader, range, buffer);
    DCHECK(buffer_desc != NULL);

    // No locks in this section.  Only working on local vars.  We don't want to hold a 
    // lock across the read call.
    buffer_desc->status_ = OpenScanRange(reader->hdfs_connection_, range);
    if (buffer_desc->status_.ok()) {
      // Update counters.
      SCOPED_TIMER(&read_timer_);
      SCOPED_TIMER(reader->read_timer_);
      
      buffer_desc->status_ = ReadFromScanRange(
          reader->hdfs_connection_, range, buffer, &buffer_desc->len_,
          &buffer_desc->eosr_);
      buffer_desc->scan_range_offset_ = range->bytes_read_ - buffer_desc->len_;
    
      if (reader->bytes_read_counter_ != NULL) {
        COUNTER_UPDATE(reader->bytes_read_counter_, buffer_desc->len_);
      }
      COUNTER_UPDATE(&total_bytes_read_counter_, buffer_desc->len_);
    }

    // Finished read, update reader/disk based on the results
    HandleReadFinished(disk_queue, reader, buffer_desc);
  } 

  DCHECK(shut_down_);
=======
  buffer_desc->Reset(reader, range, buffer, buffer_size);
  buffer_desc->SetMemTracker(reader->mem_tracker_);
  return buffer_desc;
}

char* DiskIoMgr::GetFreeBuffer(int64_t* buffer_size) {
  DCHECK_LE(*buffer_size, max_buffer_size_);
  DCHECK_GT(*buffer_size, 0);
  *buffer_size = min(static_cast<int64_t>(max_buffer_size_), *buffer_size);
  int idx = free_buffers_idx(*buffer_size);
  // Quantize buffer size to nearest power of 2 greater than the specified buffer size and
  // convert to bytes
  *buffer_size = (1 << idx) * min_buffer_size_;

  unique_lock<mutex> lock(free_buffers_lock_);
  char* buffer = NULL;
  if (free_buffers_[idx].empty()) {
    ++num_allocated_buffers_;
    if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(1L);
    }
    if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != NULL) {
      ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(*buffer_size);
    }
    // Update the process mem usage.  This is checked the next time we start
    // a read for the next reader (DiskIoMgr::GetNextScanRange)
    process_mem_tracker_->Consume(*buffer_size);
    buffer = new char[*buffer_size];
  } else {
    if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(-1L);
    }
    buffer = free_buffers_[idx].front();
    free_buffers_[idx].pop_front();
  }
  DCHECK(buffer != NULL);
  return buffer;
}

void DiskIoMgr::GcIoBuffers() {
  unique_lock<mutex> lock(free_buffers_lock_);
  int buffers_freed = 0;
  int bytes_freed = 0;
  for (int idx = 0; idx < free_buffers_.size(); ++idx) {
    for (list<char*>::iterator iter = free_buffers_[idx].begin();
         iter != free_buffers_[idx].end(); ++iter) {
      int64_t buffer_size = (1 << idx) * min_buffer_size_;
      process_mem_tracker_->Release(buffer_size);
      --num_allocated_buffers_;
      delete[] *iter;

      ++buffers_freed;
      bytes_freed += buffer_size;
    }
    free_buffers_[idx].clear();
  }

  if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != NULL) {
    ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(-buffers_freed);
  }
  if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != NULL) {
    ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(-bytes_freed);
  }
  if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != NULL) {
    ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Update(0);
  }
}

void DiskIoMgr::ReturnFreeBuffer(BufferDescriptor* desc) {
  ReturnFreeBuffer(desc->buffer_, desc->buffer_len_);
  desc->SetMemTracker(NULL);
  desc->buffer_ = NULL;
}

void DiskIoMgr::ReturnFreeBuffer(char* buffer, int64_t buffer_size) {
  DCHECK(buffer != NULL);
  int idx = free_buffers_idx(buffer_size);
  DCHECK_EQ(BitUtil::Ceil(buffer_size, min_buffer_size_) & ~(1 << idx), 0)
      << "buffer_size_ / min_buffer_size_ should be power of 2, got buffer_size = "
      << buffer_size << ", min_buffer_size_ = " << min_buffer_size_;
  unique_lock<mutex> lock(free_buffers_lock_);
  if (!FLAGS_disable_mem_pools && free_buffers_[idx].size() < FLAGS_max_free_io_buffers) {
    free_buffers_[idx].push_back(buffer);
    if (ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_UNUSED_BUFFERS->Increment(1L);
    }
  } else {
    process_mem_tracker_->Release(buffer_size);
    --num_allocated_buffers_;
    delete[] buffer;
    if (ImpaladMetrics::IO_MGR_NUM_BUFFERS != NULL) {
      ImpaladMetrics::IO_MGR_NUM_BUFFERS->Increment(-1L);
    }
    if (ImpaladMetrics::IO_MGR_TOTAL_BYTES != NULL) {
      ImpaladMetrics::IO_MGR_TOTAL_BYTES->Increment(-buffer_size);
    }
  }
}

// This function gets the next RequestRange to work on for this disk. It checks for
// cancellation and
// a) Updates ready_to_start_ranges if there are no scan ranges queued for this disk.
// b) Adds an unstarted write range to in_flight_ranges_. The write range is processed
//    immediately if there are no preceding scan ranges in in_flight_ranges_
// It blocks until work is available or the thread is shut down.
// Work is available if there is a RequestContext with
//  - A ScanRange with a buffer available, or
//  - A WriteRange in unstarted_write_ranges_.
bool DiskIoMgr::GetNextRequestRange(DiskQueue* disk_queue, RequestRange** range,
    RequestContext** request_context) {
  int disk_id = disk_queue->disk_id;
  *range = NULL;

  // This loops returns either with work to do or when the disk IoMgr shuts down.
  while (true) {
    *request_context = NULL;
    RequestContext::PerDiskState* request_disk_state = NULL;
    {
      unique_lock<mutex> disk_lock(disk_queue->lock);

      while (!shut_down_ && disk_queue->request_contexts.empty()) {
        // wait if there are no readers on the queue
        disk_queue->work_available.wait(disk_lock);
      }
      if (shut_down_) break;
      DCHECK(!disk_queue->request_contexts.empty());

      // Get the next reader and remove the reader so that another disk thread
      // can't pick it up.  It will be enqueued before issuing the read to HDFS
      // so this is not a big deal (i.e. multiple disk threads can read for the
      // same reader).
      // TODO: revisit.
      *request_context = disk_queue->request_contexts.front();
      disk_queue->request_contexts.pop_front();
      DCHECK(*request_context != NULL);
      request_disk_state = &((*request_context)->disk_states_[disk_id]);
      request_disk_state->IncrementRequestThreadAndDequeue();
    }

    // NOTE: no locks were taken in between.  We need to be careful about what state
    // could have changed to the reader and disk in between.
    // There are some invariants here.  Only one disk thread can have the
    // same reader here (the reader is removed from the queue).  There can be
    // other disk threads operating on this reader in other functions though.

    // We just picked a reader, check the mem limits.
    // TODO: we can do a lot better here.  The reader can likely make progress
    // with fewer io buffers.
    bool process_limit_exceeded = process_mem_tracker_->LimitExceeded();
    bool reader_limit_exceeded = (*request_context)->mem_tracker_ != NULL
        ? (*request_context)->mem_tracker_->AnyLimitExceeded() : false;

    if (process_limit_exceeded || reader_limit_exceeded) {
      (*request_context)->Cancel(Status::MEM_LIMIT_EXCEEDED);
    }

    unique_lock<mutex> request_lock((*request_context)->lock_);
    VLOG_FILE << "Disk (id=" << disk_id << ") reading for "
        << (*request_context)->DebugString();

    // Check if reader has been cancelled
    if ((*request_context)->state_ == RequestContext::Cancelled) {
      request_disk_state->DecrementRequestThreadAndCheckDone(*request_context);
      continue;
    }

    DCHECK_EQ((*request_context)->state_, RequestContext::Active)
        << (*request_context)->DebugString();

    if (request_disk_state->next_scan_range_to_start() == NULL &&
        !request_disk_state->unstarted_scan_ranges()->empty()) {
      // We don't have a range queued for this disk for what the caller should
      // read next. Populate that.  We want to have one range waiting to minimize
      // wait time in GetNextRange.
      ScanRange* new_range = request_disk_state->unstarted_scan_ranges()->Dequeue();
      --(*request_context)->num_unstarted_scan_ranges_;
      (*request_context)->ready_to_start_ranges_.Enqueue(new_range);
      request_disk_state->set_next_scan_range_to_start(new_range);

      if ((*request_context)->num_unstarted_scan_ranges_ == 0) {
        // All the ranges have been started, notify everyone blocked on GetNextRange.
        // Only one of them will get work so make sure to return NULL to the other
        // caller threads.
        (*request_context)->ready_to_start_ranges_cv_.notify_all();
      } else {
        (*request_context)->ready_to_start_ranges_cv_.notify_one();
      }
    }

    // Always enqueue a WriteRange to be processed into in_flight_ranges_.
    // This is done so in_flight_ranges_ does not exclusively contain ScanRanges.
    // For now, enqueuing a WriteRange on each invocation of GetNextRequestRange()
    // does not flood in_flight_ranges() with WriteRanges because the entire
    // WriteRange is processed and removed from the queue after GetNextRequestRange()
    // returns. (A DCHECK is used to ensure that writes do not exceed 8MB).
    if (!request_disk_state->unstarted_write_ranges()->empty()) {
      WriteRange* write_range = request_disk_state->unstarted_write_ranges()->Dequeue();
      request_disk_state->in_flight_ranges()->Enqueue(write_range);
    }

    // Get the next scan range to work on from the reader. Only in_flight_ranges
    // are eligible since the disk threads do not start new ranges on their own.

    // There are no inflight ranges, nothing to do.
    if (request_disk_state->in_flight_ranges()->empty()) {
      request_disk_state->DecrementRequestThread();
      continue;
    }
    DCHECK_GT(request_disk_state->num_remaining_ranges(), 0);
    *range = request_disk_state->in_flight_ranges()->Dequeue();
    DCHECK(*range != NULL);

    // Now that we've picked a request range, put the context back on the queue so
    // another thread can pick up another request range for this context.
    request_disk_state->ScheduleContext(*request_context, disk_id);
    DCHECK((*request_context)->Validate()) << endl << (*request_context)->DebugString();
    return true;
  }

  DCHECK(shut_down_);
  return false;
}

void DiskIoMgr::HandleWriteFinished(RequestContext* writer, WriteRange* write_range,
    const Status& write_status) {
  {
    unique_lock<mutex> writer_lock(writer->lock_);
    DCHECK(writer->Validate()) << endl << writer->DebugString();
    RequestContext::PerDiskState& state = writer->disk_states_[write_range->disk_id_];
    if (writer->state_ == RequestContext::Cancelled) {
      state.DecrementRequestThreadAndCheckDone(writer);
    } else {
      state.DecrementRequestThread();
    }
    --state.num_remaining_ranges();
  }
  // The status of the write does not affect the status of the writer context.
  write_range->callback_(write_status);
}

void DiskIoMgr::HandleReadFinished(DiskQueue* disk_queue, RequestContext* reader,
    BufferDescriptor* buffer) {
  unique_lock<mutex> reader_lock(reader->lock_);

  RequestContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
  DCHECK(reader->Validate()) << endl << reader->DebugString();
  DCHECK_GT(state.num_threads_in_op(), 0);
  DCHECK(buffer->buffer_ != NULL);

  if (reader->state_ == RequestContext::Cancelled) {
    state.DecrementRequestThreadAndCheckDone(reader);
    DCHECK(reader->Validate()) << endl << reader->DebugString();
    ReturnFreeBuffer(buffer);
    buffer->buffer_ = NULL;
    buffer->scan_range_->Cancel(reader->status_);
    // Enqueue the buffer to use the scan range's buffer cleanup path.
    buffer->scan_range_->EnqueueBuffer(buffer);
    return;
  }

  DCHECK_EQ(reader->state_, RequestContext::Active);
  DCHECK(buffer->buffer_ != NULL);

  // Update the reader's scan ranges.  There are a three cases here:
  //  1. Read error
  //  2. End of scan range
  //  3. Middle of scan range
  if (!buffer->status_.ok()) {
    // Error case
    ReturnFreeBuffer(buffer);
    buffer->eosr_ = true;
    --state.num_remaining_ranges();
    buffer->scan_range_->Cancel(buffer->status_);
  } else if (buffer->eosr_) {
    --state.num_remaining_ranges();
  }

  bool queue_full = buffer->scan_range_->EnqueueBuffer(buffer);
  if (!buffer->eosr_) {
    if (queue_full) {
      reader->blocked_ranges_.Enqueue(buffer->scan_range_);
    } else {
      reader->ScheduleScanRange(buffer->scan_range_);
    }
  }
  state.DecrementRequestThread();
}

void DiskIoMgr::WorkLoop(DiskQueue* disk_queue) {
  // The thread waits until there is work or the entire system is being shut down.
  // If there is work, performs the read or write requested and re-enqueues the
  // requesting context.
  // Locks are not taken when reading from or writing to disk.
  // The main loop has three parts:
  //   1. GetNextRequestContext(): get the next request context (read or write) to
  //      process and dequeue it.
  //   2. For the dequeued request, gets the next scan- or write-range to process and
  //      re-enqueues the request.
  //   3. Perform the read or write as specified.
  // Cancellation checking needs to happen in both steps 1 and 3.
  while (true) {
    RequestContext* worker_context = NULL;;
    RequestRange* range = NULL;

    if (!GetNextRequestRange(disk_queue, &range, &worker_context)) {
      DCHECK(shut_down_);
      break;
    }

    if (range->request_type() == RequestType::READ) {
      ReadRange(disk_queue, worker_context, static_cast<ScanRange*>(range));
    } else {
      DCHECK(range->request_type() == RequestType::WRITE);
      Write(worker_context, static_cast<WriteRange*>(range));
    }
  }

  DCHECK(shut_down_);
}

// This function reads the specified scan range associated with the
// specified reader context and disk queue.
void DiskIoMgr::ReadRange(DiskQueue* disk_queue, RequestContext* reader,
    ScanRange* range) {
  char* buffer = NULL;
  int64_t bytes_remaining = range->len_ - range->bytes_read_;
  DCHECK_GT(bytes_remaining, 0);
  int64_t buffer_size = ::min(bytes_remaining, static_cast<int64_t>(max_buffer_size_));
  bool enough_memory = true;
  if (reader->mem_tracker_ != NULL) {
    enough_memory = reader->mem_tracker_->SpareCapacity() > LOW_MEMORY;
    if (!enough_memory) {
      // Low memory, GC and try again.
      GcIoBuffers();
      enough_memory = reader->mem_tracker_->SpareCapacity() > LOW_MEMORY;
    }
  }

  if (!enough_memory) {
    RequestContext::PerDiskState& state = reader->disk_states_[disk_queue->disk_id];
    unique_lock<mutex> reader_lock(reader->lock_);

    // Just grabbed the reader lock, check for cancellation.
    if (reader->state_ == RequestContext::Cancelled) {
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      state.DecrementRequestThreadAndCheckDone(reader);
      range->Cancel(reader->status_);
      DCHECK(reader->Validate()) << endl << reader->DebugString();
      return;
    }

    if (!range->ready_buffers_.empty()) {
      // We have memory pressure and this range doesn't need another buffer
      // (it already has one queued). Skip this range and pick it up later.
      range->blocked_on_queue_ = true;
      reader->blocked_ranges_.Enqueue(range);
      state.DecrementRequestThread();
      return;
    } else {
      // We need to get a buffer anyway since there are none queued. The query
      // is likely to fail due to mem limits but there's nothing we can do about that
      // now.
    }
  }

  buffer = GetFreeBuffer(&buffer_size);
  ++reader->num_used_buffers_;

  // Validate more invariants.
  DCHECK_GT(reader->num_used_buffers_, 0);
  DCHECK(range != NULL);
  DCHECK(reader != NULL);
  DCHECK(buffer != NULL);

  BufferDescriptor* buffer_desc = GetBufferDesc(reader, range, buffer, buffer_size);
  DCHECK(buffer_desc != NULL);

  // No locks in this section.  Only working on local vars.  We don't want to hold a
  // lock across the read call.
  buffer_desc->status_ = range->Open();
  if (buffer_desc->status_.ok()) {
    // Update counters.
    if (reader->active_read_thread_counter_) {
      reader->active_read_thread_counter_->Add(1L);
    }
    if (reader->disks_accessed_bitmap_) {
      int64_t disk_bit = 1 << disk_queue->disk_id;
      reader->disks_accessed_bitmap_->BitOr(disk_bit);
    }
    SCOPED_TIMER(&read_timer_);
    SCOPED_TIMER(reader->read_timer_);

    buffer_desc->status_ = range->Read(buffer, &buffer_desc->len_, &buffer_desc->eosr_);
    buffer_desc->scan_range_offset_ = range->bytes_read_ - buffer_desc->len_;

    if (reader->bytes_read_counter_ != NULL) {
      COUNTER_ADD(reader->bytes_read_counter_, buffer_desc->len_);
    }

    COUNTER_ADD(&total_bytes_read_counter_, buffer_desc->len_);
    if (reader->active_read_thread_counter_) {
      reader->active_read_thread_counter_->Add(-1L);
    }
  }

  // Finished read, update reader/disk based on the results
  HandleReadFinished(disk_queue, reader, buffer_desc);
}

void DiskIoMgr::Write(RequestContext* writer_context, WriteRange* write_range) {
  FILE* file_handle = fopen(write_range->file(), "rb+");
  Status ret_status;
  if (file_handle == NULL) {
    ret_status = Status(TStatusCode::RUNTIME_ERROR,
        Substitute("fopen($0, \"rb+\") failed with errno=$1 description=$2",
            write_range->file_, errno, GetStrErrMsg()));
  } else {
    ret_status = WriteRangeHelper(file_handle, write_range);

    int success = fclose(file_handle);
    if (ret_status.ok() && success != 0) {
      ret_status = Status(TStatusCode::RUNTIME_ERROR, Substitute("fclose($0) failed",
          write_range->file_));
    }
  }

  HandleWriteFinished(writer_context, write_range, ret_status);
}

Status DiskIoMgr::WriteRangeHelper(FILE* file_handle, WriteRange* write_range) {
  // First ensure that disk space is allocated via fallocate().
  int file_desc = fileno(file_handle);
  int success = 0;
  if (write_range->len_ > 0) {
    success = posix_fallocate(file_desc, write_range->offset(), write_range->len_);
  }
  if (success != 0) {
    return Status(TStatusCode::RUNTIME_ERROR,
        Substitute("posix_fallocate($0, $1, $2) failed for file $3"
            " with returnval=$4 description=$5", file_desc, write_range->offset(),
            write_range->len_, write_range->file_, success, GetStrErrMsg()));
  }
  // Seek to the correct offset and perform the write.
  success = fseek(file_handle, write_range->offset(), SEEK_SET);
  if (success != 0) {
    return Status(TStatusCode::RUNTIME_ERROR,
        Substitute("fseek($0, $1, SEEK_SET) failed with errno=$2 description=$3",
            write_range->file_, write_range->offset(), errno, GetStrErrMsg()));
  }

  int64_t bytes_written = fwrite(write_range->data_, 1, write_range->len_, file_handle);
  if (bytes_written < write_range->len_) {
    return Status(TStatusCode::RUNTIME_ERROR,
        Substitute("fwrite(buffer, 1, $0, $1) failed with errno=$2 description=$3",
            write_range->len_, write_range->file_, errno, GetStrErrMsg()));
  }
  if (ImpaladMetrics::IO_MGR_BYTES_WRITTEN != NULL) {
    ImpaladMetrics::IO_MGR_BYTES_WRITTEN->Increment(write_range->len_);
  }

  return Status::OK;
}

int DiskIoMgr::free_buffers_idx(int64_t buffer_size) {
  int64_t buffer_size_scaled = BitUtil::Ceil(buffer_size, min_buffer_size_);
  int idx = BitUtil::Log2(buffer_size_scaled);
  DCHECK_GE(idx, 0);
  DCHECK_LT(idx, free_buffers_.size());
  return idx;
}

Status DiskIoMgr::AddWriteRange(RequestContext* writer, WriteRange* write_range) {
  DCHECK_LE(write_range->len(), max_buffer_size_);
  unique_lock<mutex> writer_lock(writer->lock_);

  if (writer->state_ == RequestContext::Cancelled) {
    DCHECK(!writer->status_.ok());
    return writer->status_;
  }

  writer->AddRequestRange(write_range, false);
  return Status::OK;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
