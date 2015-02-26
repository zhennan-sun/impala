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

=======
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#ifndef IMPALA_UTIL_METRICS_H
#define IMPALA_UTIL_METRICS_H

#include <map>
#include <string>
#include <sstream>
#include <boost/thread/mutex.hpp>
#include <boost/scoped_ptr.hpp>

#include "common/logging.h"
#include "common/status.h"
#include "common/object-pool.h"
<<<<<<< HEAD

namespace impala {

class Webserver;
=======
#include "util/debug-util.h"
#include "util/webserver.h"

namespace impala {

// Helper method to print a single primitive value as a Json atom
template<typename T> void PrintPrimitiveAsJson(const T& v, std::stringstream* out) {
  (*out) << v;
}

// Specialisation to print string values inside quotes when writing to Json
template<> void PrintPrimitiveAsJson<std::string>(const std::string& v,
                                                  std::stringstream* out);

// Specialisation to intercept NaN and inf and print them as null,
// because JSON doesn't allow for non-finite floating point values (!)
template<> void PrintPrimitiveAsJson<double>(const double& v, std::stringstream* out);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

// Publishes execution metrics to a webserver page
// TODO: Reconsider naming here; Metrics is too general.
class Metrics {
 private:
  // Superclass for metric types, to allow for a single container to hold all metrics
  class GenericMetric {
   public:
<<<<<<< HEAD
=======
    // Empty virtual destructor
    virtual ~GenericMetric() {}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    // Print key and value to a string
    virtual void Print(std::stringstream* out) = 0;

    // Print key and value in Json format
    virtual void PrintJson(std::stringstream* out) = 0;
  };

 public:
<<<<<<< HEAD
  // Structure containing a metric value. Provides for thread-safe update and 
=======
  // Structure containing a metric value. Provides for thread-safe update and
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  // test-and-set operations.
  template<typename T>
  class Metric : GenericMetric {
   public:
    // Sets current metric value to parameter
<<<<<<< HEAD
    void Update(const T& value) { 
=======
    void Update(const T& value) {
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      boost::lock_guard<boost::mutex> l(lock_);
      value_ = value;
    }

    // If current value == test_, update with new value. In all cases return
<<<<<<< HEAD
    // current value so that success can be detected. 
=======
    // current value so that success can be detected.
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    T TestAndSet(const T& value, const T& test) {
      boost::lock_guard<boost::mutex> l(lock_);
      if (value_ == test) {
        value_ = value;
        return test;
      }
      return value_;
    }

    // Reads the current value under the metric lock
    T value() {
      boost::lock_guard<boost::mutex> l(lock_);
<<<<<<< HEAD
=======
      CalculateValue();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      return value_;
    }

    virtual void Print(std::stringstream* out) {
      boost::lock_guard<boost::mutex> l(lock_);
<<<<<<< HEAD
      (*out) << key_ << ":";
      PrintValue(out);
    }    

    virtual void PrintJson(std::stringstream* out) {
      boost::lock_guard<boost::mutex> l(lock_);
=======
      CalculateValue();
      (*out) << key_ << ":";
      PrintValue(out);
    }

    virtual void PrintJson(std::stringstream* out) {
      boost::lock_guard<boost::mutex> l(lock_);
      CalculateValue();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      (*out) << "\"" << key_ << "\": ";
      PrintValueJson(out);
    }

<<<<<<< HEAD
    Metric(const std::string& key, const T& value) 
        : value_(value), key_(key) { }

=======
    Metric(const std::string& key, const T& value)
        : value_(value), key_(key) { }

    virtual ~Metric() { }

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
   protected:
    // Subclasses are required to implement this to print a string
    // representation of the metric to the supplied stringstream.
    // Both methods are always called with lock_ taken, so implementations must
    // not try and take lock_ themselves..
    virtual void PrintValue(std::stringstream* out) = 0;
    virtual void PrintValueJson(std::stringstream* out) = 0;

<<<<<<< HEAD
=======
    // Subclasses may implement this to update value_ before it's retrieved. Always called
    // with lock_ held.
    virtual void CalculateValue() { };

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    // Guards access to value
    boost::mutex lock_;
    T value_;

    // Unique key identifying this metric
    const std::string key_;

    friend class Metrics;
<<<<<<< HEAD
  };

  // PrimitiveMetrics are the most common metric type, whose values natively
  // support operator<< and optionally operator+. 
  template<typename T>
  class PrimitiveMetric : public Metric<T> {
   public:
    PrimitiveMetric(const std::string& key, const T& value) 
=======

    // Some sub-metrics may not want to initialise a value
    // (e.g. statistic-gathering metrics). This constructor is
    // accessible to subclasses, but not to clients.
    Metric(const std::string& key) : key_(key) { }
  };

  // PrimitiveMetrics are the most common metric type, whose values natively
  // support operator<< and optionally operator+.
  template<typename T>
  class PrimitiveMetric : public Metric<T> {
   public:
    PrimitiveMetric(const std::string& key, const T& value)
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
        : Metric<T>(key, value) {
    }

    // Requires that T supports operator+. Returns value of metric after increment
    T Increment(const T& delta) {
      boost::lock_guard<boost::mutex> l(this->lock_);
      this->value_ += delta;
      return this->value_;
    }

   protected:
<<<<<<< HEAD
    virtual void PrintValue(std::stringstream* out)  {
      (*out) << this->value_;
    }    

    virtual void PrintValueJson(std::stringstream* out)  {
      (*out) << "\"" << this->value_ << "\"";
    }    
  };

  // Convenient typedefs for common primitive metric types.
  typedef struct PrimitiveMetric<int64_t> IntMetric;
  typedef struct PrimitiveMetric<double> DoubleMetric;
  typedef struct PrimitiveMetric<std::string> StringMetric;
  typedef struct PrimitiveMetric<bool> BooleanMetric;
=======
    virtual void PrintValue(std::stringstream* out) {
      (*out) << this->value_;
    }

    virtual void PrintValueJson(std::stringstream* out) {
      PrintPrimitiveAsJson(this->value_, out);
    }
  };

  // Convenient typedefs for common primitive metric types.
  typedef class PrimitiveMetric<int64_t> IntMetric;
  typedef class PrimitiveMetric<double> DoubleMetric;
  typedef class PrimitiveMetric<std::string> StringMetric;
  typedef class PrimitiveMetric<bool> BooleanMetric;

  class BytesMetric : public IntMetric {
   public:
    BytesMetric(const std::string& key, const int64_t& value) : IntMetric(key, value) { }

   protected:
    virtual void PrintValue(std::stringstream* out) {
      (*out) << PrettyPrinter::Print(value_, TCounterType::BYTES);
    }
  };
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  Metrics();

  // Create a primitive metric object with given key and initial value (owned by
  // this object) If a metric is already registered to this name it will be
  // overwritten (in debug builds it is an error)
  template<typename T>
<<<<<<< HEAD
  PrimitiveMetric<T>* CreateAndRegisterPrimitiveMetric(const std::string& key, 
=======
  PrimitiveMetric<T>* CreateAndRegisterPrimitiveMetric(const std::string& key,
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
      const T& value) {
    return RegisterMetric(new PrimitiveMetric<T>(key, value));
  }

  // Registers a new metric. Ownership of the metric will be transferred to this
  // Metrics object, so callers should take care not to destroy the Metric they
  // pass in.
<<<<<<< HEAD
  // If a metric already exists with the supplied metric's key, it is replaced. 
  // The template parameter M must be a subclass of Metric.
  template <typename M>
  M* RegisterMetric(M* metric) {
    boost::lock_guard<boost::mutex> l(lock_);    
    DCHECK(!metric->key_.empty());
    DCHECK(metric_map_.find(metric->key_) == metric_map_.end()) 
      << "Multiple registrations of metric key: " << metric->key_;

    M* mt = obj_pool_->Add(metric);
    metric_map_[metric->key_] = mt;
    return mt;    
=======
  // If a metric already exists with the supplied metric's key, it is replaced.
  // The template parameter M must be a subclass of Metric.
  template <typename M>
  M* RegisterMetric(M* metric) {
    boost::lock_guard<boost::mutex> l(lock_);
    DCHECK(!metric->key_.empty());
    DCHECK(metric_map_.find(metric->key_) == metric_map_.end());

    M* mt = obj_pool_->Add(metric);
    metric_map_[metric->key_] = mt;
    return mt;
  }

  // Returns a metric by key.  Returns NULL if there is no metric with that
  // key.  This is not a very cheap operation and should not be called in a loop.
  // If the metric needs to be updated in a loop, the returned metric should be cached.
  template <typename M>
  M* GetMetric(const std::string& key) {
    boost::lock_guard<boost::mutex> l(lock_);
    MetricMap::iterator it = metric_map_.find(key);
    if (it == metric_map_.end()) return NULL;
    return reinterpret_cast<M*>(it->second);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  // Register page callbacks with the webserver
  Status Init(Webserver* webserver);

  // Useful for debuggers, returns the output of TextCallback
  std::string DebugString();

<<<<<<< HEAD
=======
  // Same as above, but for Json output
  std::string DebugStringJson();

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
 private:
  // Pool containing all metric objects
  boost::scoped_ptr<ObjectPool> obj_pool_;

  // Contains all Metric objects, indexed by key
  typedef std::map<std::string, GenericMetric*> MetricMap;
  MetricMap metric_map_;

  // Guards metric_map_
  boost::mutex lock_;

<<<<<<< HEAD
  // Writes metric_map_ as a list of key : value pairs
  void PrintMetricMap(std::stringstream* output);

  // Builds a list of metrics as Json-style "key": "value" pairs
  void PrintMetricMapAsJson(std::vector<std::string>* metrics);

  // Webserver callback (on /metrics), renders metrics as single text page
  void TextCallback(std::stringstream* output);

  // Webserver callback (on /jsonmetrics), renders metrics as a single json document
  void JsonCallback(std::stringstream* output);
};

=======
  // Webserver callback (on /metrics), renders metrics as single text page
  void TextCallback(const Webserver::ArgumentMap& args, rapidjson::Document* output);

  // Webserver callback (on /jsonmetrics), renders metrics as a single json document
  void JsonCallback(const Webserver::ArgumentMap& args, rapidjson::Document* document);
};

// Specialize int metrics to use atomics and avoid locking
template<>
inline int64_t Metrics::PrimitiveMetric<int64_t>::Increment(const int64_t& delta) {
  return __sync_add_and_fetch(&value_, delta);
}


>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}

#endif // IMPALA_UTIL_METRICS_H
