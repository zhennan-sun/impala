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
#include "common/logging.h"
#include "util/webserver.h"
#include "util/metrics.h"
=======
#include "util/metrics.h"

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include <sstream>
#include <boost/algorithm/string/join.hpp>
#include <boost/foreach.hpp>
#include <boost/bind.hpp>
#include <boost/mem_fn.hpp>
<<<<<<< HEAD
=======
#include <boost/math/special_functions/fpclassify.hpp>
#include <gutil/strings/substitute.h>

#include "common/logging.h"
#include "util/impalad-metrics.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

using namespace impala;
using namespace std;
using namespace boost;
using namespace boost::algorithm;
<<<<<<< HEAD

Metrics::Metrics() 
=======
using namespace rapidjson;
using namespace strings;

Metrics::Metrics()
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  : obj_pool_(new ObjectPool()) { }

Status Metrics::Init(Webserver* webserver) {
  if (webserver != NULL) {
<<<<<<< HEAD
    Webserver::PathHandlerCallback default_callback =
        bind<void>(mem_fn(&Metrics::TextCallback), this, _1);    
    webserver->RegisterPathHandler("/metrics", default_callback);

    Webserver::PathHandlerCallback json_callback =
        bind<void>(mem_fn(&Metrics::JsonCallback), this, _1);    
    webserver->RegisterPathHandler("/jsonmetrics", json_callback);
=======
    Webserver::UrlCallback default_callback =
        bind<void>(mem_fn(&Metrics::TextCallback), this, _1, _2);
    webserver->RegisterUrlCallback("/metrics", "common-pre.tmpl", default_callback);

    Webserver::UrlCallback json_callback =
        bind<void>(mem_fn(&Metrics::JsonCallback), this, _1, _2);
    webserver->RegisterUrlCallback("/jsonmetrics", "common-pre.tmpl", json_callback,
        false);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }

  return Status::OK;
}

<<<<<<< HEAD
void Metrics::PrintMetricMap(stringstream* output) {
  lock_guard<mutex> l(lock_);
  BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
    m.second->Print(output);
    (*output) << endl;
  }
}

void Metrics::PrintMetricMapAsJson(vector<string>* metrics) {
  lock_guard<mutex> l(lock_);
  BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
    stringstream ss;
    m.second->PrintJson(&ss);
    metrics->push_back(ss.str());
  }
}

string Metrics::DebugString() {
  stringstream ss;
  TextCallback(&ss);
  return ss.str();
}

void Metrics::TextCallback(stringstream* output) {
  (*output) << "<pre>";
  PrintMetricMap(output);
  (*output) << "</pre>";
}

void Metrics::JsonCallback(stringstream* output) {
  (*output) << "{";
  vector<string> metrics;
  PrintMetricMapAsJson(&metrics);
  (*output) << join(metrics, ",\n");
  (*output) << "}";
=======
string Metrics::DebugString() {
  Webserver::ArgumentMap empty_map;
  Document document;
  document.SetObject();
  TextCallback(empty_map, &document);
  return document["contents"].GetString();
}

string Metrics::DebugStringJson() {
  Webserver::ArgumentMap empty_map;
  Document document;
  document.SetObject();
  JsonCallback(empty_map, &document);
  return document["contents"].GetString();
}

void Metrics::TextCallback(const Webserver::ArgumentMap& args, Document* document) {
  Webserver::ArgumentMap::const_iterator metric_name = args.find("metric");
  lock_guard<mutex> l(lock_);
  stringstream output;
  if (metric_name == args.end()) {
    BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
      m.second->Print(&output);
      output << endl;
    }
  } else {
    MetricMap::const_iterator metric = metric_map_.find(metric_name->second);
    if (metric == metric_map_.end()) {
      Value error(Substitute("Metric '$0' not found", metric_name->second).c_str(),
          document->GetAllocator());
      document->AddMember("error", error, document->GetAllocator());
      return;
    }
    metric->second->Print(&output);
    output << endl;
  }
  Value contents(output.str().c_str(), document->GetAllocator());
  document->AddMember("contents", contents, document->GetAllocator());
}

void Metrics::JsonCallback(const Webserver::ArgumentMap& args, Document* document) {
  Webserver::ArgumentMap::const_iterator metric_name = args.find("metric");
  lock_guard<mutex> l(lock_);
  stringstream output;
  output << "{";
  if (metric_name == args.end()) {
    bool first = true;
    BOOST_FOREACH(const MetricMap::value_type& m, metric_map_) {
      if (first) {
        first = false;
      } else {
        output << ",\n";
      }
      m.second->PrintJson(&output);
    }
  } else {
    MetricMap::const_iterator metric = metric_map_.find(metric_name->second);
    if (metric != metric_map_.end()) {
      metric->second->PrintJson(&output);
      output << endl;
    }
  }
  output << "}";
  Value contents(output.str().c_str(), document->GetAllocator());
  document->AddMember("contents", contents, document->GetAllocator());
  document->AddMember(Webserver::ENABLE_RAW_JSON_KEY, true, document->GetAllocator());
}

namespace impala {

template<> void PrintPrimitiveAsJson<string>(const string& v, stringstream* out) {
  (*out) << "\"" << v << "\"";
}

template<> void PrintPrimitiveAsJson<double>(const double& v, stringstream* out) {
  if (isfinite(v)) {
    (*out) << v;
  } else {
    // This does not call the std::string override, but instead writes
    // the literal null, in keeping with the JSON spec.
    PrintPrimitiveAsJson("null", out);
  }
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
