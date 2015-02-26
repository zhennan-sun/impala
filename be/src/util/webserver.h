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


#ifndef IMPALA_UTIL_WEBSERVER_H
#define IMPALA_UTIL_WEBSERVER_H

<<<<<<< HEAD
#include <mongoose/mongoose.h>
#include <string>
#include <map>
#include <boost/function.hpp>
#include <boost/thread/mutex.hpp>

#include "common/status.h"

namespace impala {

// Wrapper class for the Mongoose web server library. Clients may register callback
// methods which produce output for a given URL path
class Webserver {
 public:
  typedef boost::function<void (std::stringstream* output)> PathHandlerCallback;

  // If interface is set to the empty string the socket will bind to all available
  // interfaces.
  Webserver(const std::string& interface, const int port);
=======
#include <squeasel/squeasel.h>
#include <string>
#include <map>
#include <boost/function.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <rapidjson/document.h>

#include "common/status.h"
#include "util/network-util.h"

namespace impala {

// Wrapper class for the Squeasel web server library. Clients may register callback
// methods which produce Json documents which are rendered via a template file to either
// HTML or text.
class Webserver {
 public:
  typedef std::map<std::string, std::string> ArgumentMap;
  typedef boost::function<void (const ArgumentMap& args, rapidjson::Document* json)>
      UrlCallback;

  // Any callback may add a member to their Json output with key ENABLE_RAW_JSON_KEY; this
  // causes the result of the template rendering process to be sent to the browser as
  // text, not HTML.
  static const char* ENABLE_RAW_JSON_KEY;

  // Using this constructor, the webserver will bind to all available interfaces.
  Webserver(const int port);
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa

  // Uses FLAGS_webserver_{port, interface}
  Webserver();

  ~Webserver();

  // Starts a webserver on the port passed to the constructor. The webserver runs in a
  // separate thread, so this call is non-blocking.
  Status Start();

  // Stops the webserver synchronously.
  void Stop();

<<<<<<< HEAD
  // Register a handler with a particular URL path. Path should not include the
  // http://hostname/ prefix.
  void RegisterPathHandler(const std::string& path, const PathHandlerCallback& callback);

 private:
  // Static so that it can act as a function pointer, and then call the next method
  static void* MongooseCallbackStatic(enum mg_event event, 
      struct mg_connection* connection);

  // Dispatch point for all incoming requests.
  void* MongooseCallback(enum mg_event event, struct mg_connection* connection,
      const struct mg_request_info* request_info);

  // Registered to handle "/", and prints a list of available URIs
  void RootHandler(std::stringstream* output);

  // Lock guarding the path_handlers_ map
  boost::mutex path_handlers_lock_;

  // Map of path to a list of handlers. More than one handler may register itself with a
  // path so that many components may contribute to a single page.
  typedef std::map<std::string, std::vector<PathHandlerCallback> > PathHandlerMap;
  PathHandlerMap path_handlers_;

  const int port_;
  // If empty, webserver will bind to all interfaces.
  const std::string& interface_;

  // Handle to Mongoose context; owned and freed by Mongoose internally
  struct mg_context* context_;
=======
  // Register a callback for a Url that produces a json document that will be rendered
  // with the template at 'template_filename'. The URL 'path' should not include the
  // http://hostname/ prefix. If is_on_nav_bar is true, the page will appear in the
  // standard navigation bar rendered on all pages.
  //
  // Only one callback may be registered per URL.
  //
  // The path of the template file is relative to the webserver's document
  // root.
  void RegisterUrlCallback(const std::string& path, const std::string& template_filename,
      const UrlCallback& callback, bool is_on_nav_bar = true);

  const TNetworkAddress& http_address() { return http_address_; }

  // True if serving all traffic over SSL, false otherwise
  bool IsSecure() const;

 private:
  // Contains all information relevant to rendering one Url. Each Url has one callback
  // that produces the output to render. The callback either produces a Json document
  // which is rendered via a template file, or it produces an HTML string that is embedded
  // directly into the output.
  class UrlHandler {
   public:
    UrlHandler(const UrlCallback& cb, const std::string& template_filename,
        bool is_on_nav_bar)
        : is_on_nav_bar_(is_on_nav_bar), template_callback_(cb),
          template_filename_(template_filename) { }

    bool is_on_nav_bar() const { return is_on_nav_bar_; }
    const UrlCallback& callback() const { return template_callback_; }
    const std::string& template_filename() const { return template_filename_; }

   private:
    // If true, the page appears in the navigation bar.
    bool is_on_nav_bar_;

    // Callback to produce a Json document to render via a template.
    UrlCallback template_callback_;

    // Path to the file that contains the template to render, relative to the webserver's
    // document root.
    std::string template_filename_;
  };

  // Squeasel callback for log events. Returns squeasel success code.
  static int LogMessageCallbackStatic(const struct sq_connection* connection,
      const char* message);

  // Squeasel callback for HTTP request events. Static so that it can act as a function
  // pointer, and then call the next method. Returns squeasel success code.
  static int BeginRequestCallbackStatic(struct sq_connection* connection);

  // Dispatch point for all incoming requests. Returns squeasel success code.
  int BeginRequestCallback(struct sq_connection* connection,
      struct sq_request_info* request_info);

  // Registered to handle "/", populates document with various system-wide information.
  void RootHandler(const ArgumentMap& args, rapidjson::Document* document);

  // Called when an error is encountered, e.g. when a handler for a URI cannot be found.
  void ErrorHandler(const ArgumentMap& args, rapidjson::Document* document);

  // Builds a map of argument name to argument value from a typical URL argument
  // string (that is, "key1=value1&key2=value2.."). If no value is given for a
  // key, it is entered into the map as (key, "").
  void BuildArgumentMap(const std::string& args, ArgumentMap* output);

  // Adds a __common__ object to document with common data that every webpage might want
  // to read (e.g. the names of links to write to the navbar).
  void GetCommonJson(rapidjson::Document* document);

  // Lock guarding the path_handlers_ map
  boost::shared_mutex url_handlers_lock_;

  // Map of path to a UrlHandler containing a list of handlers for that
  // path. More than one handler may register itself with a path so that many
  // components may contribute to a single page.
  typedef std::map<std::string, UrlHandler> UrlHandlerMap;
  UrlHandlerMap url_handlers_;

  // The address of the interface on which to run this webserver.
  TNetworkAddress http_address_;

  // Handle to Squeasel context; owned and freed by Squeasel internally
  struct sq_context* context_;

  // Catch-all handler for error messages
  UrlHandler error_handler_;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
};

}

#endif // IMPALA_UTIL_WEBSERVER_H
