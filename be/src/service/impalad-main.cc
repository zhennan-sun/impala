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

//
// This file contains the main() function for the impala daemon process,
// which exports the Thrift services ImpalaService and ImpalaInternalService.

#include <unistd.h>
#include <jni.h>
<<<<<<< HEAD
#include <boost/scoped_ptr.hpp>
#include <boost/unordered_map.hpp>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadPoolServer.h>
#include <transport/TServerSocket.h>
#include <server/TServer.h>
#include <transport/TTransportUtils.h>
#include <concurrency/PosixThreadFactory.h>

#include "common/logging.h"
// TODO: fix this: we currently need to include uid-util.h before impala-server.h
#include "util/uid-util.h"
#include "exec/hbase-table-scanner.h"
#include "runtime/hbase-table-cache.h"
=======

#include "common/logging.h"
#include "common/init.h"
#include "exec/hbase-table-scanner.h"
#include "exec/hbase-table-writer.h"
#include "runtime/hbase-table-factory.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "codegen/llvm-codegen.h"
#include "common/status.h"
#include "runtime/coordinator.h"
#include "runtime/exec-env.h"
<<<<<<< HEAD
#include "testutil/test-exec-env.h"
#include "util/cpu-info.h"
#include "util/debug-util.h"
#include "util/disk-info.h"
#include "util/jni-util.h"
#include "util/logging.h"
#include "util/thrift-util.h"
#include "sparrow/subscription-manager.h"
#include "util/thrift-server.h"
#include "common/service-ids.h"
#include "util/authorization.h"
=======
#include "util/jni-util.h"
#include "util/network-util.h"
#include "rpc/thrift-util.h"
#include "rpc/thrift-server.h"
#include "rpc/rpc-trace.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "service/impala-server.h"
#include "service/fe-support.h"
#include "gen-cpp/ImpalaService.h"
#include "gen-cpp/ImpalaInternalService.h"
<<<<<<< HEAD

using namespace impala;
using namespace std;
using namespace sparrow;
using namespace boost;
using namespace apache::thrift::server;
using namespace apache::thrift::protocol;
using namespace apache::thrift::transport;
using namespace apache::thrift::concurrency;

DECLARE_string(classpath);
DECLARE_string(ipaddress);
DECLARE_bool(use_statestore);
DECLARE_int32(fe_port);
DECLARE_int32(be_port);
DECLARE_string(principal);
DECLARE_string(hostname);

int main(int argc, char** argv) {
  // Set the default hostname.  The user can override this with the hostname flag.
  FLAGS_hostname = GetHostname();

  google::ParseCommandLineFlags(&argc, &argv, true);
  InitGoogleLoggingSafe(argv[0]);
  
  // gflags defines -version already to just print the name of the binary (they have
  // a TODO to print the build info) so we can't use that.  Furthermore hadoop also
  // display the version by running 'hadoop version' so we'll just go with that.
  if (argc == 2 && strcmp(argv[1], "version") == 0) {
    cout << GetVersionString() << endl;
    return 0;
  }
  LOG(INFO) << GetVersionString();
  
  LOG(INFO) << "Using hostname: " << FLAGS_hostname;
  
  LogCommandLineFlags();

  InitThriftLogging();
  CpuInfo::Init();
  DiskInfo::Init();
  LlvmCodeGen::InitializeLlvm();
  
  // Enable Kerberos security if requested.
  if (!FLAGS_principal.empty()) {
    EXIT_IF_ERROR(InitKerberos("Impalad"));
  }
  
  JniUtil::InitLibhdfs();
  EXIT_IF_ERROR(JniUtil::Init());
  EXIT_IF_ERROR(HBaseTableScanner::Init());
  EXIT_IF_ERROR(HBaseTableCache::Init());
=======
#include "util/impalad-metrics.h"
#include "util/thread.h"

using namespace impala;
using namespace std;

DECLARE_string(classpath);
DECLARE_bool(use_statestore);
DECLARE_int32(beeswax_port);
DECLARE_int32(hs2_port);
DECLARE_int32(be_port);
DECLARE_string(principal);

int main(int argc, char** argv) {
  InitCommonRuntime(argc, argv, true);

  LlvmCodeGen::InitializeLlvm();
  JniUtil::InitLibhdfs();
  EXIT_IF_ERROR(HBaseTableScanner::Init());
  EXIT_IF_ERROR(HBaseTableFactory::Init());
  EXIT_IF_ERROR(HBaseTableWriter::InitJNI());
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  InitFeSupport();

  // start backend service for the coordinator on be_port
  ExecEnv exec_env;
<<<<<<< HEAD
  ThriftServer* fe_server = NULL;
  ThriftServer* be_server = NULL;
  ImpalaServer* server = 
      CreateImpalaServer(&exec_env, FLAGS_fe_port, FLAGS_be_port, &fe_server, &be_server);
  be_server->Start();

  Status status = exec_env.StartServices();
  if (!status.ok()) {
    LOG(ERROR) << "Impalad services did not start correctly, exiting";
=======
  StartThreadInstrumentation(exec_env.metrics(), exec_env.webserver());
  InitRpcEventTracing(exec_env.webserver());

  ThriftServer* beeswax_server = NULL;
  ThriftServer* hs2_server = NULL;
  ThriftServer* be_server = NULL;
  ImpalaServer* server = NULL;
  EXIT_IF_ERROR(CreateImpalaServer(&exec_env, FLAGS_beeswax_port, FLAGS_hs2_port,
      FLAGS_be_port, &beeswax_server, &hs2_server, &be_server, &server));

  EXIT_IF_ERROR(be_server->Start());

  Status status = exec_env.StartServices();
  if (!status.ok()) {
    LOG(ERROR) << "Impalad services did not start correctly, exiting.  Error: "
               << status.GetErrorMsg();
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
    ShutdownLogging();
    exit(1);
  }

<<<<<<< HEAD
  // register be service *after* starting the be server thread and after starting
  // the subscription mgr handler thread
  scoped_ptr<SubscriptionManager::UpdateCallback> cb;
  if (FLAGS_use_statestore) {
    THostPort host_port;
    host_port.port = FLAGS_be_port;
    host_port.ipaddress = FLAGS_ipaddress;
    host_port.hostname = FLAGS_hostname;
    // TODO: Unregister on tear-down (after impala service changes)
    Status status =
        exec_env.subscription_mgr()->RegisterService(IMPALA_SERVICE_ID, host_port);

    unordered_set<ServiceId> services;
    services.insert(IMPALA_SERVICE_ID);
    cb.reset(new SubscriptionManager::UpdateCallback(
        bind<void>(mem_fn(&ImpalaServer::MembershipCallback), server, _1)));
    exec_env.subscription_mgr()->RegisterSubscription(services, "impala.server", 
        cb.get());
                                                      
    if (!status.ok()) {
      LOG(ERROR) << "Could not register with state store service: "
                 << status.GetErrorMsg();
      ShutdownLogging();
      exit(1);
    }
  }

  // this blocks until the fe server terminates
  fe_server->Start();
  fe_server->Join();

  delete be_server;
  delete fe_server;
=======
  // this blocks until the beeswax and hs2 servers terminate
  EXIT_IF_ERROR(beeswax_server->Start());
  EXIT_IF_ERROR(hs2_server->Start());
  ImpaladMetrics::IMPALA_SERVER_READY->Update(true);
  LOG(INFO) << "Impala has started.";
  beeswax_server->Join();
  hs2_server->Join();

  delete be_server;
  delete beeswax_server;
  delete hs2_server;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
