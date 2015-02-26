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

#include "runtime/client-cache.h"

#include <sstream>
<<<<<<< HEAD
#include <server/TServer.h>
#include <protocol/TBinaryProtocol.h>
#include <transport/TSocket.h>
#include <transport/TTransportUtils.h>
=======
#include <thrift/server/TServer.h>
#include <thrift/protocol/TBinaryProtocol.h>
#include <thrift/transport/TSocket.h>
#include <thrift/transport/TTransportUtils.h>
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include <memory>

#include <boost/foreach.hpp>

#include "common/logging.h"
<<<<<<< HEAD
#include "util/thrift-util.h"
=======
#include "util/container-util.h"
#include "util/network-util.h"
#include "rpc/thrift-util.h"
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
#include "gen-cpp/ImpalaInternalService.h"

using namespace std;
using namespace boost;
using namespace apache::thrift;
using namespace apache::thrift::server;
using namespace apache::thrift::transport;
using namespace apache::thrift::protocol;

namespace impala {

<<<<<<< HEAD
BackendClientCache::BackendClientCache(int max_clients, int max_clients_per_backend)
  : max_clients_(max_clients),
    max_clients_per_backend_(max_clients_per_backend) {
}

Status BackendClientCache::GetClient(
    const pair<string, int>& hostport, ImpalaInternalServiceClient** client) {
  VLOG_RPC << "GetClient("
           << hostport.first << ":" << hostport.second << ")";
  lock_guard<mutex> l(lock_);
  ClientCache::iterator cache_entry = client_cache_.find(hostport);
  if (cache_entry == client_cache_.end()) {
    cache_entry =
        client_cache_.insert(make_pair(hostport, list<BackendClient*>())).first;
    DCHECK(cache_entry != client_cache_.end());
  }

  list<BackendClient*>& info_list = cache_entry->second;
  if (!info_list.empty()) {
    *client = info_list.front()->iface();
    VLOG_RPC << "GetClient(): adding client for "
             << info_list.front()->ipaddress()
             << ":" << info_list.front()->port();
    info_list.pop_front();
  } else {
    auto_ptr<BackendClient> info(
        new BackendClient(cache_entry->first.first, cache_entry->first.second));
    RETURN_IF_ERROR(info->Open());
    client_map_[info->iface()] = info.get();
    VLOG_CONNECTION << "GetClient(): creating client for "
                    << info->ipaddress() << ":" << info->port();
    *client = info.release()->iface();
  }

  return Status::OK;
}

Status BackendClientCache::ReopenClient(ImpalaInternalServiceClient* client) {
  lock_guard<mutex> l(lock_);
  ClientMap::iterator i = client_map_.find(client);
  DCHECK(i != client_map_.end());
  BackendClient* info = i->second;
  RETURN_IF_ERROR(info->Close());
  RETURN_IF_ERROR(info->Open());
  return Status::OK;
}

void BackendClientCache::ReleaseClient(ImpalaInternalServiceClient* client) {
  lock_guard<mutex> l(lock_);
  ClientMap::iterator i = client_map_.find(client);
  DCHECK(i != client_map_.end());
  BackendClient* info = i->second;
  VLOG_RPC << "releasing client for "
           << info->ipaddress() << ":" << info->port();
  ClientCache::iterator j =
      client_cache_.find(make_pair(info->ipaddress(), info->port()));
  DCHECK(j != client_cache_.end());
  j->second.push_back(info);
}

void BackendClientCache::CloseConnections(const pair<string, int>& hostport) {
  lock_guard<mutex> l(lock_);
  ClientCache::iterator cache_entry = client_cache_.find(hostport);
  if (cache_entry == client_cache_.end()) return;
  VLOG_RPC << "Invalidating all " << cache_entry->second.size() << " clients for: " 
           << hostport.first << ":" << hostport.second;
  BOOST_FOREACH(BackendClient* client, cache_entry->second) {
    client->Close();
  }
}

string BackendClientCache::DebugString() {
  lock_guard<mutex> l(lock_);
  stringstream out;
  out << "BackendClientCache(#hosts=" << client_cache_.size()
      << " [";
  for (ClientCache::iterator i = client_cache_.begin(); i != client_cache_.end(); ++i) {
    if (i != client_cache_.begin()) out << " ";
    out << i->first.first << ":" << i->first.second << ":" << i->second.size();
=======
Status ClientCacheHelper::GetClient(const TNetworkAddress& address,
    ClientFactory factory_method, ClientKey* client_key) {
  shared_ptr<PerHostCache> host_cache;
  {
    lock_guard<mutex> lock(cache_lock_);
    VLOG(2) << "GetClient(" << address << ")";
    shared_ptr<PerHostCache>* ptr = &per_host_caches_[address];
    if (ptr->get() == NULL) ptr->reset(new PerHostCache());
    host_cache = *ptr;
  }

  {
    lock_guard<mutex> lock(host_cache->lock);
    if (!host_cache->clients.empty()) {
      *client_key = host_cache->clients.front();
      VLOG(2) << "GetClient(): returning cached client for " << address;
      host_cache->clients.pop_front();
      if (metrics_enabled_) clients_in_use_metric_->Increment(1);
      return Status::OK;
    }
  }

  // Only get here if host_cache->clients.empty(). No need for the lock.
  RETURN_IF_ERROR(CreateClient(address, factory_method, client_key));
  if (metrics_enabled_) clients_in_use_metric_->Increment(1);
  return Status::OK;
}

Status ClientCacheHelper::ReopenClient(ClientFactory factory_method,
    ClientKey* client_key) {
  // Clients are not ordinarily removed from the cache completely (in the future, they may
  // be); this is the only method where a client may be deleted and replaced with another.
  shared_ptr<ThriftClientImpl> client_impl;
  ClientMap::iterator client;
  {
    lock_guard<mutex> lock(client_map_lock_);
    client = client_map_.find(*client_key);
    DCHECK(client != client_map_.end());
    client_impl = client->second;
  }
  VLOG(1) << "ReopenClient(): re-creating client for " << client_impl->address();

  client_impl->Close();

  // TODO: Thrift TBufferedTransport cannot be re-opened after Close() because it does not
  // clean up internal buffers it reopens. To work around this issue, create a new client
  // instead.
  ClientKey* old_client_key = client_key;
  if (metrics_enabled_) total_clients_metric_->Increment(-1);
  Status status = CreateClient(client_impl->address(), factory_method, client_key);
  // Only erase the existing client from the map if creation of the new one succeeded.
  // This helps to ensure the proper accounting of metrics in the presence of
  // re-connection failures (the original client should be released as usual).
  if (status.ok()) {
    lock_guard<mutex> lock(client_map_lock_);
    client_map_.erase(client);
  } else {
    // Restore the client used before the failed re-opening attempt, so the caller can
    // properly release it.
    *client_key = *old_client_key;
  }
  return status;
}

Status ClientCacheHelper::CreateClient(const TNetworkAddress& address,
    ClientFactory factory_method, ClientKey* client_key) {
  shared_ptr<ThriftClientImpl> client_impl(factory_method(address, client_key));
  VLOG(2) << "CreateClient(): creating new client for " << client_impl->address();
  Status status = client_impl->OpenWithRetry(num_tries_, wait_ms_);
  if (!status.ok()) {
    *client_key = NULL;
    return status;
  }
  // Set the TSocket's send and receive timeouts.
  client_impl->setRecvTimeout(recv_timeout_ms_);
  client_impl->setSendTimeout(send_timeout_ms_);

  // Because the client starts life 'checked out', we don't add it to its host cache.
  {
    lock_guard<mutex> lock(client_map_lock_);
    client_map_[*client_key] = client_impl;
  }

  if (metrics_enabled_) total_clients_metric_->Increment(1);
  return Status::OK;
}

void ClientCacheHelper::ReleaseClient(ClientKey* client_key) {
  DCHECK(*client_key != NULL) << "Trying to release NULL client";
  shared_ptr<ThriftClientImpl> client_impl;
  {
    lock_guard<mutex> lock(client_map_lock_);
    ClientMap::iterator client = client_map_.find(*client_key);
    DCHECK(client != client_map_.end());
    client_impl = client->second;
  }
  VLOG(2) << "Releasing client for " << client_impl->address() << " back to cache";
  {
    lock_guard<mutex> lock(cache_lock_);
    PerHostCacheMap::iterator cache = per_host_caches_.find(client_impl->address());
    DCHECK(cache != per_host_caches_.end());
    lock_guard<mutex> entry_lock(cache->second->lock);
    cache->second->clients.push_back(*client_key);
  }
  if (metrics_enabled_) clients_in_use_metric_->Increment(-1);
  *client_key = NULL;
}

void ClientCacheHelper::CloseConnections(const TNetworkAddress& address) {
  PerHostCache* cache;
  {
    lock_guard<mutex> lock(cache_lock_);
    PerHostCacheMap::iterator cache_it = per_host_caches_.find(address);
    if (cache_it == per_host_caches_.end()) return;
    cache = cache_it->second.get();
  }

  {
    VLOG(2) << "Invalidating all " << cache->clients.size() << " clients for: "
            << address;
    lock_guard<mutex> entry_lock(cache->lock);
    lock_guard<mutex> map_lock(client_map_lock_);
    BOOST_FOREACH(ClientKey client_key, cache->clients) {
      ClientMap::iterator client_map_entry = client_map_.find(client_key);
      DCHECK(client_map_entry != client_map_.end());
      client_map_entry->second->Close();
    }
  }
}

string ClientCacheHelper::DebugString() {
  lock_guard<mutex> lock(cache_lock_);
  stringstream out;
  out << "ClientCacheHelper(#hosts=" << per_host_caches_.size()
      << " [";
  bool first = true;
  BOOST_FOREACH(const PerHostCacheMap::value_type& cache, per_host_caches_) {
    lock_guard<mutex> host_cache_lock(cache.second->lock);
    if (!first) out << " ";
    out << cache.first << ":" << cache.second->clients.size();
    first = false;
>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
  }
  out << "])";
  return out.str();
}

<<<<<<< HEAD
=======
void ClientCacheHelper::TestShutdown() {
  vector<TNetworkAddress> addresses;
  {
    lock_guard<mutex> lock(cache_lock_);
    BOOST_FOREACH(const PerHostCacheMap::value_type& cache_entry, per_host_caches_) {
      addresses.push_back(cache_entry.first);
    }
  }
  BOOST_FOREACH(const TNetworkAddress& address, addresses) {
    CloseConnections(address);
  }
}

void ClientCacheHelper::InitMetrics(Metrics* metrics, const string& key_prefix) {
  DCHECK(metrics != NULL);
  // Not strictly needed if InitMetrics is called before any cache usage, but ensures that
  // metrics_enabled_ is published.
  lock_guard<mutex> lock(cache_lock_);
  stringstream count_ss;
  count_ss << key_prefix << ".client-cache.clients-in-use";
  clients_in_use_metric_ =
      metrics->CreateAndRegisterPrimitiveMetric(count_ss.str(), 0L);

  stringstream max_ss;
  max_ss << key_prefix << ".client-cache.total-clients";
  total_clients_metric_ = metrics->CreateAndRegisterPrimitiveMetric(max_ss.str(), 0L);
  metrics_enabled_ = true;
}

>>>>>>> d520a9cdea2fc97e8d5da9fbb0244e60ee416bfa
}
