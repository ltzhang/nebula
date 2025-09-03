/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "graph/service/QueryEngine.h"

#include "common/base/Base.h"
#include "common/memory/MemoryUtils.h"
#include "common/meta/ServerBasedIndexManager.h"
#include "common/meta/ServerBasedSchemaManager.h"
#include "graph/context/QueryContext.h"
#include "graph/optimizer/OptRule.h"
#include "graph/planner/PlannersRegister.h"
#include "graph/service/GraphFlags.h"
#include "graph/service/QueryInstance.h"
#include "version/Version.h"

// Include KVT storage client if enabled
#ifdef ENABLE_KVT
#include "clients/storage/kvt/KVTStorageClient.h"
#endif

DECLARE_bool(local_config);
DECLARE_bool(enable_optimizer);
DECLARE_string(meta_server_addrs);
DECLARE_bool(enable_kvt_storage);
DEFINE_int32(check_memory_interval_in_secs, 1, "Memory check interval in seconds");

namespace nebula {
namespace graph {

Status QueryEngine::init(std::shared_ptr<folly::IOThreadPoolExecutor> ioExecutor,
                         meta::MetaClient* metaClient) {
  metaClient_ = metaClient;
  schemaManager_ = meta::ServerBasedSchemaManager::create(metaClient_);
  indexManager_ = meta::ServerBasedIndexManager::create(metaClient_);
  
  // Choose storage backend based on configuration (default: KVT)
  if (FLAGS_enable_kvt_storage) {
#ifdef ENABLE_KVT
    LOG(INFO) << "Using KVT storage backend (default)";
    auto kvtStorage = std::make_unique<storage::KVTStorageClient>(ioExecutor, metaClient_);
    auto status = kvtStorage->init();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize KVT storage: " << status.toString();
      return status;
    }
    storage_ = std::move(kvtStorage);
#else
    LOG(ERROR) << "KVT storage requested but not compiled in. Please compile with -DENABLE_KVT=ON";
    return Status::Error("KVT storage not available");
#endif
  } else {
    LOG(INFO) << "Using distributed storage backend (--enable_kvt_storage=false)";
    storage_ = std::make_unique<storage::StorageClient>(ioExecutor, metaClient_);
  }
  
  charsetInfo_ = CharsetInfo::instance();

  PlannersRegister::registerPlanners();

  // Set default optimizer rules
  std::vector<const opt::RuleSet*> rulesets{&opt::RuleSet::DefaultRules()};
  if (FLAGS_enable_optimizer) {
    rulesets.emplace_back(&opt::RuleSet::QueryRules0());
    rulesets.emplace_back(&opt::RuleSet::QueryRules());
  }
  optimizer_ = std::make_unique<opt::Optimizer>(rulesets);

  return setupMemoryMonitorThread();
}

// Create query context and query instance and execute it
void QueryEngine::execute(RequestContextPtr rctx) {
  auto qctx = std::make_unique<QueryContext>(std::move(rctx),
                                             schemaManager_.get(),
                                             indexManager_.get(),
                                             storage_.get(),
                                             metaClient_,
                                             charsetInfo_);
  auto* instance = new QueryInstance(std::move(qctx), optimizer_.get());
  instance->execute();
}

Status QueryEngine::setupMemoryMonitorThread() {
  memoryMonitorThread_ = std::make_unique<thread::GenericWorker>();
  if (!memoryMonitorThread_ || !memoryMonitorThread_->start("graph-memory-monitor")) {
    return Status::Error("Fail to start query engine background thread.");
  }

  auto updateMemoryWatermark = []() -> Status {
    auto status = memory::MemoryUtils::hitsHighWatermark();
    NG_RETURN_IF_ERROR(status);
    memory::MemoryUtils::kHitMemoryHighWatermark.store(std::move(status).value());
    return Status::OK();
  };

  // Just to test whether to get the right memory info
  NG_RETURN_IF_ERROR(updateMemoryWatermark());

  auto ms = FLAGS_check_memory_interval_in_secs * 1000;
  memoryMonitorThread_->addRepeatTask(ms, updateMemoryWatermark);

  return Status::OK();
}

}  // namespace graph
}  // namespace nebula
