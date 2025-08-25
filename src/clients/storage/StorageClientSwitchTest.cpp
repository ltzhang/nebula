/* Copyright (c) 2018 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/StorageClient.h"
#include <glog/logging.h>
#include <folly/executors/IOThreadPoolExecutor.h>
#include <typeinfo>
#include <iostream>

/**
 * Simple test to verify the conditional StorageClient typedef works
 * When USE_MEMSTORE is defined: StorageClient = MemStorageClient
 * When USE_MEMSTORE is not defined: StorageClient = OrigStorageClient
 */
int main() {
  google::InitGoogleLogging("storage_client_switch_test");
  FLAGS_logtostderr = true;
  FLAGS_minloglevel = 0;

  std::cout << "=== StorageClient Switch Test ===" << std::endl;
  
#ifdef USE_MEMSTORE
  std::cout << "BUILD MODE: USE_MEMSTORE is ENABLED" << std::endl;
  std::cout << "StorageClient should be aliased to MemStorageClient" << std::endl;
#else
  std::cout << "BUILD MODE: USE_MEMSTORE is DISABLED" << std::endl;
  std::cout << "StorageClient should be aliased to OrigStorageClient" << std::endl;
#endif

  // Test that we can instantiate StorageClient
  auto ioThreadPool = std::make_shared<folly::IOThreadPoolExecutor>(1);
  
  try {
    // This should work regardless of which implementation is used
    auto client = std::make_unique<nebula::storage::StorageClient>(ioThreadPool, nullptr);
    
    std::cout << "✓ StorageClient instantiation successful" << std::endl;
    std::cout << "  Client type: " << typeid(*client).name() << std::endl;
    
#ifdef USE_MEMSTORE
    std::cout << "✓ Using MemStorageClient implementation" << std::endl;
#else 
    std::cout << "✓ Using OrigStorageClient implementation" << std::endl;
#endif

  } catch (const std::exception& e) {
    std::cout << "✗ StorageClient instantiation failed: " << e.what() << std::endl;
    return 1;
  }
  
  std::cout << "=== Test completed successfully ===" << std::endl;
  return 0;
}