# Copyright (c) 2024 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

# Only build KVT tests if KVT is enabled and testing is enabled
if(ENABLE_KVT AND ENABLE_TESTING)
  # KVTKeyEncoder test
  nebula_add_test(
    NAME kvt_key_encoder_test
    SOURCES KVTKeyEncoderTest.cpp
    OBJECTS
      $<TARGET_OBJECTS:kvt_storage_client_obj>
      $<TARGET_OBJECTS:kvt_mem_obj>
      $<TARGET_OBJECTS:datatypes_obj>
      $<TARGET_OBJECTS:base_obj>
    LIBRARIES
      gtest
      gtest_main
  )
  
  # KVTTransactionManager test
  nebula_add_test(
    NAME kvt_transaction_manager_test
    SOURCES KVTTransactionManagerTest.cpp
    OBJECTS
      $<TARGET_OBJECTS:kvt_storage_client_obj>
      $<TARGET_OBJECTS:kvt_mem_obj>
      $<TARGET_OBJECTS:base_obj>
    LIBRARIES
      gtest
      gtest_main
  )
  
  # KVTStorageClient integration test (basic)
  nebula_add_test(
    NAME kvt_storage_client_test
    SOURCES KVTStorageClientTest.cpp
    OBJECTS
      $<TARGET_OBJECTS:kvt_storage_client_obj>
      $<TARGET_OBJECTS:kvt_mem_obj>
      $<TARGET_OBJECTS:storage_client_base_obj>
      $<TARGET_OBJECTS:meta_client_obj>
      $<TARGET_OBJECTS:datatypes_obj>
      $<TARGET_OBJECTS:base_obj>
      $<TARGET_OBJECTS:thrift_obj>
    LIBRARIES
      gtest
      gtest_main
      ${THRIFT_LIBRARIES}
      folly
  )
  
  # KVT validation test
  nebula_add_test(
    NAME kvt_validation_test
    SOURCES kvt_validation_test.cpp
    OBJECTS
      $<TARGET_OBJECTS:kvt_mem_obj>
      $<TARGET_OBJECTS:base_obj>
    LIBRARIES
      gtest
      gtest_main
  )
  
  # KVT-NebulaGraph integration test
  nebula_add_test(
    NAME kvt_nebula_integration_test
    SOURCES kvt_nebula_integration_test.cpp
    OBJECTS
      $<TARGET_OBJECTS:kvt_storage_client_obj>
      $<TARGET_OBJECTS:kvt_mem_obj>
      $<TARGET_OBJECTS:storage_client_base_obj>
      $<TARGET_OBJECTS:meta_client_obj>
      $<TARGET_OBJECTS:datatypes_obj>
      $<TARGET_OBJECTS:base_obj>
      $<TARGET_OBJECTS:thrift_obj>
      $<TARGET_OBJECTS:storage_thrift_obj>
      $<TARGET_OBJECTS:meta_thrift_obj>
      $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
      gtest
      gtest_main
      ${THRIFT_LIBRARIES}
      folly
  )
  
  # KVT reverse edge index test
  nebula_add_test(
    NAME kvt_reverse_edge_test
    SOURCES kvt_reverse_edge_test.cpp
    OBJECTS
      $<TARGET_OBJECTS:kvt_storage_client_obj>
      $<TARGET_OBJECTS:kvt_mem_obj>
      $<TARGET_OBJECTS:storage_client_base_obj>
      $<TARGET_OBJECTS:meta_client_obj>
      $<TARGET_OBJECTS:datatypes_obj>
      $<TARGET_OBJECTS:base_obj>
      $<TARGET_OBJECTS:thrift_obj>
      $<TARGET_OBJECTS:storage_thrift_obj>
      $<TARGET_OBJECTS:meta_thrift_obj>
      $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
      gtest
      gtest_main
      ${THRIFT_LIBRARIES}
      folly
  )
  
  # KVT comprehensive test suite
  nebula_add_test(
    NAME kvt_comprehensive_test
    SOURCES kvt_comprehensive_test.cpp
    OBJECTS
      $<TARGET_OBJECTS:kvt_storage_client_obj>
      $<TARGET_OBJECTS:kvt_mem_obj>
      $<TARGET_OBJECTS:storage_client_base_obj>
      $<TARGET_OBJECTS:meta_client_obj>
      $<TARGET_OBJECTS:datatypes_obj>
      $<TARGET_OBJECTS:base_obj>
      $<TARGET_OBJECTS:thrift_obj>
      $<TARGET_OBJECTS:storage_thrift_obj>
      $<TARGET_OBJECTS:meta_thrift_obj>
      $<TARGET_OBJECTS:common_thrift_obj>
    LIBRARIES
      gtest
      gtest_main
      ${THRIFT_LIBRARIES}
      folly
  )
endif()