# Copyright (c) 2024 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

if(ENABLE_KVT)
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
endif()