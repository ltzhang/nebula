# Copyright (c) 2020 vesoft inc. All rights reserved.
#
# This source code is licensed under Apache 2.0 License.

nebula_add_library(
    storage_client_obj OBJECT
    StorageClient.cpp
)


nebula_add_library(
    storage_client_base_obj OBJECT
    StorageClientBase.cpp
)


nebula_add_library(
    internal_storage_client_obj OBJECT
    InternalStorageClient.cpp
)

nebula_add_subdirectory(stats)

# KVT Storage Client support
option(ENABLE_KVT "Enable KVT storage backend" ON)

if(ENABLE_KVT)
  # Compile kvt_mem.cpp to object file
  add_library(kvt_mem_obj OBJECT kvt/kvt_mem.cpp)
  # Disable warnings for reference implementation
  target_compile_options(kvt_mem_obj PRIVATE 
    -Wno-unused-parameter 
    -Wno-reorder 
    -Wno-shadow 
    -Wno-sign-compare)
  
  nebula_add_library(
    kvt_storage_client_obj OBJECT
    kvt/KVTStorageClient.cpp
    kvt/KVTKeyEncoder.cpp
    kvt/KVTTransactionManager.cpp
    kvt/KVTValueEncoder.cpp
  )
  
  # Disable warnings for prototype implementation
  target_compile_options(kvt_storage_client_obj PRIVATE
    -Wno-unused-parameter
    -Wno-unused-variable  
    -Wno-deprecated-declarations
    -Wno-shadow
    -Wno-overflow)
  
  # Link with kvt_mem implementation
  target_link_libraries(kvt_storage_client_obj
    PRIVATE
    kvt_mem_obj
  )
  
  message(STATUS "KVT storage backend enabled")
endif()

# Add KVT tests subdirectory (will be built when KVT is enabled and testing is on)
nebula_add_subdirectory(kvt/test)

