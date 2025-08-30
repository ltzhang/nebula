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


nebula_add_library(
    mem_store_obj OBJECT
    MemStore.cpp
)


nebula_add_library(
    mem_storage_client_obj OBJECT
    MemStorageClient.cpp
)


nebula_add_library(
    kvt_impl_obj OBJECT
    kvt/kvt_impl.cpp
    kvt/kvt_mem.cpp
)

nebula_add_library(
    kvt_store_obj OBJECT
    KvtStore.cpp
)


nebula_add_library(
    kvt_storage_client_obj OBJECT
    KvtStorageClient.cpp
)


# Optional test executable for verifying the storage client switch
if(ENABLE_TESTING)
    nebula_add_executable(
        NAME
            storage_client_switch_test
        SOURCES
            StorageClientSwitchTest.cpp
        OBJECTS
            $<TARGET_OBJECTS:mem_store_obj>
            $<TARGET_OBJECTS:mem_storage_client_obj>
            $<TARGET_OBJECTS:storage_client_obj>
            $<TARGET_OBJECTS:storage_client_base_obj>
        LIBRARIES
            ${THRIFT_LIBRARIES}
            ${PROXYGEN_LIBRARIES}
            wangle
            glog
            gflags
            folly
    )
endif()

nebula_add_subdirectory(stats)

