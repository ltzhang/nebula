/* Copyright (c) 2024 vesoft inc. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License.
 */

#include "clients/storage/kvt/KVTStorageClient.h"
#include "clients/storage/kvt/KVTKeyEncoder.h"
#include "clients/storage/kvt/KVTTransactionManager.h"
#include "clients/storage/kvt/KVTValueEncoder.h"
#include "common/base/Logging.h"
#include "common/datatypes/DataSet.h"
#include "common/datatypes/Value.h"

namespace nebula {
namespace storage {

cpp2::RequestCommon KVTStorageClient::CommonRequestParam::toReqCommon() const {
  cpp2::RequestCommon common;
  // Use new API style
  common.session_id_ref() = session;
  common.plan_id_ref() = plan;
  common.profile_detail_ref() = profile;
  return common;
}

KVTStorageClient::KVTStorageClient(
    std::shared_ptr<folly::IOThreadPoolExecutor> ioThreadPool,
    meta::MetaClient* metaClient)
    : StorageClientBase<cpp2::GraphStorageServiceAsyncClient,
                       thrift::ThriftClientManager<cpp2::GraphStorageServiceAsyncClient>>(
          ioThreadPool, metaClient),
      ioThreadPool_(ioThreadPool) {
  LOG(INFO) << "Creating KVTStorageClient";
}

KVTStorageClient::~KVTStorageClient() {
  if (kvtInitialized_) {
    LOG(INFO) << "Shutting down KVT system";
    kvt_shutdown();
  }
}

Status KVTStorageClient::init() {
  if (kvtInitialized_) {
    return Status::OK();
  }

  KVTError err = kvt_initialize();
  if (err != KVTError::SUCCESS) {
    return Status::Error("Failed to initialize KVT system");
  }
  
  kvtInitialized_ = true;
  LOG(INFO) << "KVT system initialized successfully";
  return Status::OK();
}

Status KVTStorageClient::createSpaceTables(GraphSpaceID space) {
  std::string error;
  uint64_t tableId;
  
  // Create vertex table
  std::string vertexTableName = "vertices_space_" + std::to_string(space);
  KVTError err = kvt_create_table(vertexTableName, "hash", tableId, error);
  if (err == KVTError::SUCCESS) {
    vertexTables_[space] = tableId;
    LOG(INFO) << "Created vertex table for space " << space << " with id " << tableId;
  } else if (err != KVTError::TABLE_ALREADY_EXISTS) {
    return Status::Error("Failed to create vertex table: " + error);
  } else {
    // Table already exists, get its ID
    err = kvt_get_table_id(vertexTableName, tableId, error);
    if (err != KVTError::SUCCESS) {
      return Status::Error("Failed to get vertex table ID: " + error);
    }
    vertexTables_[space] = tableId;
  }
  
  // Create edge table
  std::string edgeTableName = "edges_space_" + std::to_string(space);
  err = kvt_create_table(edgeTableName, "hash", tableId, error);
  if (err == KVTError::SUCCESS) {
    edgeTables_[space] = tableId;
    LOG(INFO) << "Created edge table for space " << space << " with id " << tableId;
  } else if (err != KVTError::TABLE_ALREADY_EXISTS) {
    return Status::Error("Failed to create edge table: " + error);
  } else {
    // Table already exists, get its ID
    err = kvt_get_table_id(edgeTableName, tableId, error);
    if (err != KVTError::SUCCESS) {
      return Status::Error("Failed to get edge table ID: " + error);
    }
    edgeTables_[space] = tableId;
  }
  
  // Create index table
  std::string indexTableName = "indices_space_" + std::to_string(space);
  err = kvt_create_table(indexTableName, "range", tableId, error);
  if (err == KVTError::SUCCESS) {
    indexTables_[space] = tableId;
    LOG(INFO) << "Created index table for space " << space << " with id " << tableId;
  } else if (err != KVTError::TABLE_ALREADY_EXISTS) {
    return Status::Error("Failed to create index table: " + error);
  } else {
    // Table already exists, get its ID
    err = kvt_get_table_id(indexTableName, tableId, error);
    if (err != KVTError::SUCCESS) {
      return Status::Error("Failed to get index table ID: " + error);
    }
    indexTables_[space] = tableId;
  }
  
  return Status::OK();
}

uint64_t KVTStorageClient::getVertexTableId(GraphSpaceID space) {
  auto it = vertexTables_.find(space);
  if (it != vertexTables_.end()) {
    return it->second;
  }
  
  // Try to create tables if not exist
  auto status = createSpaceTables(space);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create tables for space " << space;
    return 0;
  }
  
  return vertexTables_[space];
}

uint64_t KVTStorageClient::getEdgeTableId(GraphSpaceID space) {
  auto it = edgeTables_.find(space);
  if (it != edgeTables_.end()) {
    return it->second;
  }
  
  // Try to create tables if not exist
  auto status = createSpaceTables(space);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create tables for space " << space;
    return 0;
  }
  
  return edgeTables_[space];
}

uint64_t KVTStorageClient::getIndexTableId(GraphSpaceID space) {
  auto it = indexTables_.find(space);
  if (it != indexTables_.end()) {
    return it->second;
  }
  
  // Try to create tables if not exist
  auto status = createSpaceTables(space);
  if (!status.ok()) {
    LOG(ERROR) << "Failed to create tables for space " << space;
    return 0;
  }
  
  return indexTables_[space];
}

// Placeholder implementations for now - will be implemented in subsequent phases
StorageRpcRespFuture<cpp2::GetNeighborsResponse> KVTStorageClient::getNeighbors(
    const CommonRequestParam& param,
    std::vector<std::string> colNames,
    const std::vector<Value>& vids,
    const std::vector<EdgeType>& edgeTypes,
    cpp2::EdgeDirection edgeDirection,
    const std::vector<cpp2::StatProp>* statProps,
    const std::vector<cpp2::VertexProp>* vertexProps,
    const std::vector<cpp2::EdgeProp>* edgeProps,
    const std::vector<cpp2::Expr>* expressions,
    bool dedup,
    bool random,
    const std::vector<cpp2::OrderBy>& orderBy,
    int64_t limit,
    const Expression* filter,
    const Expression* tagFilter) {
  
  LOG(INFO) << "getNeighbors called for space " << param.space 
            << " with " << vids.size() << " vertices";
  
  // Initialize KVT if not already done
  if (!kvtInitialized_) {
    auto status = init();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize KVT: " << status.toString();
      StorageRpcResponse<cpp2::GetNeighborsResponse> resp(1);
      resp.markFailure();
      return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(std::move(resp));
    }
  }
  
  // Get table IDs for this space
  uint64_t vertexTableId = getVertexTableId(param.space);
  uint64_t edgeTableId = getEdgeTableId(param.space);
  
  if (vertexTableId == 0 || edgeTableId == 0) {
    LOG(ERROR) << "Failed to get table IDs for space " << param.space;
    StorageRpcResponse<cpp2::GetNeighborsResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(std::move(resp));
  }
  
  // Create transaction manager
  KVTTransactionManager txManager;
  
  // Start transaction for consistent reads
  auto txResult = txManager.startTransaction();
  if (!txResult.ok()) {
    LOG(ERROR) << "Failed to start transaction: " << txResult.status().toString();
    StorageRpcResponse<cpp2::GetNeighborsResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(std::move(resp));
  }
  
  auto tx = std::move(txResult).value();
  uint64_t txId = tx->getId();
  
  // Prepare response
  StorageRpcResponse<cpp2::GetNeighborsResponse> rpcResp(1);
  cpp2::GetNeighborsResponse getNeighborsResp;
  
  // Process each vertex
  DataSet resultDataSet;
  resultDataSet.colNames = colNames;
  
  for (const auto& vid : vids) {
    PartitionID partId = 0;  // TODO: Calculate proper partition
    
    // Scan for edges based on direction
    std::vector<std::pair<std::string, std::string>> edgeResults;
    
    if (edgeDirection == cpp2::EdgeDirection::OUT_EDGE || 
        edgeDirection == cpp2::EdgeDirection::BOTH) {
      // Scan outgoing edges
      for (EdgeType edgeType : edgeTypes) {
        std::string edgePrefix = KVTKeyEncoder::edgePrefix(
            param.space, partId, &vid, edgeType);
        std::string scanStart = edgePrefix;
        std::string scanEnd = edgePrefix;
        scanEnd.push_back(0xFF);
        
        std::vector<std::pair<std::string, std::string>> outEdges;
        std::string error;
        KVTError err = kvt_scan(txId, edgeTableId, scanStart, scanEnd, 
                                limit > 0 ? limit : 10000, outEdges, error);
        
        if (err == KVTError::SUCCESS) {
          edgeResults.insert(edgeResults.end(), outEdges.begin(), outEdges.end());
        }
      }
    }
    
    if (edgeDirection == cpp2::EdgeDirection::IN_EDGE || 
        edgeDirection == cpp2::EdgeDirection::BOTH) {
      // Use reverse edge index to find incoming edges
      for (EdgeType edgeType : edgeTypes) {
        std::string reversePrefix = KVTKeyEncoder::reverseEdgePrefix(
            param.space, partId, &vid, edgeType);
        std::string scanStart = reversePrefix;
        std::string scanEnd = reversePrefix;
        scanEnd.push_back(0xFF);
        
        std::vector<std::pair<std::string, std::string>> inEdges;
        std::string error;
        KVTError err = kvt_scan(txId, edgeTableId, scanStart, scanEnd,
                                limit > 0 ? limit : 10000, inEdges, error);
        
        if (err == KVTError::SUCCESS) {
          // For IN_EDGE, we need to transform the reverse edge back
          for (const auto& [reverseKey, value] : inEdges) {
            // Decode reverse edge to get the actual edge info
            GraphSpaceID spaceId;
            PartitionID partId;
            Value dstId, srcId;
            EdgeType edgeType;
            EdgeRanking ranking;
            
            if (KVTKeyEncoder::decodeReverseEdgeKey(reverseKey, spaceId, partId,
                                                    dstId, edgeType, ranking, srcId)) {
              // Reconstruct the forward edge key for consistency
              std::string forwardKey = KVTKeyEncoder::encodeEdgeKey(
                  spaceId, partId, srcId, edgeType, ranking, dstId);
              edgeResults.push_back({forwardKey, value});
            }
          }
        }
      }
    }
    
    // Process edge results
    for (const auto& [edgeKey, edgeValue] : edgeResults) {
      // Decode edge key to get destination vertex
      GraphSpaceID spaceId;
      PartitionID partId;
      Value srcId, dstId;
      EdgeType edgeType;
      EdgeRanking ranking;
      
      if (KVTKeyEncoder::decodeEdgeKey(edgeKey, spaceId, partId, 
                                       srcId, edgeType, ranking, dstId)) {
        Row row;
        
        // Add source vertex ID
        row.values.push_back(srcId);
        
        // Add edge information
        row.values.push_back(Value(int64_t(edgeType)));
        row.values.push_back(Value(int64_t(ranking)));
        row.values.push_back(dstId);
        
        // Decode and add edge properties if requested
        if (edgeProps && !edgeProps->empty()) {
          auto props = KVTValueEncoder::decodeEdgeProps(edgeValue);
          for (const auto& eProp : *edgeProps) {
            for (const auto& propName : eProp.get_props()) {
              auto it = props.find(propName);
              if (it != props.end()) {
                row.values.push_back(it->second);
              } else {
                row.values.push_back(Value());  // null value
              }
            }
          }
        }
        
        // If vertex properties are requested, fetch them
        if (vertexProps && !vertexProps->empty()) {
          // Fetch destination vertex properties
          KVTBatchOps vertexOps;
          for (const auto& vProp : *vertexProps) {
            TagID tagId = vProp.tag_ref().value();
            std::string vertexKey = KVTKeyEncoder::encodeVertexKey(
                param.space, partId, dstId, tagId);
            
            KVTOp op;
            op.op = OP_GET;
            op.table_id = vertexTableId;
            op.key = vertexKey;
            vertexOps.push_back(op);
          }
          
          KVTBatchResults vertexResults;
          std::string error;
          kvt_batch_execute(txId, vertexOps, vertexResults, error);
          
          for (const auto& vResult : vertexResults) {
            if (vResult.error == KVTError::SUCCESS) {
              auto props = KVTValueEncoder::decodeVertexProps(vResult.value);
              // Add vertex properties to row
              for (const auto& vProp : *vertexProps) {
                for (const auto& propName : vProp.get_props()) {
                  auto it = props.find(propName);
                  if (it != props.end()) {
                    row.values.push_back(it->second);
                  } else {
                    row.values.push_back(Value());
                  }
                }
              }
            }
          }
        }
        
        resultDataSet.rows.push_back(std::move(row));
      }
    }
  }
  
  // Apply dedup if requested
  if (dedup && !resultDataSet.rows.empty()) {
    std::sort(resultDataSet.rows.begin(), resultDataSet.rows.end());
    auto last = std::unique(resultDataSet.rows.begin(), resultDataSet.rows.end());
    resultDataSet.rows.erase(last, resultDataSet.rows.end());
  }
  
  // Apply limit
  if (limit > 0 && resultDataSet.rows.size() > static_cast<size_t>(limit)) {
    resultDataSet.rows.resize(limit);
  }
  
  // Commit transaction
  Status commitStatus = tx->commit();
  if (!commitStatus.ok()) {
    LOG(ERROR) << "Failed to commit transaction: " << commitStatus.toString();
    rpcResp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(std::move(rpcResp));
  }
  
  // Set response
  getNeighborsResp.vertices_ref() = std::move(resultDataSet);
  rpcResp.addResponse(std::move(getNeighborsResp));
  
  LOG(INFO) << "getNeighbors completed successfully with " 
            << resultDataSet.rows.size() << " neighbors found";
  
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(std::move(rpcResp));
}

StorageRpcRespFuture<cpp2::GetDstBySrcResponse> KVTStorageClient::getDstBySrc(
    const CommonRequestParam& param,
    const std::vector<Value>& vertices,
    const std::vector<EdgeType>& edgeTypes) {
  // TODO: Implement using KVT
  LOG(ERROR) << "getDstBySrc not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetDstBySrcResponse>>(
      StorageRpcResponse<cpp2::GetDstBySrcResponse>(1));
}

StorageRpcRespFuture<cpp2::GetPropResponse> KVTStorageClient::getProps(
    const CommonRequestParam& param,
    const DataSet& input,
    const std::vector<cpp2::VertexProp>* vertexProps,
    const std::vector<cpp2::EdgeProp>* edgeProps,
    const std::vector<cpp2::Expr>* expressions,
    bool dedup,
    const std::vector<cpp2::OrderBy>& orderBy,
    int64_t limit,
    const Expression* filter) {
  
  LOG(INFO) << "getProps called for space " << param.space 
            << " with " << input.rows.size() << " input rows";
  
  // Initialize KVT if not already done
  if (!kvtInitialized_) {
    auto status = init();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize KVT: " << status.toString();
      StorageRpcResponse<cpp2::GetPropResponse> resp(1);
      resp.markFailure();
      return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetPropResponse>>(std::move(resp));
    }
  }
  
  // Get table IDs for this space
  uint64_t vertexTableId = getVertexTableId(param.space);
  uint64_t edgeTableId = getEdgeTableId(param.space);
  
  if (vertexTableId == 0 || edgeTableId == 0) {
    LOG(ERROR) << "Failed to get table IDs for space " << param.space;
    StorageRpcResponse<cpp2::GetPropResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetPropResponse>>(std::move(resp));
  }
  
  // Create transaction manager
  KVTTransactionManager txManager;
  
  // Prepare batch operations
  KVTBatchOps batchOps;
  
  // Process input rows to determine what to fetch
  bool fetchingVertices = (vertexProps != nullptr && !vertexProps->empty());
  bool fetchingEdges = (edgeProps != nullptr && !edgeProps->empty());
  
  if (fetchingVertices) {
    // For each input row, construct vertex keys based on vertexProps
    for (const auto& row : input.rows) {
      if (row.values.empty()) continue;
      
      // Assume first column is vertex ID, second (if present) is tag ID
      const Value& vertexId = row.values[0];
      
      for (const auto& vProp : *vertexProps) {
        TagID tagId = vProp.tag_ref().value();
        PartitionID partId = 0;  // TODO: Calculate proper partition
        
        std::string key = KVTKeyEncoder::encodeVertexKey(
            param.space, partId, vertexId, tagId);
        
        KVTOp op;
        op.op = OP_GET;
        op.table_id = vertexTableId;
        op.key = key;
        batchOps.push_back(op);
      }
    }
  }
  
  if (fetchingEdges) {
    // For each input row, construct edge keys based on edgeProps
    for (const auto& row : input.rows) {
      if (row.values.size() < 4) {
        LOG(WARNING) << "Edge row has insufficient columns";
        continue;
      }
      
      // For edges: src, edge_type, ranking, dst
      const Value& srcId = row.values[0];
      EdgeType edgeType = row.values[1].getInt();
      EdgeRanking ranking = row.values[2].getInt();
      const Value& dstId = row.values[3];
      PartitionID partId = 0;  // TODO: Calculate proper partition
      
      std::string key = KVTKeyEncoder::encodeEdgeKey(
          param.space, partId, srcId, edgeType, ranking, dstId);
      
      KVTOp op;
      op.op = OP_GET;
      op.table_id = edgeTableId;
      op.key = key;
      batchOps.push_back(op);
    }
  }
  
  // Execute batch operations
  auto batchResult = txManager.executeBatch(batchOps);
  
  // Prepare response
  StorageRpcResponse<cpp2::GetPropResponse> rpcResp(1);
  cpp2::GetPropResponse getPropResp;
  
  if (!batchResult.ok()) {
    LOG(ERROR) << "Batch execution failed: " << batchResult.status().toString();
    rpcResp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetPropResponse>>(std::move(rpcResp));
  }
  
  const auto& results = batchResult.value();
  
  // Build response DataSet
  DataSet resultDataSet;
  
  // Set column names based on requested properties
  if (fetchingVertices && vertexProps) {
    for (const auto& vProp : *vertexProps) {
      for (const auto& prop : vProp.get_props()) {
        resultDataSet.colNames.push_back(prop);
      }
    }
  }
  
  if (fetchingEdges && edgeProps) {
    for (const auto& eProp : *edgeProps) {
      for (const auto& prop : eProp.get_props()) {
        resultDataSet.colNames.push_back(prop);
      }
    }
  }
  
  // Process results and build rows
  size_t resultIdx = 0;
  for (const auto& opResult : results) {
    if (opResult.error == KVTError::SUCCESS) {
      // Parse the value and extract properties
      // For now, we'll return the raw value as a single column
      Row row;
      row.values.emplace_back(opResult.value);
      resultDataSet.rows.push_back(std::move(row));
    } else if (opResult.error == KVTError::KEY_NOT_FOUND) {
      // Key not found - add empty row or handle as needed
      LOG(INFO) << "Key not found for operation " << resultIdx;
    } else {
      LOG(WARNING) << "Operation " << resultIdx << " failed with error " 
                   << static_cast<int>(opResult.error);
    }
    resultIdx++;
  }
  
  // Apply dedup if requested
  if (dedup && !resultDataSet.rows.empty()) {
    std::sort(resultDataSet.rows.begin(), resultDataSet.rows.end());
    auto last = std::unique(resultDataSet.rows.begin(), resultDataSet.rows.end());
    resultDataSet.rows.erase(last, resultDataSet.rows.end());
  }
  
  // Apply limit
  if (limit > 0 && resultDataSet.rows.size() > static_cast<size_t>(limit)) {
    resultDataSet.rows.resize(limit);
  }
  
  // Set the result
  getPropResp.props_ref() = std::move(resultDataSet);
  
  // Add to response
  rpcResp.addResponse(std::move(getPropResp));
  
  LOG(INFO) << "getProps completed successfully with " 
            << resultDataSet.rows.size() << " result rows";
  
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetPropResponse>>(std::move(rpcResp));
}

StorageRpcRespFuture<cpp2::ExecResponse> KVTStorageClient::addVertices(
    const CommonRequestParam& param,
    std::vector<cpp2::NewVertex> vertices,
    std::unordered_map<TagID, std::vector<std::string>> propNames,
    bool ifNotExists,
    bool ignoreExistedIndex) {
  
  LOG(INFO) << "addVertices called for space " << param.space 
            << " with " << vertices.size() << " vertices";
  
  // Initialize KVT if not already done
  if (!kvtInitialized_) {
    auto status = init();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize KVT: " << status.toString();
      StorageRpcResponse<cpp2::ExecResponse> resp(1);
      resp.markFailure();
      return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
    }
  }
  
  // Get vertex table ID for this space
  uint64_t vertexTableId = getVertexTableId(param.space);
  if (vertexTableId == 0) {
    LOG(ERROR) << "Failed to get vertex table ID for space " << param.space;
    StorageRpcResponse<cpp2::ExecResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
  }
  
  // Create transaction manager
  KVTTransactionManager txManager;
  
  // Prepare batch operations
  KVTBatchOps batchOps;
  
  for (const auto& vertex : vertices) {
    const Value& vertexId = vertex.get_id();
    
    // Process each tag for this vertex
    for (const auto& tag : vertex.get_tags()) {
      TagID tagId = tag.get_tag_id();
      PartitionID partId = 0;  // TODO: Calculate proper partition based on vertex ID
      
      // Encode the key
      std::string key = KVTKeyEncoder::encodeVertexKey(
          param.space, partId, vertexId, tagId);
      
      // Check if we need to verify existence first
      if (ifNotExists) {
        // Add a GET operation first to check existence
        KVTOp checkOp;
        checkOp.op = OP_GET;
        checkOp.table_id = vertexTableId;
        checkOp.key = key;
        batchOps.push_back(checkOp);
      }
      
      // Get property names for this tag
      auto propNamesIt = propNames.find(tagId);
      if (propNamesIt == propNames.end()) {
        LOG(WARNING) << "No property names found for tag " << tagId;
        continue;
      }
      
      // Encode the value with properties
      std::string value = KVTValueEncoder::encodeNewVertex(
          vertex, tagId, propNamesIt->second);
      
      // Add SET operation
      KVTOp setOp;
      setOp.op = OP_SET;
      setOp.table_id = vertexTableId;
      setOp.key = key;
      setOp.value = value;
      batchOps.push_back(setOp);
    }
  }
  
  // Execute batch operations
  auto batchResult = txManager.executeBatch(batchOps);
  
  // Prepare response
  StorageRpcResponse<cpp2::ExecResponse> rpcResp(1);
  cpp2::ExecResponse execResp;
  
  if (!batchResult.ok()) {
    LOG(ERROR) << "Batch execution failed: " << batchResult.status().toString();
    rpcResp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
  }
  
  // Process results
  const auto& results = batchResult.value();
  int successCount = 0;
  int failureCount = 0;
  
  // If ifNotExists is true, we need to check GET results
  size_t resultIdx = 0;
  for (const auto& vertex : vertices) {
    for (const auto& tag : vertex.get_tags()) {
      if (ifNotExists) {
        // Check the GET result
        if (resultIdx < results.size()) {
          const auto& checkResult = results[resultIdx++];
          if (checkResult.error == KVTError::SUCCESS) {
            // Key already exists, skip the SET
            LOG(INFO) << "Vertex already exists, skipping";
            resultIdx++;  // Skip the corresponding SET operation
            continue;
          }
        }
      }
      
      // Check the SET result
      if (resultIdx < results.size()) {
        const auto& setResult = results[resultIdx++];
        if (setResult.error == KVTError::SUCCESS) {
          successCount++;
        } else {
          failureCount++;
          LOG(WARNING) << "Failed to add vertex tag: " 
                       << static_cast<int>(setResult.error);
        }
      }
    }
  }
  
  // Set response result
  cpp2::ResponseCommon respCommon;
  nebula::cpp2::ErrorCode code = failureCount > 0 ? 
      nebula::cpp2::ErrorCode::E_PARTIAL_SUCCEEDED :
      nebula::cpp2::ErrorCode::SUCCEEDED;
  respCommon.failed_parts_ref() = {};
  respCommon.latency_in_us_ref() = 0;
  execResp.result_ref() = respCommon;
  
  rpcResp.addResponse(std::move(execResp));
  
  LOG(INFO) << "addVertices completed with " << successCount 
            << " successes and " << failureCount << " failures";
  
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
}

StorageRpcRespFuture<cpp2::ExecResponse> KVTStorageClient::addEdges(
    const CommonRequestParam& param,
    std::vector<cpp2::NewEdge> edges,
    std::vector<std::string> propNames,
    bool ifNotExists,
    bool ignoreExistedIndex) {
  
  LOG(INFO) << "addEdges called for space " << param.space 
            << " with " << edges.size() << " edges";
  
  // Initialize KVT if not already done
  if (!kvtInitialized_) {
    auto status = init();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize KVT: " << status.toString();
      StorageRpcResponse<cpp2::ExecResponse> resp(1);
      resp.markFailure();
      return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
    }
  }
  
  // Get edge table ID for this space
  uint64_t edgeTableId = getEdgeTableId(param.space);
  if (edgeTableId == 0) {
    LOG(ERROR) << "Failed to get edge table ID for space " << param.space;
    StorageRpcResponse<cpp2::ExecResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
  }
  
  // Create transaction manager
  KVTTransactionManager txManager;
  
  // Prepare batch operations
  KVTBatchOps batchOps;
  
  for (const auto& edge : edges) {
    const auto& edgeKey = edge.get_key();
    const Value& srcId = edgeKey.get_src();
    const Value& dstId = edgeKey.get_dst();
    EdgeType edgeType = edgeKey.get_edge_type();
    EdgeRanking ranking = edgeKey.get_ranking();
    PartitionID partId = 0;  // TODO: Calculate proper partition based on source vertex
    
    // Encode the key
    std::string key = KVTKeyEncoder::encodeEdgeKey(
        param.space, partId, srcId, edgeType, ranking, dstId);
    
    // Check if we need to verify existence first
    if (ifNotExists) {
      // Add a GET operation first to check existence
      KVTOp checkOp;
      checkOp.op = OP_GET;
      checkOp.table_id = edgeTableId;
      checkOp.key = key;
      batchOps.push_back(checkOp);
    }
    
    // Encode the value with properties
    std::string value = KVTValueEncoder::encodeNewEdge(edge, propNames);
    
    // Add SET operation for the forward edge
    KVTOp setOp;
    setOp.op = OP_SET;
    setOp.table_id = edgeTableId;
    setOp.key = key;
    setOp.value = value;
    batchOps.push_back(setOp);
    
    // Also add reverse edge index for efficient IN_EDGE queries
    std::string reverseKey = KVTKeyEncoder::encodeReverseEdgeKey(
        param.space, partId, dstId, edgeType, ranking, srcId);
    
    KVTOp reverseOp;
    reverseOp.op = OP_SET;
    reverseOp.table_id = edgeTableId;
    reverseOp.key = reverseKey;
    reverseOp.value = value;  // Store same edge data
    batchOps.push_back(reverseOp);
  }
  
  // Execute batch operations
  auto batchResult = txManager.executeBatch(batchOps);
  
  // Prepare response
  StorageRpcResponse<cpp2::ExecResponse> rpcResp(1);
  cpp2::ExecResponse execResp;
  
  if (!batchResult.ok()) {
    LOG(ERROR) << "Batch execution failed: " << batchResult.status().toString();
    rpcResp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
  }
  
  // Process results
  const auto& results = batchResult.value();
  int successCount = 0;
  int failureCount = 0;
  
  // If ifNotExists is true, we need to check GET results
  size_t resultIdx = 0;
  for (size_t i = 0; i < edges.size(); i++) {
    if (ifNotExists) {
      // Check the GET result
      if (resultIdx < results.size()) {
        const auto& checkResult = results[resultIdx++];
        if (checkResult.error == KVTError::SUCCESS) {
          // Key already exists, skip the SET
          LOG(INFO) << "Edge already exists, skipping";
          resultIdx++;  // Skip the corresponding SET operation
          continue;
        }
      }
    }
    
    // Check the SET result
    if (resultIdx < results.size()) {
      const auto& setResult = results[resultIdx++];
      if (setResult.error == KVTError::SUCCESS) {
        successCount++;
      } else {
        failureCount++;
        LOG(WARNING) << "Failed to add edge: " 
                     << static_cast<int>(setResult.error);
      }
    }
  }
  
  // Set response result
  cpp2::ResponseCommon respCommon;
  nebula::cpp2::ErrorCode code = failureCount > 0 ? 
      nebula::cpp2::ErrorCode::E_PARTIAL_SUCCEEDED :
      nebula::cpp2::ErrorCode::SUCCEEDED;
  respCommon.failed_parts_ref() = {};
  respCommon.latency_in_us_ref() = 0;
  execResp.result_ref() = respCommon;
  
  rpcResp.addResponse(std::move(execResp));
  
  LOG(INFO) << "addEdges completed with " << successCount 
            << " successes and " << failureCount << " failures";
  
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
}

StorageRpcRespFuture<cpp2::ExecResponse> KVTStorageClient::deleteEdges(
    const CommonRequestParam& param,
    std::vector<storage::cpp2::EdgeKey> edges) {
  
  LOG(INFO) << "deleteEdges called for space " << param.space 
            << " with " << edges.size() << " edges";
  
  // Initialize KVT if not already done
  if (!kvtInitialized_) {
    auto status = init();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize KVT: " << status.toString();
      StorageRpcResponse<cpp2::ExecResponse> resp(1);
      resp.markFailure();
      return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
    }
  }
  
  // Get edge table ID for this space
  uint64_t edgeTableId = getEdgeTableId(param.space);
  if (edgeTableId == 0) {
    LOG(ERROR) << "Failed to get edge table ID for space " << param.space;
    StorageRpcResponse<cpp2::ExecResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
  }
  
  // Create transaction manager
  KVTTransactionManager txManager;
  
  // Prepare batch operations
  KVTBatchOps batchOps;
  
  for (const auto& edgeKey : edges) {
    const Value& srcId = edgeKey.get_src();
    const Value& dstId = edgeKey.get_dst();
    EdgeType edgeType = edgeKey.get_edge_type();
    EdgeRanking ranking = edgeKey.get_ranking();
    PartitionID partId = 0;  // TODO: Calculate proper partition
    
    // Encode the forward edge key
    std::string key = KVTKeyEncoder::encodeEdgeKey(
        param.space, partId, srcId, edgeType, ranking, dstId);
    
    // Add DELETE operation for forward edge
    KVTOp delOp;
    delOp.op = OP_DEL;
    delOp.table_id = edgeTableId;
    delOp.key = key;
    batchOps.push_back(delOp);
    
    // Also delete the reverse edge index
    std::string reverseKey = KVTKeyEncoder::encodeReverseEdgeKey(
        param.space, partId, dstId, edgeType, ranking, srcId);
    
    KVTOp delReverseOp;
    delReverseOp.op = OP_DEL;
    delReverseOp.table_id = edgeTableId;
    delReverseOp.key = reverseKey;
    batchOps.push_back(delReverseOp);
  }
  
  // Execute batch operations
  auto batchResult = txManager.executeBatch(batchOps);
  
  // Prepare response
  StorageRpcResponse<cpp2::ExecResponse> rpcResp(1);
  cpp2::ExecResponse execResp;
  
  if (!batchResult.ok()) {
    LOG(ERROR) << "Batch execution failed: " << batchResult.status().toString();
    rpcResp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
  }
  
  // Process results
  const auto& results = batchResult.value();
  int successCount = 0;
  int failureCount = 0;
  
  for (const auto& result : results) {
    if (result.error == KVTError::SUCCESS || 
        result.error == KVTError::KEY_NOT_FOUND) {
      // Consider KEY_NOT_FOUND as success for delete
      successCount++;
    } else {
      failureCount++;
      LOG(WARNING) << "Failed to delete edge: " 
                   << static_cast<int>(result.error);
    }
  }
  
  // Set response result
  cpp2::ResponseCommon respCommon;
  nebula::cpp2::ErrorCode code = failureCount > 0 ? 
      nebula::cpp2::ErrorCode::E_PARTIAL_SUCCEEDED :
      nebula::cpp2::ErrorCode::SUCCEEDED;
  respCommon.failed_parts_ref() = {};
  respCommon.latency_in_us_ref() = 0;
  execResp.result_ref() = respCommon;
  
  rpcResp.addResponse(std::move(execResp));
  
  LOG(INFO) << "deleteEdges completed with " << successCount 
            << " successes and " << failureCount << " failures";
  
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
}

StorageRpcRespFuture<cpp2::ExecResponse> KVTStorageClient::deleteVertices(
    const CommonRequestParam& param,
    std::vector<Value> ids) {
  
  LOG(INFO) << "deleteVertices called for space " << param.space 
            << " with " << ids.size() << " vertices";
  
  // Initialize KVT if not already done
  if (!kvtInitialized_) {
    auto status = init();
    if (!status.ok()) {
      LOG(ERROR) << "Failed to initialize KVT: " << status.toString();
      StorageRpcResponse<cpp2::ExecResponse> resp(1);
      resp.markFailure();
      return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
    }
  }
  
  // Get table IDs for this space
  uint64_t vertexTableId = getVertexTableId(param.space);
  uint64_t edgeTableId = getEdgeTableId(param.space);
  
  if (vertexTableId == 0 || edgeTableId == 0) {
    LOG(ERROR) << "Failed to get table IDs for space " << param.space;
    StorageRpcResponse<cpp2::ExecResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
  }
  
  // Create transaction manager
  KVTTransactionManager txManager;
  
  // Start transaction for consistent deletion
  auto txResult = txManager.startTransaction();
  if (!txResult.ok()) {
    LOG(ERROR) << "Failed to start transaction: " << txResult.status().toString();
    StorageRpcResponse<cpp2::ExecResponse> resp(1);
    resp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(resp));
  }
  
  auto tx = std::move(txResult).value();
  uint64_t txId = tx->getId();
  
  KVTBatchOps batchOps;
  
  for (const auto& vertexId : ids) {
    PartitionID partId = 0;  // TODO: Calculate proper partition
    
    // First, we need to scan and delete all vertex tags
    // Create a prefix for all tags of this vertex
    std::string vertexPrefix = KVTKeyEncoder::vertexPrefix(param.space, partId, &vertexId);
    
    // Scan for all vertex keys with this prefix
    std::string scanStart = vertexPrefix;
    std::string scanEnd = vertexPrefix;
    scanEnd.push_back(0xFF);  // Add a high byte to create an upper bound
    
    std::vector<std::pair<std::string, std::string>> vertexResults;
    std::string error;
    KVTError err = kvt_scan(txId, vertexTableId, scanStart, scanEnd, 
                            1000, vertexResults, error);
    
    if (err == KVTError::SUCCESS) {
      // Delete all found vertex keys
      for (const auto& [key, value] : vertexResults) {
        KVTOp delOp;
        delOp.op = OP_DEL;
        delOp.table_id = vertexTableId;
        delOp.key = key;
        batchOps.push_back(delOp);
      }
    }
    
    // Next, delete all edges where this vertex is the source
    std::string edgePrefix = KVTKeyEncoder::edgePrefix(param.space, partId, &vertexId);
    scanStart = edgePrefix;
    scanEnd = edgePrefix;
    scanEnd.push_back(0xFF);
    
    std::vector<std::pair<std::string, std::string>> edgeResults;
    err = kvt_scan(txId, edgeTableId, scanStart, scanEnd, 
                   10000, edgeResults, error);
    
    if (err == KVTError::SUCCESS) {
      // Delete all found edge keys
      for (const auto& [key, value] : edgeResults) {
        KVTOp delOp;
        delOp.op = OP_DEL;
        delOp.table_id = edgeTableId;
        delOp.key = key;
        batchOps.push_back(delOp);
      }
    }
    
    // Delete incoming edges (where this vertex is the destination)
    // Use reverse edge index to efficiently find incoming edges
    std::string reversePrefix = KVTKeyEncoder::reverseEdgePrefix(
        spaceId, 0, &vid);  // partId = 0, will scan all edge types
    std::string scanEnd = reversePrefix;
    scanEnd.push_back(0xFF);
    
    std::vector<std::pair<std::string, std::string>> incomingEdges;
    std::string scanError;
    KVTError scanErr = kvt_scan(txId, edgeTableId, reversePrefix, scanEnd,
                                10000, incomingEdges, scanError);
    
    if (scanErr == KVTError::SUCCESS) {
      for (const auto& [reverseKey, value] : incomingEdges) {
        // Delete the reverse edge index entry
        KVTOp delReverseOp;
        delReverseOp.op = OP_DEL;
        delReverseOp.table_id = edgeTableId;
        delReverseOp.key = reverseKey;
        batchOps.push_back(delReverseOp);
        
        // Also delete the corresponding forward edge
        GraphSpaceID spaceId;
        PartitionID partId;
        Value dstId, srcId;
        EdgeType edgeType;
        EdgeRanking ranking;
        
        if (KVTKeyEncoder::decodeReverseEdgeKey(reverseKey, spaceId, partId,
                                                dstId, edgeType, ranking, srcId)) {
          std::string forwardKey = KVTKeyEncoder::encodeEdgeKey(
              spaceId, partId, srcId, edgeType, ranking, dstId);
          
          KVTOp delForwardOp;
          delForwardOp.op = OP_DEL;
          delForwardOp.table_id = edgeTableId;
          delForwardOp.key = forwardKey;
          batchOps.push_back(delForwardOp);
        }
      }
    }
  }
  
  // Execute batch operations
  KVTBatchResults results;
  std::string error;
  KVTError err = kvt_batch_execute(txId, batchOps, results, error);
  
  // Prepare response
  StorageRpcResponse<cpp2::ExecResponse> rpcResp(1);
  cpp2::ExecResponse execResp;
  
  if (err != KVTError::SUCCESS && err != KVTError::BATCH_NOT_FULLY_SUCCESS) {
    LOG(ERROR) << "Batch execution failed: " << error;
    tx->rollback();
    rpcResp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
  }
  
  // Process results
  int successCount = 0;
  int failureCount = 0;
  
  for (const auto& result : results) {
    if (result.error == KVTError::SUCCESS || 
        result.error == KVTError::KEY_NOT_FOUND) {
      successCount++;
    } else {
      failureCount++;
      LOG(WARNING) << "Failed to delete: " << static_cast<int>(result.error);
    }
  }
  
  // Commit transaction
  Status commitStatus = tx->commit();
  if (!commitStatus.ok()) {
    LOG(ERROR) << "Failed to commit transaction: " << commitStatus.toString();
    rpcResp.markFailure();
    return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
  }
  
  // Set response result
  cpp2::ResponseCommon respCommon;
  nebula::cpp2::ErrorCode code = failureCount > 0 ? 
      nebula::cpp2::ErrorCode::E_PARTIAL_SUCCEEDED :
      nebula::cpp2::ErrorCode::SUCCEEDED;
  respCommon.failed_parts_ref() = {};
  respCommon.latency_in_us_ref() = 0;
  execResp.result_ref() = respCommon;
  
  rpcResp.addResponse(std::move(execResp));
  
  LOG(INFO) << "deleteVertices completed with " << successCount 
            << " deletes and " << failureCount << " failures";
  
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(std::move(rpcResp));
}

StorageRpcRespFuture<cpp2::ExecResponse> KVTStorageClient::deleteTags(
    const CommonRequestParam& param,
    std::vector<cpp2::DelTags> delTags) {
  // TODO: Implement using KVT
  LOG(ERROR) << "deleteTags not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(
      StorageRpcResponse<cpp2::ExecResponse>(1));
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> KVTStorageClient::updateVertex(
    const CommonRequestParam& param,
    Value vertexId,
    TagID tagId,
    std::vector<cpp2::UpdatedProp> updatedProps,
    bool insertable,
    std::vector<std::string> returnProps,
    std::string condition) {
  // TODO: Implement using KVT
  LOG(ERROR) << "updateVertex not yet implemented for KVT";
  storage::cpp2::UpdateResponse resp;
  return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(std::move(resp));
}

folly::Future<StatusOr<storage::cpp2::UpdateResponse>> KVTStorageClient::updateEdge(
    const CommonRequestParam& param,
    storage::cpp2::EdgeKey edgeKey,
    std::vector<cpp2::UpdatedProp> updatedProps,
    bool insertable,
    std::vector<std::string> returnProps,
    std::string condition) {
  // TODO: Implement using KVT
  LOG(ERROR) << "updateEdge not yet implemented for KVT";
  storage::cpp2::UpdateResponse resp;
  return folly::makeFuture<StatusOr<storage::cpp2::UpdateResponse>>(std::move(resp));
}

folly::Future<StatusOr<cpp2::GetUUIDResp>> KVTStorageClient::getUUID(
    GraphSpaceID space,
    const std::string& name,
    folly::EventBase* evb) {
  // TODO: Implement using KVT
  LOG(ERROR) << "getUUID not yet implemented for KVT";
  cpp2::GetUUIDResp resp;
  return folly::makeFuture<StatusOr<cpp2::GetUUIDResp>>(std::move(resp));
}

StorageRpcRespFuture<cpp2::LookupIndexResp> KVTStorageClient::lookupIndex(
    const CommonRequestParam& param,
    const std::vector<storage::cpp2::IndexQueryContext>& contexts,
    bool isEdge,
    int32_t tagOrEdge,
    const std::vector<std::string>& returnCols,
    std::vector<storage::cpp2::OrderBy> orderBy,
    int64_t limit) {
  // TODO: Implement using KVT
  LOG(ERROR) << "lookupIndex not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::LookupIndexResp>>(
      StorageRpcResponse<cpp2::LookupIndexResp>(1));
}

StorageRpcRespFuture<cpp2::GetNeighborsResponse> KVTStorageClient::lookupAndTraverse(
    const CommonRequestParam& param,
    cpp2::IndexSpec indexSpec,
    cpp2::TraverseSpec traverseSpec) {
  // TODO: Implement using KVT
  LOG(ERROR) << "lookupAndTraverse not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::GetNeighborsResponse>>(
      StorageRpcResponse<cpp2::GetNeighborsResponse>(1));
}

StorageRpcRespFuture<cpp2::ScanResponse> KVTStorageClient::scanEdge(
    const CommonRequestParam& param,
    const std::vector<cpp2::EdgeProp>& edgeProp,
    int64_t limit,
    const Expression* filter) {
  // TODO: Implement using KVT
  LOG(ERROR) << "scanEdge not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ScanResponse>>(
      StorageRpcResponse<cpp2::ScanResponse>(1));
}

StorageRpcRespFuture<cpp2::ScanResponse> KVTStorageClient::scanVertex(
    const CommonRequestParam& param,
    const std::vector<cpp2::VertexProp>& vertexProp,
    int64_t limit,
    const Expression* filter) {
  // TODO: Implement using KVT
  LOG(ERROR) << "scanVertex not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ScanResponse>>(
      StorageRpcResponse<cpp2::ScanResponse>(1));
}

folly::SemiFuture<StorageRpcResponse<cpp2::KVGetResponse>> KVTStorageClient::get(
    GraphSpaceID space,
    std::vector<std::string>&& keys,
    bool returnPartly,
    folly::EventBase* evb) {
  (void)space;
  (void)keys;
  (void)returnPartly;
  (void)evb;
  // TODO: Implement using KVT
  LOG(ERROR) << "get not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::KVGetResponse>>(
      StorageRpcResponse<cpp2::KVGetResponse>(1));
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> KVTStorageClient::put(
    GraphSpaceID space,
    std::vector<KeyValue> kvs,
    folly::EventBase* evb) {
  (void)space;
  (void)kvs;
  (void)evb;
  // TODO: Implement using KVT
  LOG(ERROR) << "put not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(
      StorageRpcResponse<cpp2::ExecResponse>(1));
}

folly::SemiFuture<StorageRpcResponse<cpp2::ExecResponse>> KVTStorageClient::remove(
    GraphSpaceID space,
    std::vector<std::string> keys,
    folly::EventBase* evb) {
  (void)space;
  (void)keys;
  (void)evb;
  // TODO: Implement using KVT
  LOG(ERROR) << "remove not yet implemented for KVT";
  return folly::makeSemiFuture<StorageRpcResponse<cpp2::ExecResponse>>(
      StorageRpcResponse<cpp2::ExecResponse>(1));
}

// Helper methods implementations
StatusOr<std::function<const VertexID&(const Row&)>> KVTStorageClient::getIdFromRow(
    GraphSpaceID space, bool isEdgeProps) const {
  (void)space;
  (void)isEdgeProps;
  // TODO: Implement based on KVT key encoding
  return Status::Error("Not implemented");
}

StatusOr<std::function<const VertexID&(const cpp2::NewVertex&)>> 
KVTStorageClient::getIdFromNewVertex(GraphSpaceID space) const {
  (void)space;
  // TODO: Implement based on KVT key encoding
  return Status::Error("Not implemented");
}

StatusOr<std::function<const VertexID&(const cpp2::NewEdge&)>> 
KVTStorageClient::getIdFromNewEdge(GraphSpaceID space) const {
  (void)space;
  // TODO: Implement based on KVT key encoding
  return Status::Error("Not implemented");
}

StatusOr<std::function<const VertexID&(const cpp2::EdgeKey&)>> 
KVTStorageClient::getIdFromEdgeKey(GraphSpaceID space) const {
  (void)space;
  // TODO: Implement based on KVT key encoding
  return Status::Error("Not implemented");
}

StatusOr<std::function<const VertexID&(const Value&)>> 
KVTStorageClient::getIdFromValue(GraphSpaceID space) const {
  (void)space;
  // TODO: Implement based on KVT key encoding
  return Status::Error("Not implemented");
}

StatusOr<std::function<const VertexID&(const cpp2::DelTags&)>> 
KVTStorageClient::getIdFromDelTags(GraphSpaceID space) const {
  (void)space;
  // TODO: Implement based on KVT key encoding
  return Status::Error("Not implemented");
}

}  // namespace storage
}  // namespace nebula