// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/snode/snode_client.h"

DECLARE_string(rsfs_snode_addr);
DECLARE_string(rsfs_snode_port);
DECLARE_int32(rsfs_snode_connect_retry_times);
DECLARE_int32(rsfs_snode_connect_retry_period);
DECLARE_int32(rsfs_snode_connect_timeout_period);

namespace rsfs {
namespace snode {


SNodeClient::SNodeClient()
    : RpcClient<SNodeServer::Stub>(
                FLAGS_rsfs_snode_addr + ":" + FLAGS_rsfs_snode_port,
                FLAGS_rsfs_snode_connect_retry_period,
                FLAGS_rsfs_snode_connect_timeout_period,
                FLAGS_rsfs_snode_connect_retry_times) {}

SNodeClient::SNodeClient(int32_t wait_time, int32_t rpc_timeout,
                                   int32_t retry_times)
    : RpcClient<SNodeServer::Stub>(
            FLAGS_rsfs_snode_addr + ":" + FLAGS_rsfs_snode_port,
            wait_time, rpc_timeout, retry_times) {}


SNodeClient::~SNodeClient() {}

void SNodeClient::ResetSNodeClient(const std::string& server_addr) {
    ResetClient(server_addr);
}

bool SNodeClient::OpenData(const OpenDataRequest* request,
                           OpenDataResponse* response) {
    return SendMessageWithRetry(&SNodeServer::Stub::OpenData,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "OpenData");
}

bool SNodeClient::CloseData(const CloseDataRequest* request,
                           CloseDataResponse* response) {
    return SendMessageWithRetry(&SNodeServer::Stub::CloseData,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "CloseData");
}

bool SNodeClient::WriteData(const WriteDataRequest* request,
                            WriteDataResponse* response) {
    return SendMessageWithRetry(&SNodeServer::Stub::WriteData,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "WriteData");
}

bool SNodeClient::ReadData(const ReadDataRequest* request,
                            ReadDataResponse* response) {
    return SendMessageWithRetry(&SNodeServer::Stub::ReadData,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "ReadData");
}

bool SNodeClient::IsRetryStatus(const StatusCode& status) {
    return (status == kSNodeNotInited
            || status == kSNodeIsBusy
            || status == kSNodeIsIniting);
}

} // namespace snode
} // namespace rsfs
