// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include <string>

#include "rsfs/master/master_client.h"


DECLARE_string(rsfs_master_addr);
DECLARE_string(rsfs_master_port);
DECLARE_int32(rsfs_master_connect_retry_times);
DECLARE_int32(rsfs_master_connect_retry_period);
DECLARE_int32(rsfs_master_connect_timeout_period);

namespace rsfs {
namespace master {

MasterClient::MasterClient()
    : RpcClient<MasterServer::Stub>(FLAGS_rsfs_master_addr + ":" + FLAGS_rsfs_master_port,
                                    FLAGS_rsfs_master_connect_retry_period,
                                    FLAGS_rsfs_master_connect_timeout_period,
                                    FLAGS_rsfs_master_connect_retry_times) {}

MasterClient::~MasterClient() {}

void MasterClient::ResetMasterClient(const std::string& server_addr) {
    ResetClient(server_addr);
}

bool MasterClient::OpenFile(const OpenFileRequest* request,
                            OpenFileResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::OpenFile,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "OpenFile");
}

bool MasterClient::CloseFile(const CloseFileRequest* request,
                            CloseFileResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::CloseFile,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "CloseFile");
}

bool MasterClient::ListFile(const ListFileRequest* request,
                            ListFileResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::ListFile,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "ListFile");
}

bool MasterClient::Register(const RegisterRequest* request,
                            RegisterResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Register,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "Register");
}

bool MasterClient::Report(const ReportRequest* request,
                          ReportResponse* response) {
    return SendMessageWithRetry(&MasterServer::Stub::Report,
                                request, response,
                                (google::protobuf::Closure*)NULL,
                                "Report");
}

bool MasterClient::PollAndResetServerAddr() {
    // connect ZK to update master addr
    return true;
}

bool MasterClient::IsRetryStatus(const StatusCode& status) {
    return (status == kMasterNotInited
            || status == kMasterIsBusy
            || status == kMasterIsSecondary);
}

} // namespace master
} // namespace rsfs
