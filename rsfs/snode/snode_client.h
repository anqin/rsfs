// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef RSFS_SNODE_SNODE_CLIENT_H
#define RSFS_SNODE_SNODE_CLIENT_H

#include "rsfs/proto/snode_rpc.pb.h"
#include "rsfs/rpc_client.h"

namespace rsfs {
namespace snode {

class SNodeClient : public RpcClient<SNodeServer::Stub> {
public:
    SNodeClient();
    SNodeClient(int32_t wait_time, int32_t rpc_timeout, int32_t retry_times);
    ~SNodeClient();

    void ResetSNodeClient(const std::string& server_addr);

    bool OpenData(const OpenDataRequest* request,
                   OpenDataResponse* response);

    bool CloseData(const CloseDataRequest* request,
                   CloseDataResponse* response);

    bool WriteData(const WriteDataRequest* request,
                   WriteDataResponse* response);

    bool ReadData(const ReadDataRequest* request,
                   ReadDataResponse* response);

private:
    bool IsRetryStatus(const StatusCode& status);
};

} // namespace snode
} // namespace rsfs

#endif // RSFS_SNODE_SNODE_CLIENT_H
