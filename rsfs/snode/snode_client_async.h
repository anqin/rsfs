// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef RSFS_SNODE_SNODE_CLIENT_ASYNC_H
#define RSFS_SNODE_SNODE_CLIENT_ASYNC_H

#include "thirdparty/sofa/pbrpc/pbrpc.h"

#include "rsfs/proto/snode_rpc.pb.h"
#include "rsfs/rpc_client_async.h"

DECLARE_int32(rsfs_snode_connect_retry_period);
DECLARE_int32(rsfs_snode_rpc_timeout_period);

class ThreadPool;

namespace rsfs {
namespace snode {

class SNodeClientAsync : public RpcClientAsync<SNodeServer::Stub> {
public:
    static void SetThreadPool(ThreadPool* thread_pool);

    static void SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                             int32_t pending_buffer_size, int32_t thread_num);

    SNodeClientAsync(const std::string& addr = "",
                     int32_t rpc_timeout = FLAGS_rsfs_snode_rpc_timeout_period);

    ~SNodeClientAsync();

    bool OpenData(const OpenDataRequest* request,
                  OpenDataResponse* response,
                  Closure<void, OpenDataRequest*, OpenDataResponse*, bool, int>* done = NULL);

    bool CloseData(const CloseDataRequest* request,
                  CloseDataResponse* response,
                  Closure<void, CloseDataRequest*, CloseDataResponse*, bool, int>* done = NULL);

    bool WriteData(const WriteDataRequest* request,
                   WriteDataResponse* response,
                   Closure<void, WriteDataRequest*, WriteDataResponse*, bool, int>* done = NULL);

    bool ReadData(const ReadDataRequest* request,
                  ReadDataResponse* response,
                  Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done = NULL);

private:
    bool IsRetryStatus(const StatusCode& status);

    int32_t m_rpc_timeout;
    static ThreadPool* m_thread_pool;
};

} // namespace sdk
} // namespace rsfs

#endif // RSFS_SNODE_SNODE_CLIENT_ASYNC_H
