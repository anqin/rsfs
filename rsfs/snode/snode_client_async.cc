// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/snode/snode_client_async.h"

namespace rsfs {
namespace snode {

ThreadPool* SNodeClientAsync::m_thread_pool = NULL;

void SNodeClientAsync::SetThreadPool(ThreadPool* thread_pool) {
    m_thread_pool = thread_pool;
}

void SNodeClientAsync::SetRpcOption(int32_t max_inflow, int32_t max_outflow,
                                    int32_t pending_buffer_size, int32_t thread_num) {
    RpcClientAsyncBase::SetOption(max_inflow, max_outflow,
                                  pending_buffer_size, thread_num);
}

SNodeClientAsync::SNodeClientAsync(const std::string& server_addr,
                                   int32_t rpc_timeout)
    : RpcClientAsync<SNodeServer::Stub>(server_addr),
      m_rpc_timeout(rpc_timeout) {}

SNodeClientAsync::~SNodeClientAsync() {}

bool SNodeClientAsync::OpenData(const OpenDataRequest* request,
                                OpenDataResponse* response,
                                Closure<void, OpenDataRequest*, OpenDataResponse*, bool, int>* done) {
    return SendMessageWithRetry(&SNodeServer::Stub::OpenData,
                                request, response, done, "OpenData",
                                m_rpc_timeout, m_thread_pool);
}

bool SNodeClientAsync::CloseData(const CloseDataRequest* request,
                                 CloseDataResponse* response,
                                 Closure<void, CloseDataRequest*, CloseDataResponse*, bool, int>* done) {
    return SendMessageWithRetry(&SNodeServer::Stub::CloseData,
                                request, response, done, "CloseData",
                                m_rpc_timeout, m_thread_pool);
}

bool SNodeClientAsync::WriteData(const WriteDataRequest* request,
                                 WriteDataResponse* response,
                                 Closure<void, WriteDataRequest*, WriteDataResponse*, bool, int>* done) {
    return SendMessageWithRetry(&SNodeServer::Stub::WriteData,
                                request, response, done, "WriteData",
                                m_rpc_timeout, m_thread_pool);
}

bool SNodeClientAsync::ReadData(const ReadDataRequest* request,
                                ReadDataResponse* response,
                                Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done) {
    return SendMessageWithRetry(&SNodeServer::Stub::ReadData,
                                request, response, done, "ReadData",
                                m_rpc_timeout, m_thread_pool);
}

bool SNodeClientAsync::IsRetryStatus(const StatusCode& status) {
    return (status == kSNodeNotInited
            || status == kSNodeIsBusy);
}

} // namespace snode
} // namespace rsfs
