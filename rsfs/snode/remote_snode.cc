// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/snode/remote_snode.h"

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "rsfs/snode/snode_impl.h"

DECLARE_int32(rsfs_snode_thread_min_num);
DECLARE_int32(rsfs_snode_thread_max_num);

namespace rsfs {
namespace snode {

RemoteSNode::RemoteSNode(SNodeImpl* snode_impl)
    : m_snode_impl(snode_impl),
      m_thread_pool(new ThreadPool(FLAGS_rsfs_snode_thread_min_num,
                                   FLAGS_rsfs_snode_thread_max_num)) {}

RemoteSNode::~RemoteSNode() {}

void RemoteSNode::OpenData(google::protobuf::RpcController* controller,
                           const OpenDataRequest* request,
                           OpenDataResponse* response,
                           google::protobuf::Closure* done) {
    Closure<void>* callback =
        NewClosure(this, &RemoteSNode::DoOpenData, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteSNode::CloseData(google::protobuf::RpcController* controller,
                           const CloseDataRequest* request,
                           CloseDataResponse* response,
                           google::protobuf::Closure* done) {
    Closure<void>* callback =
        NewClosure(this, &RemoteSNode::DoCloseData, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteSNode::WriteData(google::protobuf::RpcController* controller,
                           const WriteDataRequest* request,
                           WriteDataResponse* response,
                           google::protobuf::Closure* done) {
    Closure<void>* callback =
        NewClosure(this, &RemoteSNode::DoWriteData, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteSNode::ReadData(google::protobuf::RpcController* controller,
                           const ReadDataRequest* request,
                           ReadDataResponse* response,
                           google::protobuf::Closure* done) {
    Closure<void>* callback =
        NewClosure(this, &RemoteSNode::DoReadData, controller,
                   request, response, done);
    m_thread_pool->AddTask(callback);
}

void RemoteSNode::DoOpenData(google::protobuf::RpcController* controller,
                             const OpenDataRequest* request,
                             OpenDataResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (OpenData)";
    m_snode_impl->OpenData(request, response, done);
    LOG(INFO) << "finish RPC (OpenData)";
}

void RemoteSNode::DoCloseData(google::protobuf::RpcController* controller,
                             const CloseDataRequest* request,
                             CloseDataResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (CloseData)";
    m_snode_impl->CloseData(request, response, done);
    LOG(INFO) << "finish RPC (CloseData)";
}

void RemoteSNode::DoWriteData(google::protobuf::RpcController* controller,
                             const WriteDataRequest* request,
                             WriteDataResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (WriteData)";
    m_snode_impl->WriteData(request, response, done);
    LOG(INFO) << "finish RPC (WriteData)";
}

void RemoteSNode::DoReadData(google::protobuf::RpcController* controller,
                             const ReadDataRequest* request,
                             ReadDataResponse* response,
                             google::protobuf::Closure* done) {
    LOG(INFO) << "accept RPC (ReadData)";
    m_snode_impl->ReadData(request, response, done);
    LOG(INFO) << "finish RPC (ReadData)";
}

} // namespace snode
} // namespace rsfs
