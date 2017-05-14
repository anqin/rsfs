// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_SNODE_REMOTE_SNODE_H
#define RSFS_SNODE_REMOTE_SNODE_H

#include "common/base/scoped_ptr.h"
#include "common/thread/thread_pool.h"

#include "rsfs/proto/snode_rpc.pb.h"

namespace rsfs {
namespace snode {

class SNodeImpl;

class RemoteSNode : public SNodeServer {
public:
    RemoteSNode(SNodeImpl* snode_impl);
    ~RemoteSNode();

    void OpenData(google::protobuf::RpcController* controller,
                  const OpenDataRequest* request,
                  OpenDataResponse* response,
                  google::protobuf::Closure* done);

    void CloseData(google::protobuf::RpcController* controller,
                  const CloseDataRequest* request,
                  CloseDataResponse* response,
                  google::protobuf::Closure* done);

    void WriteData(google::protobuf::RpcController* controller,
                  const WriteDataRequest* request,
                  WriteDataResponse* response,
                  google::protobuf::Closure* done);

    void ReadData(google::protobuf::RpcController* controller,
                  const ReadDataRequest* request,
                  ReadDataResponse* response,
                  google::protobuf::Closure* done);

private:
    void DoOpenData(google::protobuf::RpcController* controller,
                    const OpenDataRequest* request,
                    OpenDataResponse* response,
                    google::protobuf::Closure* done);

    void DoCloseData(google::protobuf::RpcController* controller,
                    const CloseDataRequest* request,
                    CloseDataResponse* response,
                    google::protobuf::Closure* done);

    void DoWriteData(google::protobuf::RpcController* controller,
                    const WriteDataRequest* request,
                    WriteDataResponse* response,
                    google::protobuf::Closure* done);

    void DoReadData(google::protobuf::RpcController* controller,
                    const ReadDataRequest* request,
                    ReadDataResponse* response,
                    google::protobuf::Closure* done);

private:
    SNodeImpl* m_snode_impl;
    scoped_ptr<ThreadPool> m_thread_pool;
};

} // namespace snode
} // namespace rsfs

#endif // RSFS_SNODE_REMOTE_SNODE_H
