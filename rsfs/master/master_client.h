// Copyright (C) 2013, Baidu Inc.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef RSFS_MASTER_MASTER_CLIENT_H
#define RSFS_MASTER_MASTER_CLIENT_H

#include <string>

#include "rsfs/proto/master_rpc.pb.h"
#include "rsfs/rpc_client.h"

namespace rsfs {
namespace master {

class MasterClient : public RpcClient<MasterServer::Stub> {
public:
    MasterClient();
    virtual ~MasterClient();

    virtual void ResetMasterClient(const std::string& server_addr);

    virtual bool OpenFile(const OpenFileRequest* request,
                          OpenFileResponse* response);

    virtual bool CloseFile(const CloseFileRequest* request,
                           CloseFileResponse* response);

    virtual bool ListFile(const ListFileRequest* request,
                          ListFileResponse* response);

    virtual bool Register(const RegisterRequest* request,
                          RegisterResponse* response);

    virtual bool Report(const ReportRequest* request,
                        ReportResponse* response);

private:
    bool PollAndResetServerAddr();
    bool IsRetryStatus(const StatusCode& status);
};

} // namespace master
} // namespace rsfs

#endif // RSFS_MASTER_MASTER_CLIENT_H
