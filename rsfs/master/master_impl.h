// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_MASTER_MASTER_IMPL_H
#define RSFS_MASTER_MASTER_IMPL_H

#include "common/base/scoped_ptr.h"
#include "common/lock/mutex.h"
#include "common/lock/rwlock.h"
#include "common/timer/timer_manager.h"

#include "rsfs/proto/master_rpc.pb.h"

namespace rsfs {
namespace master {

class NodeManager;
class MetaTree;

class MasterImpl {
public:
    enum MasterStatus {
        kNotInited = kMasterNotInited,
        kIsBusy = kMasterIsBusy,
        kIsRunning = kMasterIsRunning,
        kIsSecondary = kMasterIsSecondary,
        kIsReadonly = kMasterIsReadonly
    };

    MasterImpl();
    ~MasterImpl();

    bool Init();

    bool OpenFile(const OpenFileRequest* request,
                  OpenFileResponse* response);

    bool CloseFile(const CloseFileRequest* request,
                   CloseFileResponse* response);

    bool ListFile(const ListFileRequest* request,
                  ListFileResponse* response);

    bool Report(const ReportRequest* request,
                ReportResponse* response);

    bool Register(const RegisterRequest* request,
                  RegisterResponse* response);

private:
    bool OpenFileForRead(const OpenFileRequest* request,
                         OpenFileResponse* response);
    bool OpenFileForWrite(const OpenFileRequest* request,
                          OpenFileResponse* response);

private:
    mutable Mutex m_status_mutex;
    MasterStatus m_status;

    mutable RWLock m_rwlock;
    TimerManager m_timer_manager;

    scoped_ptr<NodeManager> m_node_manager;
    scoped_ptr<MetaTree> m_meta_tree;
};


} // namespace master
} // namespace rsfs

#endif // RSFS_MASTER_MASTER_IMPL_H
