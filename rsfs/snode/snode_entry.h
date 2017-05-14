// Copyright (C) 2013, Baidu Inc.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef RSFS_SNODE_SNODE_ENTRY_H
#define RSFS_SNODE_SNODE_ENTRY_H

#include "bobby/bobby_server.h"

#include "common/base/scoped_ptr.h"

#include "rsfs/rsfs_entry.h"

namespace rsfs {

namespace master {
class MasterClient;
}

namespace snode {

class SNodeImpl;
class RemoteSNode;

class SNodeEntry : public RsfsEntry {
public:
    SNodeEntry();
    ~SNodeEntry();

    bool StartServer();
    bool Run();
    void ShutdownServer();

private:
    scoped_ptr<SNodeImpl> m_snode_impl;
    scoped_ptr<RemoteSNode> m_remote_snode;
    scoped_ptr<master::MasterClient> m_master_client;
    scoped_ptr<bobby::BobbyServer> m_bobby_server;
};

} // namespace snode
} // namespace rsfs

#endif // RSFS_SNODE_SNODE_ENTRY_H
