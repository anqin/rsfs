// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef  RSFS_TERA_ENTRY_H
#define  RSFS_TERA_ENTRY_H

#include "common/lock/mutex.h"

namespace rsfs {

class RsfsEntry {
public:
    RsfsEntry();
    virtual ~RsfsEntry();

    virtual bool Start();
    virtual bool Run();
    virtual bool Shutdown();

protected:
    virtual bool StartServer() = 0;
    virtual void ShutdownServer() = 0;

private:
    bool ShouldStart();
    bool ShouldShutdown();

private:
    Mutex m_mutex;
    bool m_started;
};

}  // namespace rsfs

#endif  // RSFS_TERA_ENTRY_H
