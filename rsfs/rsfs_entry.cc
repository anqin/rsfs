// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "common/thread/this_thread.h"

#include "rsfs/rsfs_entry.h"

namespace rsfs {

RsfsEntry::RsfsEntry()
    : m_started(false) {}

RsfsEntry::~RsfsEntry() {}

bool RsfsEntry::Start() {
    if (ShouldStart()) {
        return StartServer();
    }
    return false;
}

bool RsfsEntry::Run() {
    ThisThread::Sleep(2000);
    return true;
}

bool RsfsEntry::Shutdown() {
    if (ShouldShutdown()) {
        ShutdownServer();
        return true;
    }
    return false;
}

bool RsfsEntry::ShouldStart() {
    Mutex::Locker lock(&m_mutex);
    if (m_started) {
        return false;
    }
    m_started = true;
    return true;
}

bool RsfsEntry::ShouldShutdown() {
    Mutex::Locker lock(&m_mutex);
    if (!m_started) {
        return false;
    }
    m_started = false;
    return true;
}

}  // namespace rsfs
