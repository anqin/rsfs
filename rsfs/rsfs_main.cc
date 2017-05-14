// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include <signal.h>

#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "version.h"
#include "common/base/scoped_ptr.h"

#include "rsfs/master/master_entry.h"
#include "rsfs/snode/snode_entry.h"
#include "rsfs/rsfs_entry.h"
#include "rsfs/utils/utils_cmd.h"

DECLARE_string(rsfs_role);

bool g_quit = false;

static void SignalIntHandler(int sig) {
    LOG(INFO) << "receive interrupt signal from user, will stop";
    g_quit = true;
}

rsfs::RsfsEntry* SwitchRsfsEntry() {
    const std::string& server_name = FLAGS_rsfs_role;

    if (server_name == "master") {
        return new rsfs::master::MasterEntry();
    } else if (server_name == "snode") {
        return new rsfs::snode::SNodeEntry();
    }
    LOG(ERROR) << "FLAGS_rsfs_role should be one of ("
        << "master | snode"
        << "), not : " << FLAGS_rsfs_role;
    return NULL;
}

int main(int argc, char** argv) {
    ::google::ParseCommandLineFlags(&argc, &argv, true);
    ::google::InitGoogleLogging(argv[0]);
    rsfs::utils::SetupLog(FLAGS_rsfs_role);

    if (argc > 1) {
        std::string ext_cmd = argv[1];
        if (ext_cmd == "version") {
            PrintSystemVersion();
            return 0;
        }
    }

    signal(SIGINT, SignalIntHandler);
    signal(SIGTERM, SignalIntHandler);

    scoped_ptr<rsfs::RsfsEntry> entry(SwitchRsfsEntry());
    if (entry.get() == NULL) {
        return -1;
    }

    if (!entry->Start()) {
        return -1;
    }

    while (!g_quit) {
        if (!entry->Run()) {
            LOG(ERROR) << "Server run error ,and then exit now ";
            break;
        }
        signal(SIGINT, SignalIntHandler);
        signal(SIGTERM, SignalIntHandler);
    }

    if (!entry->Shutdown()) {
        return -1;
    }

    return 0;
}
