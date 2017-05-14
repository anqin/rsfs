// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/sdk/sdk.h"

#include <signal.h>

#include <iomanip>
#include <iostream>
#include <string>
#include <vector>

#include "common/base/scoped_ptr.h"
#include "common/base/string_ext.h"
#include "common/base/string_number.h"
#include "common/file/file_path.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"
#include "version.h"

#include "rsfs/utils/utils_cmd.h"

DECLARE_string(flagfile);
DECLARE_string(log_dir);

DECLARE_string(rsfs_user_identity);
DECLARE_string(rsfs_user_passcode);
DECLARE_string(rsfs_sdk_conf_file);
DECLARE_string(rsfs_master_addr);
DECLARE_string(rsfs_master_port);


void Usage(const std::string& prg_name) {
    std::cout << "Usage: " << std::endl;
    std::cout << "       " << prg_name << " <cmd>  <cmd_params_list>" << std::endl;
    std::cout << "       e.g. " << "nlfs cp /path/to/src_file  /path/to/dest_file" << std::endl;
    std::cout << "       e.g. " << "nlfs rm /path/to/[dir or file]" << std::endl;
    std::cout << "       e.g. " << "nlfs ls /path/to/[dir or file]" << std::endl;
    std::cout << "       e.g. " << "nlfs [help]" << std::endl;
}

int32_t CopyOp(int32_t argc, char** argv) {
    if (argc != 4) {
        Usage(argv[0]);
        return -1;
    }
    std::string src_file = argv[2];
    std::string dest_file = argv[3];

    if (!rsfs::sdk::SDK::Copy(src_file, dest_file)) {
        LOG(ERROR) << "fail to copy from '" << argv[2] << "' to '" << argv[3] << "'";
        return -1;
    }
    return 0;
}

int32_t DeleteOp(int32_t argc, char** argv) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }
    std::string cmd = argv[1];
    std::string file_path = argv[2];
    bool is_force = false;
    if (cmd == "rmf" || cmd == "remove_force") {
        is_force = true;
    }

    if (!rsfs::sdk::SDK::Remove(file_path, is_force)) {
        LOG(ERROR) << "fail to delete '" << argv[2] << "'";
        return -1;
    }
    return 0;
}

int32_t ListOp(int32_t argc, char** argv) {
    if (argc < 3) {
        Usage(argv[0]);
        return -1;
    }
    std::string file_path = argv[2];
    std::vector<rsfs::TreeNode> node_list;
    scoped_ptr<rsfs::sdk::SDK>
        rsfs(rsfs::sdk::SDK::CreateSDKImpl(GetPathPrefix(file_path)));
    std::string path_start = file_path + "#";
    std::string path_end = file_path + "~";
    std::string last_one;
    rsfs::ErrorCode err;
    while (path_start != path_end
           && rsfs::sdk::SDK::Listx(rsfs.get(), path_start, path_end, &last_one,
                               &node_list, &err)) {
        for (uint32_t i = 0; i < node_list.size(); ++i) {
            std::cout << node_list[i].fid()
                << "\t" << std::left << std::setw(30) << node_list[i].name()
                << "\t" << std::setw(10) << node_list[i].file_size()
                << "\t" << std::setw(10) << "crashed: " << node_list[i].crash_num()
                << std::endl;
        }
        path_start = last_one;
        err.Reset();
    }
    if (err.GetType() != rsfs::ErrorCode::kOK) {
        LOG(ERROR) << "something wrong happen, err: " << err.GetReason();
        return -1;
    }
    return 0;
}

int32_t HelpOp(int32_t argc, char** argv) {
    argc = argc;
    Usage(argv[0]);
    return 0;
}

int32_t VersionOp(int32_t argc, char** argv) {
    PrintSystemVersion();
    return 0;
}

void InitFlags() {
    if (FLAGS_flagfile.empty()) {
        std::string found_path;
        if (!FLAGS_rsfs_sdk_conf_file.empty()) {
            found_path = FLAGS_rsfs_sdk_conf_file;
        } else {
            found_path = rsfs::utils::GetValueFromeEnv("RSFS_CONF");
            if (!found_path.empty() || found_path == "") {
                found_path = rsfs::utils::GetBinaryLocationDir() +
                    "/rsfs.flag";
            }
        }

        if (!found_path.empty() && IsExist(found_path)) {
            VLOG(5) << "config file is not defined, use default one: "
                << found_path;
            FLAGS_flagfile = found_path;
        } else if (IsExist("./rsfs.flag")) {
            VLOG(5) << "config file is not defined, use default one: ./rsfs.flag";
            FLAGS_flagfile = "./rsfs.flag";
        }
    }

    // init user identity & role
    std::string cur_identity = rsfs::utils::GetValueFromeEnv("USER");
    if (cur_identity.empty()) {
        cur_identity = "other";
    }
    if (FLAGS_rsfs_user_identity.empty()) {
        FLAGS_rsfs_user_identity = cur_identity;
    }

    // init log dir
    if (FLAGS_log_dir.empty()) {
        FLAGS_log_dir = "./";
    }
}


int main(int32_t argc, char** argv) {
    ::google::InitGoogleLogging(argv[0]);
    InitFlags();
    ::google::ParseCommandLineFlags(&argc, &argv, true);

    LOG(INFO) << "USER = " << FLAGS_rsfs_user_identity;
    LOG(INFO) << "Load config file: " << FLAGS_flagfile;

    if (argc < 2) {
        Usage(argv[0]);
        return -1;
    }
    int ret = 0;
    std::string cmd = argv[1];
    if (cmd == "cp" || cmd == "copy") {
        ret = CopyOp(argc, argv);
    } else if (cmd == "rm" || cmd == "remove") {
        ret = DeleteOp(argc, argv);
    } else if (cmd == "ls" || cmd == "list") {
        ret = ListOp(argc, argv);
    } else if (cmd == "help") {
        ret = HelpOp(argc, argv);
    } else {
        ret = HelpOp(argc, argv);
    }

    return ret;
}
