// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#include "rsfs/master/master_impl.h"

#include "rsfs/master/node_manager.h"
#include "rsfs/master/meta_tree.h"
#include "rsfs/proto/proto_helper.h"

namespace rsfs {
namespace master {


MasterImpl::MasterImpl()
    : m_node_manager(new NodeManager()),
      m_meta_tree(new MetaTree(m_node_manager.get())) {}

MasterImpl::~MasterImpl() {}

bool MasterImpl::Init() {
    m_status = kIsRunning;
    return true;
}

bool MasterImpl::OpenFile(const OpenFileRequest* request,
                          OpenFileResponse* response) {
    LOG(INFO) << "open file: " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    if (request->type() == OpenFileRequest::WRITE) {
        return OpenFileForWrite(request, response);
    } else {
        return OpenFileForRead(request, response);
    }
}

bool MasterImpl::CloseFile(const CloseFileRequest* request,
                           CloseFileResponse* response) {
    LOG(INFO) << "close file: " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());

    TreeNode tree_node;
    tree_node.set_name(request->file_name());
    tree_node.set_file_size(request->file_size());
    tree_node.set_tail_slice(request->tail_slice());
    tree_node.set_tail_num(request->tail_num());
    tree_node.set_crash_slice(request->crash_slice());
    tree_node.set_crash_num(request->crash_num());

    StatusCode status = kMasterOk;
    if (!m_meta_tree->CloseFile(&tree_node, &status)) {
        LOG(ERROR) << "fail to close meta tree";
        response->set_status(status);
        return false;
    }
    response->set_status(status);
    return true;
}

bool MasterImpl::ListFile(const ListFileRequest* request,
                          ListFileResponse* response) {
    LOG(INFO) << "list file: " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());

    StatusCode status = kMasterOk;
    std::string last_one;
    if (!m_meta_tree->ListFile(request->path_start(), request->path_end(),
                               request->limit(), response->mutable_metas(),
                               &last_one, &status)) {
        LOG(ERROR) << "fail to list file";
        response->set_status(status);
        return false;
    }
    response->set_last_one(last_one);
    response->set_status(status);
    return false;
}

bool MasterImpl::Report(const ReportRequest* request,
                        ReportResponse* response) {
    LOG(INFO) << "report: " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    m_node_manager->Report(request, response);
    return true;
}

bool MasterImpl::Register(const RegisterRequest* request,
                          RegisterResponse* response) {
    LOG(INFO) << "register: " << request->ShortDebugString();
    response->set_sequence_id(request->sequence_id());
    m_node_manager->Register(request, response);
    return true;
}

bool MasterImpl::OpenFileForRead(const OpenFileRequest* request,
                                 OpenFileResponse* response) {
    TreeNode tree_node;
    tree_node.set_name(request->file_name());
    tree_node.set_status(kMetaReadOpen);
    tree_node.set_chunk_num(request->node_num());

    StatusCode status = kMasterOk;
    if (!m_meta_tree->OpenFile(&tree_node, false, &status)) {
        LOG(ERROR) << "fail to open meta tree";
        response->set_status(status);
        return false;
    }
    response->set_fid(tree_node.fid());
    response->set_file_size(tree_node.file_size());
    response->mutable_nodes()->CopyFrom(tree_node.chunks());
    response->set_tail_slice(tree_node.tail_slice());
    response->set_tail_num(tree_node.tail_num());
    response->set_crash_slice(tree_node.crash_slice());
    response->set_crash_num(tree_node.crash_num());
    response->set_status(status);
    LOG(INFO) << "response: " << response->ShortDebugString();
    return true;
}

bool MasterImpl::OpenFileForWrite(const OpenFileRequest* request,
                                  OpenFileResponse* response) {
    TreeNode tree_node;
    tree_node.set_name(request->file_name());
    tree_node.set_status(kMetaWriteOpen);
    tree_node.set_chunk_num(request->node_num());

    StatusCode status = kMasterOk;
    if (!m_meta_tree->OpenFile(&tree_node, true, &status)) {
        LOG(ERROR) << "fail to open meta tree";
        response->set_status(status);
        return false;
    }
    response->set_fid(tree_node.fid());
    response->mutable_nodes()->CopyFrom(tree_node.chunks());
    response->set_status(status);
    LOG(INFO) << "response: " << response->ShortDebugString();
    return true;
}
} // namespace master
} // namespace rsfs
