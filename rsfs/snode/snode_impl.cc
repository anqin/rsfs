// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#include "rsfs/snode/snode_impl.h"

#include "common/file/file_stream.h"
#include "common/file/file_types.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "rsfs/snode/block_manager.h"
#include "rsfs/snode/snode_client_async.h"
#include "rsfs/types.h"

DECLARE_string(rsfs_snode_port);
DECLARE_int64(rsfs_heartbeat_period);
DECLARE_int64(rsfs_heartbeat_retry_period_factor);
DECLARE_int32(rsfs_heartbeat_retry_times);

DECLARE_int32(rsfs_snode_thread_min_num);
DECLARE_int32(rsfs_snode_thread_max_num);
DECLARE_bool(rsfs_snode_rpc_limit_enabled);
DECLARE_int32(rsfs_snode_rpc_limit_max_inflow);
DECLARE_int32(rsfs_snode_rpc_limit_max_outflow);
DECLARE_int32(rsfs_snode_rpc_max_pending_buffer_size);
DECLARE_int32(rsfs_snode_rpc_work_thread_num);

namespace rsfs {
namespace snode {


SNodeImpl::SNodeImpl(const SNodeInfo& snode_info,
                     master::MasterClient* master_client)
    : m_snode_info(snode_info), m_master_client(master_client),
      m_block_manager(new BlockManager()),
      m_thread_pool(new ThreadPool(FLAGS_rsfs_snode_thread_min_num,
                                   FLAGS_rsfs_snode_thread_max_num)) {

    snode::SNodeClientAsync::SetRpcOption(
        FLAGS_rsfs_snode_rpc_limit_enabled ? FLAGS_rsfs_snode_rpc_limit_max_inflow : -1,
        FLAGS_rsfs_snode_rpc_limit_enabled ? FLAGS_rsfs_snode_rpc_limit_max_outflow : -1,
        FLAGS_rsfs_snode_rpc_max_pending_buffer_size, FLAGS_rsfs_snode_rpc_work_thread_num);
    SNodeClientAsync::SetThreadPool(m_thread_pool.get());
}

SNodeImpl::~SNodeImpl() {}

bool SNodeImpl::Init() {
    return true;
}

bool SNodeImpl::Exit() {
    return true;
}

bool SNodeImpl::Register() {
    RegisterRequest request;
    RegisterResponse response;

    m_this_sequence_id = kSequenceIDStart;
    request.set_sequence_id(m_this_sequence_id);
    request.mutable_snode_info()->CopyFrom(m_snode_info);

    if (!m_master_client->Register(&request, &response)) {
        LOG(ERROR) << "Rpc failed: register, status = "
            << StatusCodeToString(response.status());
        return false;
    }
    if (response.status() == kMasterOk) {
        LOG(INFO) << "register success: " << response.ShortDebugString();
        m_snode_info.set_status(kSNodeIsRunning);
        ++m_this_sequence_id;
        return true;
    }

    LOG(INFO) << "register fail: " << response.ShortDebugString();
    return false;
}

bool SNodeImpl::Report() {
    ReportRequest request;
    request.set_sequence_id(m_this_sequence_id);
    request.mutable_snode_info()->CopyFrom(m_snode_info);

    int32_t retry = 0;
    while (retry < FLAGS_rsfs_heartbeat_retry_times) {
        ReportResponse response;
        if (!m_master_client->Report(&request, &response)) {
            LOG(ERROR) << "Rpc failed: report, status = "
                << StatusCodeToString(response.status());
        } else if (response.status() == kMasterOk) {
            LOG(INFO) << "report success, response: "
                << response.ShortDebugString();
            if (response.sequence_id() != m_this_sequence_id) {
                LOG(WARNING) << "report is lost";
                m_this_sequence_id = response.sequence_id() + 1;
            } else {
                ++m_this_sequence_id;
            }
            return true;
        } else if (response.status() == kSNodeNotRegistered) {
            if (!Register()) {
                return false;
            }
            return true;
        } else {
            LOG(ERROR) << "report failed: " << response.DebugString();
            ThisThread::Sleep(retry * FLAGS_rsfs_heartbeat_period *
                              FLAGS_rsfs_heartbeat_retry_period_factor);
        }
        ++retry;
    }
    return false;
}

void SNodeImpl::OpenData(const OpenDataRequest* request,
                         OpenDataResponse* response,
                         google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    uint64_t block_id = request->block_id();
    BlockStream::Type stream_type = BlockStream::SEQ_READ;
    if (request->mode() == OpenDataRequest::RANDOM_READ) {
        stream_type = BlockStream::RANDOM_READ;
    } else if (request->mode() == OpenDataRequest::APPEND) {
        stream_type = BlockStream::APPEND;
    }
    if (!m_block_manager->NewBlockStream(block_id, stream_type)) {
        LOG(ERROR) << "fail to new block stream [id: " << block_id
            << ", type: " << request->mode() << "]";
        response->set_status(kIOError);
        done->Run();
        return;
    }
    response->set_status(kSNodeOk);
    done->Run();
}

void SNodeImpl::CloseData(const CloseDataRequest* request,
                          CloseDataResponse* response,
                          google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    uint64_t block_id = request->block_id();
    BlockStream* stream = m_block_manager->GetBlockStream(block_id);
    if (!stream) {
        LOG(INFO) << "stream of block [id: " << block_id << "] not exist";
        response->set_status(kSNodeNotStream);
        done->Run();
        return;
    }
    if (!m_block_manager->RemoveBlockStream(request->block_id())) {
        LOG(WARNING) << "fail to remove block stream [id: "
            << request->block_id() << "]";
    }
    response->set_status(kSNodeOk);
    done->Run();
}

void SNodeImpl::WriteData(const WriteDataRequest* request,
                         WriteDataResponse* response,
                         google::protobuf::Closure* done) {
    LOG(INFO) << "WriteData: receive payload size: " << request->payload().size();
    response->set_sequence_id(request->sequence_id());
    uint64_t block_id = request->block_id();
    BlockStream* stream = m_block_manager->GetBlockStream(block_id);
    if (!stream) {
        LOG(INFO) << "stream of block [id: " << block_id << "] not exist";
        response->set_status(kSNodeNotStream);
        done->Run();
        return;
    }
    if (stream->GetType() != BlockStream::APPEND) {
        LOG(ERROR) << "wrong stream type [stream type: "
            << stream->GetType() << "]";
        response->set_status(kSNodeErrStream);
        done->Run();
        return;
    }

    FileStream* file = stream->GetFileStream();
    CHECK(file);
    FileErrorCode err = kFileSuccess;
    if (!file->Write(request->payload().data(), request->payload().size(), &err)) {
        LOG(ERROR) << "fail to write data in block [id: " << block_id
            << "], err_code: " << err;
        response->set_status(kIOError);
    } else {
        response->set_status(kSNodeOk);
    }
    stream->DecRef();
    done->Run();
}

void SNodeImpl::ReadData(const ReadDataRequest* request,
                         ReadDataResponse* response,
                         google::protobuf::Closure* done) {
    response->set_sequence_id(request->sequence_id());
    uint64_t block_id = request->block_id();
    BlockStream* stream = m_block_manager->GetBlockStream(block_id);
    if (!stream) {
        LOG(INFO) << "stream of block [id: " << block_id << "] not exist";
        response->set_status(kSNodeNotStream);
        done->Run();
        return;
    }
    if (request->type() == ReadDataRequest::SEQ_READ) {
        ReadDataSequencial(stream, request->payload_size(), response);
    } else {
        ReadDataRandom(stream, request->payload_size(), request->offset(), response);
    }
    stream->DecRef();
    done->Run();
}

bool SNodeImpl::ReadDataSequencial(BlockStream* stream, uint64_t size,
                                   ReadDataResponse* response) {
    if (stream->GetType() != BlockStream::SEQ_READ) {
        LOG(ERROR) << "wrong stream type [stream type: "
            << stream->GetType() << "]";
        response->set_status(kSNodeErrStream);
        return false;
    }
    FileStream* file = stream->GetFileStream();
    char* buf = new char[size];
    FileErrorCode err = kFileSuccess;
    if (size != file->Read(buf, size, &err)) {
        LOG(ERROR) << "fail to seq-read data, err: " << err;
        response->set_status(kIOError);
        return false;
    }
    response->set_payload(buf, size);
    response->set_status(kSNodeOk);
    delete buf;
    return true;
}

bool SNodeImpl::ReadDataRandom(BlockStream* stream, uint64_t size, uint64_t offset,
                               ReadDataResponse* response) {
    if (stream->GetType() != BlockStream::RANDOM_READ) {
        LOG(ERROR) << "wrong stream type [stream type: "
            << stream->GetType() << "]";
        response->set_status(kSNodeErrStream);
        return false;
    }
    FileStream* file = stream->GetFileStream();
    char* buf = new char[size];
    FileErrorCode err = kFileSuccess;
    int64_t read_count = file->Read(buf, size, &err);
    if (size != read_count) {
        LOG(ERROR) << "fail to random-read data, err: " << err
            << " (expected: " << size << ", actual: " << read_count
            << ", offset: " << offset << ")";
        response->set_status(kIOError);
        return false;
    }
    response->set_payload(buf, read_count);
    response->set_status(kSNodeOk);
    delete buf;
    return true;
}



} // namespace snode
} // namespace rsfs
