// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#include "rsfs/sdk/rsfs_sdk.h"

#include "sofa/pbrpc/pbrpc.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"

#include "rsfs/master/master_client.h"
#include "rsfs/proto/proto_helper.h"
#include "rsfs/sdk/sdk_utils.h"
#include "rsfs/snode/snode_client.h"
#include "rsfs/types.h"
#include "rsfs/utils/utils_cmd.h"

DECLARE_int32(rsfs_sdk_rscode_mm);
DECLARE_int32(rsfs_sdk_rscode_kk);
DECLARE_int32(rsfs_sdk_rscode_block_size);
DECLARE_int32(rsfs_sdk_rscode_tail_backup_num);
DECLARE_int32(rsfs_sdk_write_retry_times);
DECLARE_int32(rsfs_sdk_read_retry_times);
DECLARE_int32(rsfs_snode_connect_retry_period);

DECLARE_int32(rsfs_sdk_thread_min_num);
DECLARE_int32(rsfs_sdk_thread_max_num);
DECLARE_bool(rsfs_sdk_rpc_limit_enabled);
DECLARE_int32(rsfs_sdk_rpc_limit_max_inflow);
DECLARE_int32(rsfs_sdk_rpc_limit_max_outflow);
DECLARE_int32(rsfs_sdk_rpc_max_pending_buffer_size);
DECLARE_int32(rsfs_sdk_rpc_work_thread_num);
DECLARE_int32(rsfs_sdk_rpc_list_size_limit);

namespace rsfs {
namespace sdk {

REGISTER_RSFS_SDK(RSFS_SDK_PREFIX, RsfsSDK);

RsfsSDK::RsfsSDK()
    : m_master_client(new master::MasterClient()),
      m_rscode(new rscode::RSCode("rsfs_rscode",
                                  FLAGS_rsfs_sdk_rscode_mm,
                                  FLAGS_rsfs_sdk_rscode_kk,
                                  FLAGS_rsfs_sdk_rscode_block_size)),
      m_last_sequence_id(kSequenceIDStart),
      m_cur_node_no(0), m_cur_rsblock_no(0),
      m_remain_block_size(0), m_cur_slice_no(-1),
      m_max_crash_slice_no(-1), m_max_crash_block_num(0),
      m_last_block_buffer(NULL), m_file_mode("r"),
      m_file_size(0), m_file_id(0), m_seq_read_offset(0),
      m_tail_slice_no(-1), m_tail_num(0),
      m_thread_pool(FLAGS_rsfs_sdk_thread_min_num,
                    FLAGS_rsfs_sdk_thread_max_num) {
    snode::SNodeClientAsync::SetThreadPool(&m_thread_pool);
    snode::SNodeClientAsync::SetRpcOption(
        FLAGS_rsfs_sdk_rpc_limit_enabled ? FLAGS_rsfs_sdk_rpc_limit_max_inflow : -1,
        FLAGS_rsfs_sdk_rpc_limit_enabled ? FLAGS_rsfs_sdk_rpc_limit_max_outflow : -1,
        FLAGS_rsfs_sdk_rpc_max_pending_buffer_size, FLAGS_rsfs_sdk_rpc_work_thread_num);
}

RsfsSDK::~RsfsSDK() {}

std::string RsfsSDK::GetImplName() {
    return RSFS_SDK_PREFIX;
}

bool RsfsSDK::OpenImpl(const std::string& file_path,
                       const std::string& mode,
                       ErrorCode* err) {
    OpenFileRequest request;
    OpenFileResponse response;

    request.set_sequence_id(++m_last_sequence_id);
    request.set_file_name(file_path);
    request.set_node_num(m_rscode->GetMK());
    if (mode == "w") {
        request.set_type(OpenFileRequest::WRITE);
        m_remain_block_size = FLAGS_rsfs_sdk_rscode_block_size;
        m_last_block_buffer.reset(new char[FLAGS_rsfs_sdk_rscode_block_size]);
        m_cur_slice_no = 0;
    } else {
        request.set_type(OpenFileRequest::RANDOM_READ);
        m_remain_block_size = 0;
        m_last_block_buffer.reset(new char[FLAGS_rsfs_sdk_rscode_block_size *
                                  m_rscode->GetMK()]);
    }

    if (!m_master_client->OpenFile(&request, &response)
        || response.status() != kMasterOk) {
        LOG(ERROR) << "rpc fail to open file: " << file_path
            << ", err: " << StatusCodeToString(response.status());
        err->SetFailed(ErrorCode::kSystem, "rpc fail to open file");
        return false;
    }

    m_file_name = file_path;
    m_file_id = response.fid();
    m_file_mode = mode;
    m_file_size = response.file_size();
    m_tail_slice_no = response.tail_slice();
    m_tail_num = response.tail_num();
    m_node_list.CopyFrom(response.nodes());
    m_max_crash_slice_no = response.crash_slice();
    m_max_crash_block_num = response.crash_num();
    CHECK(m_node_list.size() > 0);
    PallelOpenDataFile();
    return true;
}

bool RsfsSDK::Close(ErrorCode* err) {
    CloseFileRequest request;
    CloseFileResponse response;
    request.set_file_size(m_file_size);

    HandleTailBlocks();
    PallelCloseDataFile();

    request.set_sequence_id(++m_last_sequence_id);
    request.set_file_name(m_file_name);
    request.set_tail_slice(m_cur_slice_no);
    request.set_tail_num(m_cur_rsblock_no);
    request.set_crash_slice(m_max_crash_slice_no);
    request.set_crash_num(m_max_crash_block_num);

    if (!m_master_client->CloseFile(&request, &response)
        || response.status() != kMasterOk) {
        LOG(ERROR) << "rpc fail to close file: " << m_file_name
            << ", err: " << StatusCodeToString(response.status());
        err->SetFailed(ErrorCode::kSystem, "rpc fail to close file");
        return false;
    }
    return true;
}

bool RsfsSDK::ListImpl(const std::string& start, const std::string& end,
                       std::string* last_one, std::vector<TreeNode>* list,
                       ErrorCode* err) {
    ListFileRequest request;
    ListFileResponse response;

    request.set_sequence_id(++m_last_sequence_id);
    request.set_path_start(start);
    request.set_path_end(end);
    request.set_limit(FLAGS_rsfs_sdk_rpc_list_size_limit * 1024);

    if (!m_master_client->ListFile(&request, &response)
        || response.status() != kMasterOk) {
        LOG(ERROR) << "rpc fail to List file at range: [" << start
            << ", " << end << "], err: " << StatusCodeToString(response.status());
        err->SetFailed(ErrorCode::kSystem, "rpc fail to close file");
        return false;
    }

    for (int32_t i = 0; i < response.metas_size(); ++i) {
        list->push_back(response.metas(i));
    }
    if (response.last_one() == "" || response.last_one() >= end) {
        *last_one = end;
    } else {
        *last_one = response.last_one();
    }
    return true;
}

int64_t RsfsSDK::GetSize(ErrorCode* err) {
    return m_file_size;
}

#if 0
int64_t RsfsSDK::Read(void* buf, uint32_t buf_size, ErrorCode* err) {
    AutoResetEvent done_event;
    int64_t read_count = 0;

    uint32_t receive_size = m_remain_block_size;
    if (receive_size > buf_size) {
        receive_size = buf_size;
    }
    if (receive_size > 0) {
        uint32_t start_point = FLAGS_rsfs_sdk_rscode_block_size -
            m_remain_block_size;
        memcpy(buf, m_last_block_buffer.get() + start_point, receive_size);
        read_count += receive_size;
    }

    if (buf_size == receive_size) {
        return read_count;
    }

    ReadDataRequest* request = new ReadDataRequest;
    ReadDataResponse* response = new ReadDataResponse;
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(m_cur_node_no);
    request->set_type(ReadDataRequest::SEQ_READ);
    request->set_payload_size(FLAGS_rsfs_sdk_rscode_block_size);

    Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::ReadCallback,
                   buf + receive_size, buf_size - receive_size, &done_event,
                   &read_count, err, FLAGS_rsfs_sdk_read_retry_times);

    snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
    node_client.ReadData(request, response, done);
    done_event.Wait();

    return read_count;
}
#else

int64_t RsfsSDK::Read(void* buf, uint32_t buf_size, ErrorCode* err) {
    int64_t read_count = Read(buf, buf_size, m_seq_read_offset, err);
    if (read_count > 0) {
        m_seq_read_offset += read_count;
    }
    return read_count;
}

#endif
int64_t RsfsSDK::Read(void* buf, uint32_t buf_size, int64_t offset,
                      ErrorCode* err) {
    uint32_t slice_start_block = 0;
    uint64_t offset_in_slice = 0;
    if (!GetSliceLocation(offset, &slice_start_block, &offset_in_slice)) {
        return -1;
    }
    uint32_t slice_length = FLAGS_rsfs_sdk_rscode_block_size * m_rscode->GetM();
    uint32_t remain_size = m_file_size - offset;
    if (remain_size > buf_size) {
        remain_size = buf_size;
    }
    while (remain_size > 0) {
//         if (m_cur_slice_no != slice_start_block
//             && ParallelLoadSlice(0)) {
//             m_cur_slice_no = slice_start_block;
//         }
        if (m_cur_slice_no != slice_start_block) {
            bool load = false;
            if (slice_start_block == m_tail_slice_no) {
                load = ParallelLoadTailBlock(0);
            } else {
                load = ParallelLoadSlice(0);
            }
            if (!load) {
                LOG(INFO) << "fail to para-load slice #" << slice_start_block;
                break;
            }
            m_cur_slice_no = slice_start_block;
        }
        uint32_t read_count =
            (remain_size > (slice_length - offset_in_slice))?
            (slice_length - offset_in_slice):remain_size;
        memcpy(buf, m_last_block_buffer.get() + offset_in_slice, read_count);
        remain_size -= read_count;
    }

    return buf_size - remain_size;
}

int64_t RsfsSDK::Write(void* buf, uint32_t buf_size, ErrorCode* err) {
    AutoResetEvent done_event;
    int64_t write_count = 0;

    WriteDataRequest* request = new WriteDataRequest;
    WriteDataResponse* response = new WriteDataResponse;
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(BlockFileName(m_file_id, m_cur_node_no));

    uint32_t send_size = m_remain_block_size;
    if (send_size > buf_size) {
        send_size = buf_size;
    }
    memcpy(m_last_block_buffer.get(), buf, send_size);
    request->set_payload(buf, send_size);

    Closure<void, WriteDataRequest*, WriteDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::WriteCallback,
                   buf + send_size, buf_size - send_size, &done_event,
                   &write_count, err, FLAGS_rsfs_sdk_write_retry_times);

    snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
    node_client.WriteData(request, response, done);
    done_event.Wait();

    return write_count;
}

void RsfsSDK::ReadCallback(void* buf, uint32_t buf_size,
                           AutoResetEvent* done_event, int64_t* read_count,
                           ErrorCode* err, int32_t retry,
                           ReadDataRequest* request, ReadDataResponse* response,
                           bool failed, int error_code) {
    if (failed || response->status() != kSNodeOk) {
        LOG(WARNING) << "fail to read data, rpc status: "
            << StatusCodeToString(response->status());
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to read data after " << FLAGS_rsfs_sdk_write_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            err->SetFailed(ErrorCode::kSystem, "rpc fail to read data");
            done_event->Set();
            *read_count = -1;
        } else {
            int64_t wait_time = FLAGS_rsfs_snode_connect_retry_period *
                (FLAGS_rsfs_sdk_read_retry_times - retry);
            ThisThread::Sleep(wait_time);

            Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done =
                NewClosure(this, &RsfsSDK::ReadCallback,
                           buf, buf_size, done_event, read_count, err,
                           retry - 1);

            snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
            node_client.ReadData(request, response, done);
        }
        return;
    }
}

void RsfsSDK::WriteCallback(void* buf, uint32_t buf_size,
                            AutoResetEvent* done_event, int64_t* write_count,
                            ErrorCode* err, int32_t retry,
                            WriteDataRequest* request, WriteDataResponse* response,
                            bool failed, int error_code) {
    if (failed || response->status() != kSNodeOk) {
        LOG(WARNING) << "fail to write data, rpc status: "
            << StatusCodeToString(response->status());
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to write data after " << FLAGS_rsfs_sdk_write_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            err->SetFailed(ErrorCode::kSystem, "rpc fail to write data");
            done_event->Set();
            *write_count = -1;
        } else {
            int64_t wait_time = FLAGS_rsfs_snode_connect_retry_period *
                (FLAGS_rsfs_sdk_write_retry_times - retry);
            ThisThread::Sleep(wait_time);

            Closure<void, WriteDataRequest*, WriteDataResponse*, bool, int>* done =
                NewClosure(this, &RsfsSDK::WriteCallback,
                           buf, buf_size, done_event, write_count, err,
                           retry - 1);

            snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
            node_client.WriteData(request, response, done);
        }
        return;
    }
    LOG(INFO) << "rs block #" << m_cur_rsblock_no
        << ", md5: " << utils::GetMd5(request->payload().data(), request->payload().size());
    int64_t payload_size = request->payload().size();
    if (m_cur_rsblock_no < m_rscode->GetM()) {
        // only for data block
        *write_count += payload_size;
        m_file_size += payload_size;
    }
    m_remain_block_size -= payload_size;

    if (m_remain_block_size > 0) {
        done_event->Set();
        delete request;
        delete response;
        return;
    }

    m_cur_node_no = (m_cur_node_no + 1) % m_node_list.size();
    m_remain_block_size = FLAGS_rsfs_sdk_rscode_block_size;
    if (m_cur_rsblock_no < m_rscode->GetM()) {
        // only for data block
        m_rscode->AddBlockToCache(m_cur_rsblock_no, m_last_block_buffer.get());
    }
    m_cur_rsblock_no = (m_cur_rsblock_no + 1) % m_rscode->GetMK();
    if (m_cur_rsblock_no == 0) {
        m_cur_slice_no++;
    }

    uint32_t send_size = 0;
    if (m_cur_rsblock_no >= m_rscode->GetM() && m_cur_rsblock_no < m_rscode->GetMK()) {
        if (m_cur_rsblock_no == m_rscode->GetM()) {
            m_rscode->CreateParityBlock();
        }
        // send parity block
        CHECK(m_rscode->GetBlock(m_cur_rsblock_no, m_last_block_buffer.get()));
        request->set_payload(m_last_block_buffer.get(), FLAGS_rsfs_sdk_rscode_block_size);
    } else if (buf_size > 0) {
        // send data block
        send_size = m_remain_block_size;
        if (send_size > buf_size) {
            send_size = buf_size;
        }
        memcpy(m_last_block_buffer.get(), buf, send_size);
        request->set_payload(buf, send_size);
    } else {
        VLOG(5) << "send data success";
        delete request;
        delete response;
        done_event->Set();
        return;
    }
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(BlockFileName(m_file_id, m_cur_node_no));

    Closure<void, WriteDataRequest*, WriteDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::WriteCallback,
                   buf + send_size, buf_size - send_size, done_event,
                   write_count, err, FLAGS_rsfs_sdk_write_retry_times);

    snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
    node_client.WriteData(request, response, done);
}

bool RsfsSDK::RpcChannelHealth(int32_t err_code) {
    return err_code != sofa::pbrpc::RPC_ERROR_CONNECTION_CLOSED
        && err_code != sofa::pbrpc::RPC_ERROR_SERVER_SHUTDOWN
        && err_code != sofa::pbrpc::RPC_ERROR_SERVER_UNREACHABLE
        && err_code != sofa::pbrpc::RPC_ERROR_SERVER_UNAVAILABLE;
}

bool RsfsSDK::GetSliceLocation(int64_t offset, uint32_t* slice_start_block,
                               uint64_t* offset_in_slice) {
    if (offset >= m_file_size) {
        LOG(ERROR) << "invalid file offset: " << offset
            << "[file size: " << m_file_size << "]";
        return false;
    }
    uint32_t slice_length = FLAGS_rsfs_sdk_rscode_block_size * m_rscode->GetM();
    int64_t file_offset = offset;
    if (offset < 0) {
        file_offset = m_file_size - offset;
    }
    *slice_start_block = file_offset / slice_length;
    *offset_in_slice = file_offset % slice_length;
    return true;
}

bool RsfsSDK::PallelOpenDataFile() {
    AutoResetEvent done_event;
    scoped_ptr<utils::IntMap> open_status(new utils::IntMap(m_node_list.size(), -1));
    for (uint32_t i = 0; i < m_node_list.size(); ++i) {
        Closure<void>* callback =
            NewClosure(this, &RsfsSDK::OpenDataFile,
                       m_node_list.Get(i).addr(), i, open_status.get(),
                       &done_event);
        m_thread_pool.AddTask(callback);
    }
    uint32_t wait_retry = 0;
    while (open_status->GetSetNum() < m_node_list.size()
           && wait_retry < 100) {
        if (!done_event.Wait(500)) {
            wait_retry++;
        } else {
            wait_retry = 0;
        }
        LOG(INFO) << "retry_count: " << wait_retry
            << ", set num: " << open_status->GetSetNum();
    }
    return open_status->GetSetNum() == m_node_list.size();
}

void RsfsSDK::OpenDataFile(std::string addr, uint32_t block_no,
                           utils::IntMap* open_status, AutoResetEvent* done_event) {
    LOG(INFO) << "open block #" << block_no << " on node (" << addr << ")";
    OpenDataRequest* request = new OpenDataRequest;
    OpenDataResponse* response = new OpenDataResponse;
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(BlockFileName(m_file_id, block_no));

    if (m_file_mode == "w") {
        request->set_mode(OpenDataRequest::APPEND);
    } else {
        request->set_mode(OpenDataRequest::RANDOM_READ);
    }
    Closure<void, OpenDataRequest*, OpenDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::OpenDataFileCallback,
                   addr, block_no, open_status, done_event,
                   FLAGS_rsfs_sdk_read_retry_times);

    snode::SNodeClientAsync node_client(addr);
    node_client.OpenData(request, response, done);
}

void RsfsSDK::OpenDataFileCallback(std::string addr, uint32_t block_no, utils::IntMap* open_status,
                                   AutoResetEvent* done_event, int32_t retry,
                                   OpenDataRequest* request, OpenDataResponse* response,
                                   bool failed, int error_code) {
    if (failed || response->status() != kSNodeOk) {
        LOG(WARNING) << "fail to open data, rpc status: "
            << StatusCodeToString(response->status());
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to open data after " << FLAGS_rsfs_sdk_write_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            open_status->Set(block_no, 0);
            done_event->Set();
        } else {
            int64_t wait_time = FLAGS_rsfs_snode_connect_retry_period *
                (FLAGS_rsfs_sdk_read_retry_times - retry);
            ThisThread::Sleep(wait_time);

            Closure<void, OpenDataRequest*, OpenDataResponse*, bool, int>* done =
                NewClosure(this, &RsfsSDK::OpenDataFileCallback,
                           addr, block_no, open_status, done_event,
                           retry - 1);

            snode::SNodeClientAsync node_client(addr);
            node_client.OpenData(request, response, done);
        }
        return;
    }

    delete request;
    delete response;

    open_status->Set(block_no, 1);
    done_event->Set();
    LOG(INFO) << "open success, block #" << block_no;
}

bool RsfsSDK::PallelCloseDataFile() {
    AutoResetEvent done_event;
    scoped_ptr<utils::IntMap> close_status(new utils::IntMap(m_node_list.size(), -1));
    for (uint32_t i = 0; i < m_node_list.size(); ++i) {
        Closure<void>* callback =
            NewClosure(this, &RsfsSDK::CloseDataFile,
                       m_node_list.Get(i).addr(), i, close_status.get(),
                       &done_event);
        m_thread_pool.AddTask(callback);
    }
    uint32_t wait_retry = 0;
    while (close_status->GetSetNum() < m_node_list.size()
           && wait_retry < 100) {
        if (!done_event.Wait(500)) {
            wait_retry++;
        } else {
            wait_retry = 0;
        }
        LOG(INFO) << "retry_count: " << wait_retry
            << ", set num: " << close_status->GetSetNum();
    }
    return close_status->GetSetNum() == m_node_list.size();
}

void RsfsSDK::CloseDataFile(std::string addr, uint32_t block_no,
                            utils::IntMap* close_status, AutoResetEvent* done_event) {
    CloseDataRequest* request = new CloseDataRequest;
    CloseDataResponse* response = new CloseDataResponse;
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(BlockFileName(m_file_id, block_no));

    Closure<void, CloseDataRequest*, CloseDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::CloseDataFileCallback,
                   addr, block_no, close_status, done_event,
                   FLAGS_rsfs_sdk_read_retry_times);

    snode::SNodeClientAsync node_client(addr);
    node_client.CloseData(request, response, done);
}

void RsfsSDK::CloseDataFileCallback(std::string addr, uint32_t block_no, utils::IntMap* close_status,
                                    AutoResetEvent* done_event, int32_t retry,
                                    CloseDataRequest* request, CloseDataResponse* response,
                                    bool failed, int error_code) {
    if (failed || response->status() != kSNodeOk) {
        LOG(WARNING) << "fail to close data, rpc status: "
            << StatusCodeToString(response->status());
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to close data after " << FLAGS_rsfs_sdk_write_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            close_status->Set(block_no, 0);
            done_event->Set();
        } else {
            int64_t wait_time = FLAGS_rsfs_snode_connect_retry_period *
                (FLAGS_rsfs_sdk_read_retry_times - retry);
            ThisThread::Sleep(wait_time);

            Closure<void, CloseDataRequest*, CloseDataResponse*, bool, int>* done =
                NewClosure(this, &RsfsSDK::CloseDataFileCallback,
                           addr, block_no, close_status, done_event,
                           retry - 1);

            snode::SNodeClientAsync node_client(addr);
            node_client.CloseData(request, response, done);
        }
        return;
    }

    delete request;
    delete response;

    close_status->Set(block_no, 1);
    done_event->Set();
    LOG(INFO) << "close success, block #" << block_no;
}

bool RsfsSDK::ParallelLoadSlice(uint32_t slice_no) {
    m_rscode->CleanCache();
    m_rscode->CleanBlock();
    AutoResetEvent wait_event;
    scoped_ptr<utils::IntMap> load_status(new utils::IntMap(m_rscode->GetMK(), -1));
    for (uint32_t i = 0; i < m_rscode->GetMK(); ++i) {
        uint32_t load_node_no = i % m_node_list.size();
        Closure<void>* callback =
            NewClosure(this, &RsfsSDK::LoadSliceBlock,
                       load_node_no, i, load_status.get(), &wait_event);
        m_thread_pool.AddTask(callback);
    }
    uint32_t wait_retry = 0;
    while (load_status->GetSetNum() < m_rscode->GetMK()
           && wait_retry < 100) {
        if (!wait_event.Wait(500)) {
            wait_retry++;
        } else {
            wait_retry = 0;
        }
        LOG(INFO) << "retry_count: " << wait_retry;
    }

    if (load_status->Sum(0) > m_max_crash_block_num) {
        m_max_crash_block_num = load_status->Sum(0);
        m_max_crash_slice_no = slice_no;
    }
    if (load_status->Sum(1) == m_rscode->GetMK()) {
        // success
        return true;
    } else if (load_status->Sum(0) > m_rscode->GetK()) {
        LOG(ERROR) << "the number of crashed block: " << load_status->Sum(0)
            << ", beyond: " << m_rscode->GetK();
        return false;
    }
    // recove the crash block in slice
    CHECK(m_rscode->RecoverLostBlock());
    CHECK(m_rscode->CreateParityBlock());
    int32_t success_count = 0;
    for (int32_t no = 0; no < m_rscode->GetMK(); ++no) {
        if (load_status->IsSet(no, 1)) {
            success_count++;
            continue;
        }
        char* block_addr = m_last_block_buffer.get() +
            FLAGS_rsfs_sdk_rscode_block_size * no;
        CHECK(m_rscode->GetBlock(no, block_addr))
            << ", fail to recover missing slice block #" << no;
        load_status->Set(no, 1);
        success_count++;
        LOG(INFO) << "recover block #" << no << ", md5: "
            << utils::GetMd5(block_addr, FLAGS_rsfs_sdk_rscode_block_size);
    }
    return success_count == m_rscode->GetMK();
}

void RsfsSDK::LoadSliceBlock(uint32_t node_no, uint32_t block_no,
                             utils::IntMap* load_status, AutoResetEvent* done_event) {
    ReadDataRequest* request = new ReadDataRequest;
    ReadDataResponse* response = new ReadDataResponse;
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(BlockFileName(m_file_id, block_no));
    request->set_type(ReadDataRequest::RANDOM_READ);
    request->set_payload_size(FLAGS_rsfs_sdk_rscode_block_size);

    Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::LoadBlockCallback,
                   node_no, block_no, load_status, done_event,
                   FLAGS_rsfs_sdk_read_retry_times);

    snode::SNodeClientAsync node_client(m_node_list.Get(node_no).addr());
    node_client.ReadData(request, response, done);
    LOG(INFO) << "try load block #" << block_no
        << " from node #" << node_no << " (" << node_client.GetConnectAddr() << ")";
}

void RsfsSDK::LoadBlockCallback(uint32_t node_no, uint32_t block_no,
                                utils::IntMap* load_status,
                                AutoResetEvent* done_event, int32_t retry,
                                ReadDataRequest* request, ReadDataResponse* response,
                                bool failed, int error_code) {
    if (failed || response->status() != kSNodeOk) {
        LOG(WARNING) << "fail to read data, rpc status: "
            << StatusCodeToString(response->status())
            << " [node #" << node_no << ", block #" << block_no << "]";
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to read data after " << FLAGS_rsfs_sdk_write_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            load_status->Set(block_no, 0);
            done_event->Set();
        } else {
            int64_t wait_time = FLAGS_rsfs_snode_connect_retry_period *
                (FLAGS_rsfs_sdk_read_retry_times - retry);
            ThisThread::Sleep(wait_time);

            Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done =
                NewClosure(this, &RsfsSDK::LoadBlockCallback,
                           node_no, block_no, load_status, done_event,
                           retry - 1);

            snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
            node_client.ReadData(request, response, done);
        }
        return;
    }

    LOG(INFO) << "rpc read success. block #" << block_no << " from node #" << node_no
        << ", payload size: " << response->payload().size();

    uint32_t block_offset = FLAGS_rsfs_sdk_rscode_block_size * block_no;
    memcpy(m_last_block_buffer.get() + block_offset,
           response->payload().data(),
           response->payload().size());

    m_rscode->AddBlock(block_no, response->payload().data());

    delete request;
    delete response;

    load_status->Set(block_no, 1);
    done_event->Set();

    LOG(INFO) << "load success. block #" << block_no << " from node #" << node_no;
}

void RsfsSDK::HandleTailBlocks() {
    if (m_remain_block_size > 0) {
        LOG(INFO) << "deal with the unseal block";
        ErrorCode err;
        CHECK(m_remain_block_size == Write(
                m_last_block_buffer.get() + FLAGS_rsfs_sdk_rscode_block_size -
                m_remain_block_size, m_remain_block_size, &err));
    }

    if (m_file_mode != "w"
        || m_cur_rsblock_no > m_rscode->GetM()) {
        m_cur_slice_no = -1;
        return;
    }
    for (int32_t i = 0; i < FLAGS_rsfs_sdk_rscode_kk; ++i) {
        ParallelDumpTailBlock(m_cur_node_no);
    }
}

bool RsfsSDK::ParallelDumpTailBlock(uint32_t start_node_no) {
    AutoResetEvent wait_event;
    scoped_ptr<utils::IntMap> dump_status(new utils::IntMap(m_node_list.size(), -1));
    for (uint32_t i = 0; i < m_cur_rsblock_no; ++i) {
        uint32_t dump_node_no = (start_node_no + i) % m_node_list.size();
        Closure<void>* callback =
            NewClosure(this, &RsfsSDK::DumpTailBlock,
                       dump_node_no, i, dump_status.get(), &wait_event);
        m_thread_pool.AddTask(callback);
    }
    uint32_t wait_retry = 0;
    while (dump_status->GetSetNum() < m_cur_rsblock_no
           && wait_retry < 100) {
        if (!wait_event.Wait(500)) {
            wait_retry++;
        } else {
            wait_retry = 0;
        }
        LOG(INFO) << "retry_count: " << wait_retry;
    }
    m_cur_node_no += m_cur_rsblock_no;
    return dump_status->GetSetNum() == m_cur_rsblock_no;
}

void RsfsSDK::DumpTailBlock(uint32_t node_no, uint32_t rsblock_no,
                            utils::IntMap* dump_status, AutoResetEvent* done_event) {
    WriteDataRequest* request = new WriteDataRequest;
    WriteDataResponse* response = new WriteDataResponse;
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(BlockFileName(m_file_id, node_no));

    CHECK(m_rscode->GetBlockFromCache(rsblock_no, m_last_block_buffer.get()));
    request->set_payload(m_last_block_buffer.get(), FLAGS_rsfs_sdk_rscode_block_size);

    Closure<void, WriteDataRequest*, WriteDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::DumpTailBlockCallback,
                   node_no, rsblock_no, dump_status, done_event,
                   FLAGS_rsfs_sdk_write_retry_times);

    snode::SNodeClientAsync node_client(m_node_list.Get(node_no).addr());
    node_client.WriteData(request, response, done);
}

void RsfsSDK::DumpTailBlockCallback(uint32_t node_no, uint32_t block_no,
                                    utils::IntMap* dump_status,
                                    AutoResetEvent* done_event, int32_t retry,
                                    WriteDataRequest* request, WriteDataResponse* response,
                                    bool failed, int error_code) {
    if (failed || response->status() != kSNodeOk) {
        LOG(WARNING) << "fail to read data, rpc status: "
            << StatusCodeToString(response->status())
            << " [node #" << node_no << ", block #" << block_no << "]";
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to read data after " << FLAGS_rsfs_sdk_write_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            dump_status->Set(block_no, 0);
            done_event->Set();
        } else {
            int64_t wait_time = FLAGS_rsfs_snode_connect_retry_period *
                (FLAGS_rsfs_sdk_write_retry_times - retry);
            ThisThread::Sleep(wait_time);

            Closure<void, WriteDataRequest*, WriteDataResponse*, bool, int>* done =
                NewClosure(this, &RsfsSDK::DumpTailBlockCallback,
                           node_no, block_no, dump_status, done_event,
                           retry - 1);

            snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
            node_client.WriteData(request, response, done);
        }
        return;
    }
    delete request;
    delete response;

    dump_status->Set(block_no, 1);
    done_event->Set();

    LOG(INFO) << "para-write success. block #" << block_no << " from node #" << node_no;
}

bool RsfsSDK::ParallelLoadTail(uint32_t slice_no) {
    uint32_t retry = 0;
    while (retry < FLAGS_rsfs_sdk_rscode_kk + 1
           && !ParallelLoadTailBlock(m_tail_num * retry)) {
        retry++;
    }
    if (retry > m_max_crash_block_num) {
        m_max_crash_block_num = retry;
        m_max_crash_slice_no = slice_no;
    }
    return retry <= FLAGS_rsfs_sdk_rscode_kk;
}

bool RsfsSDK::ParallelLoadTailBlock(uint32_t start_no) {
    AutoResetEvent wait_event;
    scoped_ptr<utils::IntMap> load_status(new utils::IntMap(m_rscode->GetMK(), -1));
    for (uint32_t i = 0; i < m_tail_num; ++i) {
        uint32_t load_node_no = (start_no + i) % m_node_list.size();
        Closure<void>* callback =
            NewClosure(this, &RsfsSDK::LoadTailBlock,
                       load_node_no, i, load_status.get(), &wait_event);
        m_thread_pool.AddTask(callback);
    }
    uint32_t wait_retry = 0;
    while (load_status->GetSetNum() < m_tail_num
           && wait_retry < 100) {
        if (!wait_event.Wait(500)) {
            wait_retry++;
        } else {
            wait_retry = 0;
        }
        LOG(INFO) << "retry_count: " << wait_retry;
    }
    return load_status->Sum(1) == m_tail_num;
}

void RsfsSDK::LoadTailBlock(uint32_t node_no, uint32_t block_no,
                            utils::IntMap* load_status, AutoResetEvent* done_event) {
    ReadDataRequest* request = new ReadDataRequest;
    ReadDataResponse* response = new ReadDataResponse;
    request->set_sequence_id(++m_last_sequence_id);
    request->set_block_id(BlockFileName(m_file_id, block_no));
    request->set_type(ReadDataRequest::RANDOM_READ);
    request->set_payload_size(FLAGS_rsfs_sdk_rscode_block_size);

    Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done =
        NewClosure(this, &RsfsSDK::LoadTailCallback,
                   node_no, block_no, load_status, done_event,
                   0, FLAGS_rsfs_sdk_read_retry_times);

    snode::SNodeClientAsync node_client(m_node_list.Get(node_no).addr());
    node_client.ReadData(request, response, done);
    LOG(INFO) << "try load tail block #" << block_no
        << " from node #" << node_no << " (" << node_client.GetConnectAddr() << ")";
}

void RsfsSDK::LoadTailCallback(uint32_t node_no, uint32_t block_no,
                               utils::IntMap* load_status, AutoResetEvent* done_event,
                               int32_t replica_no, int32_t retry,
                               ReadDataRequest* request, ReadDataResponse* response,
                               bool failed, int error_code) {
    if (failed || response->status() != kSNodeOk) {
        LOG(WARNING) << "fail to read data, rpc status: "
            << StatusCodeToString(response->status())
            << " [node #" << node_no << ", block #" << block_no << "]";
        if (retry <= 0 || !RpcChannelHealth(error_code)) {
            LOG(ERROR) << "fail to read data after " << FLAGS_rsfs_sdk_write_retry_times
                << ", rpc status: " << StatusCodeToString(response->status());
            delete request;
            delete response;
            if (!load_status->IsSet(block_no, 1)) {
                load_status->Set(block_no, 0);
            }
            done_event->Set();
        } else {
            int64_t wait_time = FLAGS_rsfs_snode_connect_retry_period *
                (FLAGS_rsfs_sdk_read_retry_times - retry);
            ThisThread::Sleep(wait_time);

            Closure<void, ReadDataRequest*, ReadDataResponse*, bool, int>* done =
                NewClosure(this, &RsfsSDK::LoadTailCallback,
                           node_no, block_no, load_status, done_event,
                           replica_no, retry - 1);

            snode::SNodeClientAsync node_client(m_node_list.Get(m_cur_node_no).addr());
            node_client.ReadData(request, response, done);
        }
        return;
    }

    LOG(INFO) << "rpc read success. block #" << block_no << " from node #" << node_no
        << ", payload size: " << response->payload().size();

    uint32_t block_offset = FLAGS_rsfs_sdk_rscode_block_size * block_no;
    memcpy(m_last_block_buffer.get() + block_offset,
           response->payload().data(),
           response->payload().size());

    delete request;
    delete response;

    if (!load_status->IsSet(block_no, 1)) {
        load_status->Set(block_no, 1);
    }
    done_event->Set();

    LOG(INFO) << "load tail success. block #" << block_no
        << " from node #" << node_no << " (replica #" << replica_no << ")";
}

} // namespace sdk
} // namespace rsfs
