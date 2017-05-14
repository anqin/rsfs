// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_SDK_RSFS_SDK_H
#define RSFS_SDK_RSFS_SDK_H

#include <string>

#include "common/base/closure.h"
#include "common/base/scoped_ptr.h"
#include "common/base/stdint.h"
#include "common/collection/rscode/rscode.h"
#include "common/lock/event.h"
#include "common/lock/mutex.h"
#include "thirdparty/gflags/gflags.h"
#include "thirdparty/glog/logging.h"


#include "rsfs/master/master_client.h"
#include "rsfs/proto/master_rpc.pb.h"
#include "rsfs/proto/snode_info.pb.h"
#include "rsfs/proto/snode_rpc.pb.h"
#include "rsfs/sdk/error_code.h"
#include "rsfs/sdk/sdk.h"
#include "rsfs/snode/snode_client_async.h"
#include "rsfs/proto/proto_helper.h"
#include "rsfs/utils/int_map.h"

namespace rsfs {

namespace master {
class MasterClient;
}

namespace snode {
class SNodeClientAsync;
}

namespace sdk {

const std::string RSFS_SDK_PREFIX = "/rsfs/";


class RsfsSDK : public SDK {
public:
    RsfsSDK();
    virtual ~RsfsSDK();

    std::string GetImplName();

    bool Close(ErrorCode* err);

    int64_t Read(void* buf, uint32_t buf_size, ErrorCode* err);

    int64_t Read(void* buf, uint32_t buf_size, int64_t offset,
                 ErrorCode* err);

    int64_t Write(void* buf, uint32_t buf_size, ErrorCode* err);

    int64_t GetSize(ErrorCode* err);

protected:
    bool OpenImpl(const std::string& file_path,
                  const std::string& mode,
                  ErrorCode* err);
    bool ListImpl(const std::string& start, const std::string& end,
                  std::string* last_one, std::vector<TreeNode>* list,
                  ErrorCode* err);

private:
    void WriteCallback(void* buf, uint32_t buf_size,
                       AutoResetEvent* done, int64_t* write_count,
                       ErrorCode* err, int32_t retry,
                       WriteDataRequest* request, WriteDataResponse* response,
                       bool failed, int error_code);

    void ReadCallback(void* buf, uint32_t buf_size,
                       AutoResetEvent* done_event, int64_t* read_count,
                       ErrorCode* err, int32_t retry,
                       ReadDataRequest* request, ReadDataResponse* response,
                       bool failed, int error_code);

    void LoadBlockCallback(uint32_t node_no, uint32_t block_no, utils::IntMap* load_status,
                           AutoResetEvent* done_event, int32_t retry,
                           ReadDataRequest* request, ReadDataResponse* response,
                           bool failed, int error_code);

    bool RpcChannelHealth(int32_t err_code);
    bool GetSliceLocation(int64_t offset, uint32_t* slice_start_block,
                          uint64_t* offset_in_slice);
    bool ParallelLoadSlice(uint32_t start_no);
    void LoadSliceBlock(uint32_t node_no, uint32_t block_no,
                        utils::IntMap* load_status, AutoResetEvent* event);

    bool PallelOpenDataFile();
    void OpenDataFile(std::string addr, uint32_t block_no,
                      utils::IntMap* open_status, AutoResetEvent* done_event);
    void OpenDataFileCallback(std::string addr, uint32_t block_no, utils::IntMap* open_status,
                              AutoResetEvent* done_event, int32_t retry,
                              OpenDataRequest* request, OpenDataResponse* response,
                              bool failed, int error_code);

    bool PallelCloseDataFile();
    void CloseDataFile(std::string addr, uint32_t block_no,
                       utils::IntMap* close_status, AutoResetEvent* done_event);
    void CloseDataFileCallback(std::string addr, uint32_t block_no, utils::IntMap* close_status,
                              AutoResetEvent* done_event, int32_t retry,
                              CloseDataRequest* request, CloseDataResponse* response,
                              bool failed, int error_code);

    void HandleTailBlocks();
    bool ParallelDumpTailBlock(uint32_t start_node_no);
    void DumpTailBlock(uint32_t node_no, uint32_t rsblock_no,
                       utils::IntMap* dump_status, AutoResetEvent* done_event);
    void DumpTailBlockCallback(uint32_t node_no, uint32_t buf_size,
                               utils::IntMap* dump_status,
                               AutoResetEvent* done_event, int32_t retry,
                               WriteDataRequest* request, WriteDataResponse* response,
                               bool failed, int error_code);

    bool ParallelLoadTail(uint32_t slice_no);
    bool ParallelLoadTailBlock(uint32_t start_no);
    void LoadTailBlock(uint32_t node_no, uint32_t block_no,
                       utils::IntMap* load_status, AutoResetEvent* done_event);
    void LoadTailCallback(uint32_t node_no, uint32_t block_no,
                          utils::IntMap* load_status, AutoResetEvent* done_event,
                          int32_t replica_no, int32_t retry,
                          ReadDataRequest* request, ReadDataResponse* response,
                          bool failed, int error_code);

private:
    scoped_ptr<master::MasterClient> m_master_client;
    scoped_ptr<rscode::RSCode> m_rscode;

    uint64_t m_last_sequence_id;
    uint32_t m_cur_node_no;
    uint32_t m_cur_rsblock_no;
    uint64_t m_remain_block_size;
    int64_t m_cur_slice_no;
    int64_t m_max_crash_slice_no;
    uint32_t m_max_crash_block_num;
    scoped_array<char> m_last_block_buffer;

    std::string m_file_name;
    std::string m_file_mode;
    int64_t m_file_size;
    uint64_t m_file_id;
    int64_t m_seq_read_offset;
    int64_t m_tail_slice_no;
    uint32_t m_tail_num;
    SNodeInfoList m_node_list;
    ThreadPool m_thread_pool;
};

} // namespace sdk
} // namespace rsfs

#endif // RSFS_SDK_RSFS_SDK_H
