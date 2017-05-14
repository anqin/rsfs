// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/rpc_client_async.h"

namespace rsfs {

sofa::pbrpc::RpcChannelOptions RpcClientAsyncBase::m_channel_options;
std::map<std::string, sofa::pbrpc::RpcChannel*> RpcClientAsyncBase::m_rpc_channel_list;
sofa::pbrpc::RpcClientOptions RpcClientAsyncBase::m_rpc_client_options;
sofa::pbrpc::RpcClient RpcClientAsyncBase::m_rpc_client;
Mutex RpcClientAsyncBase::m_mutex;

} // namespace rsfs
