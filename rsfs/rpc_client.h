// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//


#ifndef RSFS_RPC_CLIENT_H
#define RSFS_RPC_CLIENT_H

#include <string>

#include "bobby/bobby_client.h"
#include "bobby/rpccontroller.h"
#include "common/base/scoped_ptr.h"
#include "common/net/ip_address.h"
#include "common/thread/this_thread.h"

#include "rsfs/proto/status_code.pb.h"
#include "rsfs/proto/proto_helper.h"

namespace rsfs {

template<class ServerType>
class RpcClient {
public:
    RpcClient(const std::string& addr = "",
              int32_t wait_time = 0,
              int32_t rpc_timeout = 0,
              int32_t retry_times = 0)
    : m_wait_time(wait_time), m_rpc_timeout(rpc_timeout),
      m_retry_times(retry_times) {
        ResetClient(addr);
    }
    virtual ~RpcClient() {}

    std::string GetConnectAddr() const {
        return m_server_addr;
    }

protected:
    virtual void ResetClient(const std::string& server_addr) {
        if (m_server_addr == server_addr) {
            VLOG(10) << "address [" << server_addr << "] not be applied";
            return;
        }

        IpAddress ip_address(server_addr);
        if (!ip_address.IsValid()) {
            LOG(ERROR) << "invalid address: " << server_addr;
            return;
        }
        m_bobby_client.Reset(ip_address);
        m_server_client.reset(new ServerType(m_bobby_client.GetChannel()));

        m_server_addr = server_addr;
        VLOG(10) << "reset connected address to: " << server_addr;
    }

    template <class Request, class Response, class Callback>
    bool SendMessageWithRetry(void(ServerType::*func)(
                              google::protobuf::RpcController*,
                              const Request*, Response*, Callback*),
                              const Request* request, Response* response,
                              Callback* closure, const std::string& tips) {
        uint32_t wait_time = m_wait_time;
        uint32_t rpc_timeout = m_rpc_timeout;
        scoped_ptr<bobby::gpb::RpcController> rpc_controller(m_bobby_client.NewController());
        for (int32_t retry = 0; retry < m_retry_times; ++retry) {
            m_bobby_client.SetTimeout(rpc_controller.get(), rpc_timeout);
            if (retry == 0 || PollAndResetServerAddr()) {

                CHECK_NOTNULL(m_server_client.get());
                (m_server_client.get()->*func)(rpc_controller.get(),
                                               request, response, closure);

                if (!rpc_controller->Failed()) {
                    if (IsRetryStatus(response->status()) && retry < m_retry_times - 1) {
                        LOG(WARNING) << tips << ": Server is busy [status = "
                            << StatusCodeToString(response->status())
                            << "], retry after " << rpc_timeout << " msec";
                        ThisThread::Sleep(rpc_timeout);
                    } else {
                        return true;
                    }
                } else {
                    LOG(ERROR) << "RpcRequest failed: " << tips
                        << ". Reason: " << rpc_controller->ErrorText()
                        << " (retry = " << retry << ")";
                    ThisThread::Sleep(wait_time);
                }
            } else {
                ThisThread::Sleep(wait_time);
            }
            rpc_controller->Reset();
            wait_time *= 2;
            rpc_timeout *= 2;
        }
        return false;
    }

    virtual bool PollAndResetServerAddr() {
        return true;
    }

    virtual bool IsRetryStatus(const StatusCode& status) {
        return false;
    }

private:
    bobby::BobbyClient m_bobby_client;
    scoped_ptr<ServerType> m_server_client;

    std::string m_server_addr;
    int32_t m_wait_time;
    int32_t m_rpc_timeout;
    int32_t m_retry_times;
};

} // namespace rsfs

#endif // RSFS_RPC_CLIENT_H
