// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:


#include "rsfs/sdk/error_code.h"

namespace rsfs {


ErrorCode::ErrorCode()
    : m_err(kOK) {}

void ErrorCode::SetFailed(ErrorCodeType err, const std::string& reason) {
    m_err = err;
    m_reason = reason;
}

std::string ErrorCode::GetReason() const {
    return m_reason;
}

ErrorCode::ErrorCodeType ErrorCode::GetType() const {
    return m_err;
}

void ErrorCode::Reset() {
    m_err = kOK;
    m_reason = "";
}

const char* ErrorCodeToString(ErrorCode error_code) {
    const char* ret = "Unknown error";
    switch(error_code.GetType()) {
    case ErrorCode::kOK:
        ret = "OK";
        break;
    case ErrorCode::kNotFound:
        ret = "Not Found";
        break;
    case ErrorCode::kBadParam:
        ret = "Bad Parameter";
        break;
    case ErrorCode::kSystem:
        ret = "SystemError";
        break;
    case ErrorCode::kTimeout:
        ret = "Timeout";
        break;
    case ErrorCode::kBusy:
        ret = "SystemBusy";
        break;
    case ErrorCode::kNoQuota:
        ret = "UserNoQuota";
        break;
    case ErrorCode::kNoAuth:
        ret = "UserUnauthorized";
        break;
    case ErrorCode::kNotImpl:
        ret = "Not Implement";
    default:
        ret = "UnkownError";
    }
    return ret;
}


} // namespace rsfs
