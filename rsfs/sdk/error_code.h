// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_SDK_ERROR_CODE_H
#define RSFS_SDK_ERROR_CODE_H

#include <string>

namespace rsfs {

class ErrorCode {
public:
    enum ErrorCodeType {
        kOK = 0,
        kNotFound,
        kBadParam,
        kSystem,
        kTimeout,
        kBusy,
        kNoQuota,
        kNoAuth,
        kUnknown,
        kNotImpl
    };
    ErrorCode();
    void SetFailed(ErrorCodeType err, const std::string& reason = "");
    std::string GetReason() const;
    ErrorCodeType GetType() const;
    void Reset();

private:
    ErrorCodeType m_err;
    std::string m_reason;
};

const char* ErrorCodeToString(ErrorCode error_code);

} // namespace rsfs

#endif // RSFS_SDK_ERROR_CODE_H
