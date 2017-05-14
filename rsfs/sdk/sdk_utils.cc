// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/sdk/sdk_utils.h"

namespace rsfs {
namespace sdk {


uint64_t BlockFileName(uint64_t fid, uint32_t block_no) {
//     return block_no;
    return (fid << 32) + block_no;
}

} // namespace sdk
} // namespace rsfs
