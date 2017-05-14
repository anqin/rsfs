// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#ifndef RSFS_UTILS_INT_MAP_H
#define RSFS_UTILS_INT_MAP_H

#include <vector>

#include "common/base/stdint.h"
#include "common/lock/rwlock.h"

namespace rsfs {
namespace utils {

class IntMap {
public:
    IntMap(uint32_t num, int32_t org_value);
    ~IntMap();

    void Set(uint32_t index, int32_t value);
    void Unset(uint32_t index);
    bool IsSet(uint32_t index);
    bool IsSet(uint32_t index, int32_t cmp_value);
    uint32_t GetSetNum() const;
    uint32_t GetUnsetNum() const;
    uint32_t Sum(int32_t value) const;

private:
    mutable RWLock m_rwlock;
    std::vector<int32_t> m_map_values;
    uint32_t m_set_count;
    int32_t m_default_value;
};

} // namespace utils
} // namespace rsfs


#endif // RSFS_UTILS_INT_MAP_H
