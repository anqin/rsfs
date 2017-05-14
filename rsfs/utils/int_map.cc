// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "rsfs/utils/int_map.h"

#include "thirdparty/glog/logging.h"

namespace rsfs {
namespace utils {

IntMap::IntMap(uint32_t num, int32_t org_value)
    : m_set_count(0), m_default_value(org_value) {
    m_map_values.resize(num, m_default_value);
    CHECK(m_map_values.size() == num);
}

IntMap::~IntMap() {}

void IntMap::Set(uint32_t index, int32_t value) {
    RWLock::WriterLocker locker(&m_rwlock);
    if (index > m_map_values.size() -1) {
        return;
    }
    if (m_map_values[index] == m_default_value
        && m_set_count < m_map_values.size()) {
        m_set_count++;
    }
    m_map_values[index] = value;
}

void IntMap::Unset(uint32_t index) {
    RWLock::WriterLocker locker(&m_rwlock);
    if (index > m_map_values.size() -1) {
        return;
    }
    if (m_map_values[index] != m_default_value
        && m_set_count > 0) {
        m_set_count--;
    }
    m_map_values[index] = m_default_value;
}

bool IntMap::IsSet(uint32_t index) {
    RWLock::ReaderLocker locker(m_rwlock);
    if (index > m_map_values.size() -1) {
        return false;
    }
    return m_map_values[index] != m_default_value;
}

bool IntMap::IsSet(uint32_t index, int32_t cmp_value) {
    RWLock::ReaderLocker locker(m_rwlock);
    if (index > m_map_values.size() -1) {
        return false;
    }
    return m_map_values[index] == cmp_value;
}

uint32_t IntMap::GetSetNum() const {
    RWLock::ReaderLocker locker(m_rwlock);
    return m_set_count;
}

uint32_t IntMap::GetUnsetNum() const {
    RWLock::ReaderLocker locker(m_rwlock);
    return m_map_values.size() - m_set_count;
}

uint32_t IntMap::Sum(int32_t value) const {
    uint32_t count = 0;
    RWLock::ReaderLocker locker(m_rwlock);
    for (uint32_t i = 0; i < m_map_values.size(); ++i) {
        if (m_map_values[i] == value) {
            count++;
        }
    }
    return count;
}

} // namespace utils
} // namespace rsfs
