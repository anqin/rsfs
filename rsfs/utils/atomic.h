// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:

#ifndef RSFS_UTILS_ATOMIC_H
#define RSFS_UTILS_ATOMIC_H

#if !defined(__i386__) && !defined(__x86_64__)
#error    "Arch not supprot!"
#endif

#include <stdint.h>

namespace rsfs {

template <typename T>
inline void atomic_inc(volatile T* n)
{
    asm volatile ("lock; incl %0;":"+m"(*n)::"cc");
}
template <typename T>
inline void atomic_dec(volatile T* n)
{
    asm volatile ("lock; decl %0;":"+m"(*n)::"cc");
}
template <typename T>
inline T atomic_add_ret_old(volatile T* n, T v)
{
    asm volatile ("lock; xaddl %1, %0;":"+m"(*n),"+r"(v)::"cc");
    return v;
}
template <typename T>
inline T atomic_inc_ret_old(volatile T* n)
{
    T r = 1;
    asm volatile ("lock; xaddl %1, %0;":"+m"(*n), "+r"(r)::"cc");
    return r;
}
template <typename T>
inline T atomic_dec_ret_old(volatile T* n)
{
    T r = (T)-1;
    asm volatile ("lock; xaddl %1, %0;":"+m"(*n), "+r"(r)::"cc");
    return r;
}
template <typename T>
inline T atomic_add_ret_old64(volatile T* n, T v)
{
    asm volatile ("lock; xaddq %1, %0;":"+m"(*n),"+r"(v)::"cc");
    return v;
}
template <typename T>
inline T atomic_inc_ret_old64(volatile T* n)
{
    T r = 1;
    asm volatile ("lock; xaddq %1, %0;":"+m"(*n), "+r"(r)::"cc");
    return r;
}
template <typename T>
inline T atomic_dec_ret_old64(volatile T* n)
{
    T r = (T)-1;
    asm volatile ("lock; xaddq %1, %0;":"+m"(*n), "+r"(r)::"cc");
    return r;
}
template <typename T>
inline void atomic_add(volatile T* n, T v)
{
    asm volatile ("lock; addl %1, %0;":"+m"(*n):"r"(v):"cc");
}
template <typename T>
inline void atomic_sub(volatile T* n, T v)
{
    asm volatile ("lock; subl %1, %0;":"+m"(*n):"r"(v):"cc");
}
template <typename T, typename C, typename D>
inline T atomic_cmpxchg(volatile T* n, C cmp, D dest)
{
    asm volatile ("lock; cmpxchgl %1, %0":"+m"(*n), "+r"(dest), "+a"(cmp)::"cc");
    return cmp;
}
// return old value
template <typename T>
inline T atomic_swap(volatile T* lockword, T value)
{
    asm volatile ("lock; xchg %0, %1;" : "+r"(value), "+m"(*lockword));
    return value;
}
template <typename T, typename E, typename C>
inline T atomic_comp_swap(volatile T* lockword, E exchange, C comperand)
{
    return atomic_cmpxchg(lockword, comperand, exchange);
}

} // namespace rsfs

#endif // RSFS_UTILS_ATOMIC_H
