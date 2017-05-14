// Copyright (C) 2017, for RSFS Authors.
// Author: An Qin (anqin.qin@gmail.com)
//
// Description:
//

#include "common/base/stdint.h"
#include "thirdparty/gflags/gflags.h"

/////////  common /////////

DEFINE_string(rsfs_role, "", "the role of rsfs running binary, should be one of (master | snode)");

DEFINE_string(rsfs_user_identity, "", "the identity of rsfs user");
DEFINE_string(rsfs_user_passcode, "", "the passcode of rsfs user");

DEFINE_int64(rsfs_heartbeat_period, 5000, "the heartbeat period (ms) when send heartbeat");
DEFINE_int64(rsfs_heartbeat_retry_period_factor, 1, "the heartbeat period factor when retry send heartbeat");
DEFINE_int32(rsfs_heartbeat_retry_times, 5, "the max retry times when fail to send report request");

DEFINE_string(rsfs_working_dir, "./", "the base dir for system data");

/////////  master /////////

DEFINE_string(rsfs_master_addr, "127.0.0.1", "the master address of rsfs system");
DEFINE_string(rsfs_master_port, "10000", "the master port of rsfs system");
DEFINE_int64(rsfs_heartbeat_timeout_period_factor, 120, "the timeout period factor when lose heartbeat");
DEFINE_int32(rsfs_master_connect_retry_times, 5, "the max retry times when connect to master");
DEFINE_int32(rsfs_master_connect_retry_period, 1000, "the retry period (in ms) between two master connection");
DEFINE_int32(rsfs_master_connect_timeout_period, 5000, "the timeout period (in ms) for each master connection");
DEFINE_int32(rsfs_master_query_snode_period, 10000, "the period (in ms) for query snode status" );
DEFINE_int32(rsfs_master_common_retry_period, 1000, "the period (in ms) for common operation" );
DEFINE_int32(rsfs_master_meta_retry_times, 5, "the max retry times when master read/write meta");
DEFINE_bool(rsfs_master_rpc_limit_enabled, false, "enable the rpc traffic limit in master");
DEFINE_int32(rsfs_master_rpc_limit_max_inflow, 10, "the max bandwidth (in MB/s) for master rpc traffic limitation on input flow");
DEFINE_int32(rsfs_master_rpc_limit_max_outflow, 10, "the max bandwidth (in MB/s) for master rpc traffic limitation on output flow");
DEFINE_int32(rsfs_master_rpc_max_pending_buffer_size, 2, "max pending buffer size (in MB) for master rpc");
DEFINE_int32(rsfs_master_rpc_work_thread_num, 8, "thread num of master rpc client");

DEFINE_int32(rsfs_master_thread_min_num, 1, "the min thread number for master impl operations");
DEFINE_int32(rsfs_master_thread_max_num, 10, "the max thread number for master impl operations");

DEFINE_bool(rsfs_delete_obsolete_tabledir_enabled, false, "delete table dir or not when deleting table");

DEFINE_string(rsfs_master_db_path, "./master_meta", "the path of master meta info");
DEFINE_string(rsfs_master_dirmeta_path, "dir_meta", "the path of dir meta store");
DEFINE_string(rsfs_master_filemeta_path, "file_meta", "the path of file meta");

///////// rsfs node  /////////

DEFINE_string(rsfs_snode_addr, "127.0.0.1", "the rsfs node address of rsfs system");
DEFINE_string(rsfs_snode_port, "20000", "the rsfs node port of rsfs system");
DEFINE_int32(rsfs_snode_write_thread_num, 10, "the write thread number of rsfs node server");
DEFINE_int32(rsfs_snode_read_thread_num, 40, "the read thread number of rsfs node server");
DEFINE_int32(rsfs_snode_scan_thread_num, 5, "the scan thread number of rsfs node server");
DEFINE_int32(rsfs_snode_manual_compact_thread_num, 2, "the manual compact thread number of rsfs node server");
DEFINE_int32(rsfs_snode_thread_min_num, 1, "the min thread number for rsfs node impl operations");
DEFINE_int32(rsfs_snode_thread_max_num, 10, "the max thread number for rsfs node impl operations");
DEFINE_int32(rsfs_snode_compact_thread_num, 10, "the max thread number for leveldb compaction");

DEFINE_int32(rsfs_snode_connect_retry_times, 5, "the max retry times when connect to rsfs node");
DEFINE_int32(rsfs_snode_connect_retry_period, 1000, "the retry period (in ms) between retry two rsfs node connection");
DEFINE_int32(rsfs_snode_connect_timeout_period, 180000, "the timeout period (in ms) for each rsfs node connection");
DEFINE_string(rsfs_snode_path_prefix, "./data/", "the path prefix for table storage");
DEFINE_int32(rsfs_snode_rpc_timeout_period, 300000, "the timeout period (in ms) for snode rpc");
DEFINE_bool(rsfs_snode_rpc_limit_enabled, false, "enable the rpc traffic limit in snode");
DEFINE_int32(rsfs_snode_rpc_limit_max_inflow, 10, "the max bandwidth (in MB/s) for snode rpc traffic limitation on input flow");
DEFINE_int32(rsfs_snode_rpc_limit_max_outflow, 10, "the max bandwidth (in MB/s) for snode rpc traffic limitation on output flow");
DEFINE_int32(rsfs_snode_rpc_max_pending_buffer_size, 2, "max pending buffer size (in MB) for snode rpc");
DEFINE_int32(rsfs_snode_rpc_work_thread_num, 8, "thread num of snode rpc client");

DEFINE_bool(rsfs_snode_cpu_affinity_enabled, false, "enable cpu affinity or not");
DEFINE_string(rsfs_snode_cpu_affinity_set, "1,2", "the cpu set of cpu affinity setting");

DEFINE_bool(rsfs_snode_tcm_cache_release_enabled, true, "enable the timer to release tcmalloc cache");
DEFINE_int32(rsfs_snode_tcm_cache_release_period, 180, "the period (in sec) to try release tcmalloc cache");

///////// SDK  /////////
DEFINE_string(rsfs_sdk_conf_file, "./rsfs.flag", "conf file for client/sdk");
DEFINE_int32(rsfs_sdk_rscode_mm, 10, "the data block number in RS");
DEFINE_int32(rsfs_sdk_rscode_kk, 4, "the parity block number in RS");
DEFINE_int32(rsfs_sdk_rscode_block_size, 8192, "the block size (Bytes) in RS");
DEFINE_int32(rsfs_sdk_rscode_tail_backup_num, 3, "the backup number for tail data set in RS");

DEFINE_int32(rsfs_sdk_write_retry_times, 3, "the max retry time of write operation");
DEFINE_int32(rsfs_sdk_read_retry_times, 3, "the max retry time of read operation");
DEFINE_int32(rsfs_sdk_thread_min_num, 1, "the min thread number for sdk");
DEFINE_int32(rsfs_sdk_thread_max_num, 20, "the max thread number for sdk");
DEFINE_bool(rsfs_sdk_rpc_limit_enabled, false, "enable the rpc traffic limit in sdk");
DEFINE_int32(rsfs_sdk_rpc_limit_max_inflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on input flow");
DEFINE_int32(rsfs_sdk_rpc_limit_max_outflow, 10, "the max bandwidth (in MB/s) for sdk rpc traffic limitation on output flow");
DEFINE_int32(rsfs_sdk_rpc_max_pending_buffer_size, 200, "max pending buffer size (in MB) for sdk rpc");
DEFINE_int32(rsfs_sdk_rpc_work_thread_num, 8, "thread num of sdk rpc client");
DEFINE_int32(rsfs_sdk_rpc_list_size_limit, 1024, "the size limit (KB) of each meta list operation");
