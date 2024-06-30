use std::collections::HashMap;
use std::sync::Mutex;
use tracing::{error, info};
use tracing_subscriber;

#[macro_use(concat_string)]
extern crate concat_string;

fn main() {
    tracing_subscriber::fmt::init();

    let db: Mutex<HashMap<Vec<u8>, Vec<u8>>> = Mutex::new(HashMap::new());

    let mut s = redcon::listen("127.0.0.1:6379", db).unwrap();
    s.opened = Some(|conn, _db| info!("Got new connection from {}", conn.addr()));
    s.closed = Some(|_conn, _db, err| {
        if let Some(err) = err {
            error!("{}", err)
        }
    });
    s.command = Some(|conn, db, args| {
        let name = String::from_utf8_lossy(&args[0]).to_lowercase();

        info!("Received command: \"{}\"", name);
        match name.as_str() {
            "ping" => conn.write_string("PONG"),
            "set" => {
                if args.len() < 3 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let mut db = db.lock().unwrap();
                db.insert(args[1].to_owned(), args[2].to_owned());
                conn.write_string("OK");
            }
            "get" => {
                if args.len() < 2 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let db = db.lock().unwrap();
                match db.get(&args[1]) {
                    Some(val) => conn.write_bulk(val),
                    None => conn.write_null(),
                }
            }
            "del" => {
                if args.len() != 2 {
                    conn.write_error("ERR wrong number of arguments");
                    return;
                }
                let mut db = db.lock().unwrap();
                match db.remove(&args[1]) {
                    Some(_) => conn.write_integer(1),
                    None => conn.write_integer(0),
                }
            }
            "select" => conn.write_string("OK"),
            "info" => {
                conn.write_bulk(
                    concat_string!(
                        "# Server\r\n",
                        "redis_version:7.2.5\r\n",
                        "redis_git_sha1:00000000\r\n",
                        "redis_git_dirty:0\r\n",
                        "redis_build_id:affe2dab174e19c6\r\n",
                        "redis_mode:standalone\r\n",
                        "os:Linux 5.15.0-1015-aws x86_64\r\n",
                        "arch_bits:64\r\n",
                        "monotonic_clock:POSIX clock_gettime\r\n",
                        "multiplexing_api:epoll\r\n",
                        "atomicvar_api:c11-builtin\r\n",
                        "gcc_version:10.2.1\r\n",
                        "process_id:1\r\n",
                        "process_supervised:no\r\n",
                        "run_id:aefa5a2dac16d8c0afbf87b3c69eb466bb51828f\r\n",
                        "tcp_port:6379\r\n",
                        "server_time_usec:1719762080674117\r\n",
                        "uptime_in_seconds:10700573\r\n",
                        "uptime_in_days:123\r\n",
                        "hz:10\r\n",
                        "configured_hz:10\r\n",
                        "lru_clock:8486048\r\n",
                        "executable:/data/redis-server\r\n",
                        "config_file:/etc/redis/redis.conf\r\n",
                        "io_threads_active:0\r\n",
                        "listener0:name=tcp,bind=*,bind=-::*,port=6379\r\n",
                        "\r\n",
                        "# Clients\r\n",
                        "connected_clients:1\r\n",
                        "cluster_connections:0\r\n",
                        "maxclients:10000\r\n",
                        "client_recent_max_input_buffer:0\r\n",
                        "client_recent_max_output_buffer:0\r\n",
                        "blocked_clients:0\r\n",
                        "tracking_clients:0\r\n",
                        "clients_in_timeout_table:0\r\n",
                        "total_blocking_keys:0\r\n",
                        "total_blocking_keys_on_nokey:0\r\n",
                        "\r\n",
                        "# Memory\r\n",
                        "used_memory:906568\r\n",
                        "used_memory_human:885.32K\r\n",
                        "used_memory_rss:14397440\r\n",
                        "used_memory_rss_human:13.73M\r\n",
                        "used_memory_peak:906568\r\n",
                        "used_memory_peak_human:885.32K\r\n",
                        "used_memory_peak_perc:102.59%\r\n",
                        "used_memory_overhead:865992\r\n",
                        "used_memory_startup:865808\r\n",
                        "used_memory_dataset:40576\r\n",
                        "used_memory_dataset_perc:99.55%\r\n",
                        "allocator_allocated:1171616\r\n",
                        "allocator_active:1290240\r\n",
                        "allocator_resident:4874240\r\n",
                        "total_system_memory:8237547520\r\n",
                        "total_system_memory_human:7.67G\r\n",
                        "used_memory_lua:31744\r\n",
                        "used_memory_vm_eval:31744\r\n",
                        "used_memory_lua_human:31.00K\r\n",
                        "used_memory_scripts_eval:0\r\n",
                        "number_of_cached_scripts:0\r\n",
                        "number_of_functions:0\r\n",
                        "number_of_libraries:0\r\n",
                        "used_memory_vm_functions:32768\r\n",
                        "used_memory_vm_total:64512\r\n",
                        "used_memory_vm_total_human:63.00K\r\n",
                        "used_memory_functions:184\r\n",
                        "used_memory_scripts:184\r\n",
                        "used_memory_scripts_human:184B\r\n",
                        "maxmemory:4294967296\r\n",
                        "maxmemory_human:4.00G\r\n",
                        "maxmemory_policy:allkeys-lru\r\n",
                        "allocator_frag_ratio:1.00\r\n",
                        "allocator_frag_bytes:270920\r\n",
                        "allocator_rss_ratio:1.02\r\n",
                        "allocator_rss_bytes:11624448\r\n",
                        "rss_overhead_ratio:1.01\r\n",
                        "rss_overhead_bytes:4710400\r\n",
                        "mem_fragmentation_ratio:1.03\r\n",
                        "mem_fragmentation_bytes:16734280\r\n",
                        "mem_not_counted_for_evict:0\r\n",
                        "mem_replication_backlog:0\r\n",
                        "mem_total_replication_buffers:0\r\n",
                        "mem_clients_slaves:0\r\n",
                        "mem_clients_normal:0\r\n",
                        "mem_cluster_links:0\r\n",
                        "mem_aof_buffer:0\r\n",
                        "mem_allocator:jemalloc-5.3.0\r\n",
                        "active_defrag_running:0\r\n",
                        "lazyfree_pending_objects:0\r\n",
                        "lazyfreed_objects:0\r\n",
                        "\r\n",
                        "# Persistence\r\n",
                        "loading:0\r\n",
                        "async_loading:0\r\n",
                        "current_cow_peak:0\r\n",
                        "current_cow_size:0\r\n",
                        "current_cow_size_age:0\r\n",
                        "current_fork_perc:0.00\r\n",
                        "current_save_keys_processed:0\r\n",
                        "current_save_keys_total:0\r\n",
                        "rdb_changes_since_last_save:0\r\n",
                        "rdb_bgsave_in_progress:0\r\n",
                        "rdb_last_save_time:1719761507\r\n",
                        "rdb_last_bgsave_status:ok\r\n",
                        "rdb_last_bgsave_time_sec:-1\r\n",
                        "rdb_current_bgsave_time_sec:-1\r\n",
                        "rdb_saves:0\r\n",
                        "rdb_last_cow_size:0\r\n",
                        "rdb_last_load_keys_expired:0\r\n",
                        "rdb_last_load_keys_loaded:0\r\n",
                        "aof_enabled:0\r\n",
                        "aof_rewrite_in_progress:0\r\n",
                        "aof_rewrite_scheduled:0\r\n",
                        "aof_last_rewrite_time_sec:-1\r\n",
                        "aof_current_rewrite_time_sec:-1\r\n",
                        "aof_last_bgrewrite_status:ok\r\n",
                        "aof_rewrites:0\r\n",
                        "aof_rewrites_consecutive_failures:0\r\n",
                        "aof_last_write_status:ok\r\n",
                        "aof_last_cow_size:0\r\n",
                        "module_fork_in_progress:0\r\n",
                        "module_fork_last_cow_size:0\r\n",
                        "\r\n",
                        "# Stats\r\n",
                        "total_connections_received:0\r\n",
                        "total_commands_processed:0\r\n",
                        "instantaneous_ops_per_sec:0\r\n",
                        "total_net_input_bytes:14\r\n",
                        "total_net_output_bytes:0\r\n",
                        "total_net_repl_input_bytes:0\r\n",
                        "total_net_repl_output_bytes:0\r\n",
                        "instantaneous_input_kbps:0.00\r\n",
                        "instantaneous_output_kbps:0.00\r\n",
                        "instantaneous_input_repl_kbps:0.00\r\n",
                        "instantaneous_output_repl_kbps:0.00\r\n",
                        "rejected_connections:0\r\n",
                        "sync_full:0\r\n",
                        "sync_partial_ok:0\r\n",
                        "sync_partial_err:0\r\n",
                        "expired_keys:0\r\n",
                        "expired_stale_perc:0.00\r\n",
                        "expired_time_cap_reached_count:0\r\n",
                        "expire_cycle_cpu_milliseconds:1062047\r\n",
                        "evicted_keys:0\r\n",
                        "evicted_clients:0\r\n",
                        "total_eviction_exceeded_time:0\r\n",
                        "current_eviction_exceeded_time:0\r\n",
                        "keyspace_hits:2869581\r\n",
                        "keyspace_misses:210222\r\n",
                        "pubsub_channels:0\r\n",
                        "pubsub_patterns:0\r\n",
                        "pubsubshard_channels:0\r\n",
                        "latest_fork_usec:0\r\n",
                        "total_forks:0\r\n",
                        "migrate_cached_sockets:0\r\n",
                        "slave_expires_tracked_keys:0\r\n",
                        "active_defrag_hits:0\r\n",
                        "active_defrag_misses:0\r\n",
                        "active_defrag_key_hits:0\r\n",
                        "active_defrag_key_misses:0\r\n",
                        "total_active_defrag_time:0\r\n",
                        "current_active_defrag_time:0\r\n",
                        "tracking_total_keys:0\r\n",
                        "tracking_total_items:0\r\n",
                        "tracking_total_prefixes:0\r\n",
                        "unexpected_error_replies:0\r\n",
                        "total_error_replies:19181\r\n",
                        "dump_payload_sanitizations:0\r\n",
                        "total_reads_processed:1\r\n",
                        "total_writes_processed:0\r\n",
                        "io_threaded_reads_processed:0\r\n",
                        "io_threaded_writes_processed:0\r\n",
                        "reply_buffer_shrinks:0\r\n",
                        "reply_buffer_expands:0\r\n",
                        "eventloop_cycles:77\r\n",
                        "eventloop_duration_sum:5262\r\n",
                        "eventloop_duration_cmd_sum:0\r\n",
                        "instantaneous_eventloop_cycles_per_sec:9\r\n",
                        "instantaneous_eventloop_duration_usec:66\r\n",
                        "acl_access_denied_auth:0\r\n",
                        "acl_access_denied_cmd:0\r\n",
                        "acl_access_denied_key:0\r\n",
                        "acl_access_denied_channel:0\r\n",
                        "\r\n",
                        "# Replication\r\n",
                        "role:master\r\n",
                        "connected_slaves:0\r\n",
                        "master_failover_state:no-failover\r\n",
                        "master_replid:92fef281b2fd4ad63906bd1724167c0a8051ac94\r\n",
                        "master_replid2:0000000000000000000000000000000000000000\r\n",
                        "master_repl_offset:0\r\n",
                        "second_repl_offset:-1\r\n",
                        "repl_backlog_active:0\r\n",
                        "repl_backlog_size:1048576\r\n",
                        "repl_backlog_first_byte_offset:0\r\n",
                        "repl_backlog_histlen:0\r\n",
                        "\r\n",
                        "# CPU\r\n",
                        "used_cpu_sys:0.175160\r\n",
                        "used_cpu_user:0.053277\r\n",
                        "used_cpu_sys_children:0.006738\r\n",
                        "used_cpu_user_children:0.001050\r\n",
                        "used_cpu_sys_main_thread:0.213047\r\n",
                        "used_cpu_user_main_thread:0.413047\r\n",
                        "\r\n",
                        "# Modules\r\n",
                        "\r\n",
                        "# Errorstats\r\n",
                        "\r\n",
                        "# Cluster\r\n",
                        "cluster_enabled:0\r\n",
                        "\r\n",
                        "# Keyspace\r\n"
                    )
                    .as_bytes(),
                );
            }
            _ => conn.write_error("ERR unknown command"),
        }
    });
    info!("Serving at {}", s.local_addr());
    s.serve().unwrap();
}
