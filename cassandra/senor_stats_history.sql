CREATE TABLE streaming_data.sensor_stats_history (
    sensor_id int,
    start_time timestamp,
    end_time timestamp,
    record_count int,
    min_temperature float,
    max_temperature float,
    mean_temperature float,
    min_noise float,
    max_noise float,
    mean_noise float,
    min_vibration float,
    max_vibration float,
    mean_vibration float,
    PRIMARY KEY (sensor_id, start_time, end_time)
) WITH CLUSTERING ORDER BY (start_time DESC)
    AND additional_write_policy = '99p'
    AND bloom_filter_fp_chance = 0.01
    AND caching = {'keys': 'ALL', 'rows_per_partition': 'NONE'}
    AND cdc = false
    AND comment = ''
    AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy', 'max_threshold': '32', 'min_threshold': '4'}
    AND compression = {'chunk_length_in_kb': '16', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND crc_check_chance = 1.0
    AND default_time_to_live = 0
    AND extensions = {}
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair = 'BLOCKING'
    AND speculative_retry = '99p';