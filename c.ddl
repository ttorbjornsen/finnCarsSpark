DROP TABLE finncars.acq_car_header;

CREATE TABLE finncars.acq_car_header (
    title text,
    url text,
    location text,
    year text,
    km text,
    price text,
    load_time timestamp,
    load_date text,
    PRIMARY KEY ((load_date),url) //include load_time if unique is needed
) WITH CLUSTERING ORDER BY (url ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 1209600
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';

DROP TABLE finncars.acq_car_details;

CREATE TABLE finncars.acq_car_details (
    url text,
    properties text,
    equipment text,
    information text,
    load_time timestamp,
    load_date text,
    PRIMARY KEY ((load_date),url)
) WITH CLUSTERING ORDER BY (url ASC)
    AND bloom_filter_fp_chance = 0.01
    AND caching = '{"keys":"ALL", "rows_per_partition":"NONE"}'
    AND comment = ''
    AND compaction = {'min_threshold': '4', 'class': 'org.apache.cassandra.db.compaction.DateTieredCompactionStrategy', 'max_sstable_age_days': '365', 'base_time_seconds': '3600', 'max_threshold': '32'}
    AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
    AND dclocal_read_repair_chance = 0.1
    AND default_time_to_live = 1209600
    AND gc_grace_seconds = 864000
    AND max_index_interval = 2048
    AND memtable_flush_period_in_ms = 0
    AND min_index_interval = 128
    AND read_repair_chance = 0.0
    AND speculative_retry = '99.0PERCENTILE';