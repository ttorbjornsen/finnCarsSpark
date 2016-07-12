DROP KEYSPACE IF EXISTS finncars;

CREATE KEYSPACE finncars WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '1'}  AND durable_writes = true;

DROP TABLE IF EXISTS finncars.acq_car_header;

CREATE TABLE finncars.acq_car_header (
    title text,
    url text,
    location text,
    year text,
    km text,
    price text,
    load_time timestamp,
    load_date text,
    PRIMARY KEY (load_date, url, load_time)
) WITH CLUSTERING ORDER BY (url ASC, load_time DESC)
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

DROP TABLE IF EXISTS finncars.acq_car_details;

CREATE TABLE finncars.acq_car_details (
    url text,
    properties text,
    equipment text,
    information text,
    deleted boolean,
    load_time timestamp,
    load_date text,
    PRIMARY KEY (load_date,url, load_time)
) WITH CLUSTERING ORDER BY (url ASC, load_time DESC)
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

DROP TABLE IF EXISTS finncars.prop_car_daily;

CREATE TABLE finncars.prop_car_daily (
    url text,
    title text,
    location text,
    year text,
    km text,
    price text,
    properties map<text,text>,
    equipment set<text>,
    information text,
    deleted boolean,
    load_time timestamp,
    load_date text,
    PRIMARY KEY (load_date,url)
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