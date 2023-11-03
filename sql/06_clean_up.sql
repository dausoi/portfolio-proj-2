DROP TABLE IF EXISTS landing.[agg_table];
ALTER TABLE IF EXISTS stg.[agg_table]
    SET SCHEMA landing;
DROP TABLE IF EXISTS stg.[src_table];
