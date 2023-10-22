DROP TABLE IF EXISTS stg.[agg_table] CASCADE;
CREATE TABLE stg.[agg_table] AS
    SELECT 	agg.view_timestamp,
            CASE WHEN agg.domain_code IS NULL THEN 'N/A'
                ELSE agg.domain_code END AS domain_code,
            agg.count_views
    FROM ( SELECT pgview_timestamp::timestamp without time zone as view_timestamp,
                domain_code,
                SUM(count_views) AS count_views
            FROM stg.[src_table]
            GROUP BY domain_code, view_timestamp
            ) agg;

DROP TABLE stg.[raw_table];
