DROP VIEW IF EXISTS stg.latest_day_agg;
CREATE VIEW stg.latest_day_agg AS
    SELECT agg.domain_code::varchar(255),
            agg.view_timestamp::timestamp without time zone,
            dc.lang::varchar(255),
            dc.proj::varchar(255),
            dc.wikimedia_proj::varchar(255),
            dc.visitmode::varchar(255),
            agg.count_views::integer
    FROM stg.domain_code dc
    JOIN stg.[agg_table] agg ON dc.domain_code = agg.domain_code;

CREATE TABLE IF NOT EXISTS prod.[dest_table] AS
    SELECT * FROM stg.latest_day_agg WHERE false;
INSERT INTO prod.[dest_table]
SELECT * FROM stg.latest_day_agg;
