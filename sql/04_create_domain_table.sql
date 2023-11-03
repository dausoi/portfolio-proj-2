-- This query create a domain code in staging area
-- Used in PostgreSQL 14
DROP MATERIALIZED VIEW IF EXISTS stg.latest_domain_code;
CREATE MATERIALIZED VIEW stg.latest_domain_code AS
    SELECT DISTINCT domain_code FROM stg.[src_domain_table];

CREATE TABLE IF NOT EXISTS stg.domain_code
    (domain_code    text,
     lang           varchar,
     proj           stg.proj,
     wikimedia_proj stg.wmproj,
     visitmode      stg.visitmode);

INSERT INTO stg.domain_code (domain_code, lang, proj, wikimedia_proj, visitmode)
    SELECT domain_code,
            (stg.f_extract_domain(domain_code)).lang as lang,
            (stg.f_extract_domain(domain_code)).proj as proj,
            (stg.f_extract_domain(domain_code)).wikimedia_proj as wikimedia_proj,
            (stg.f_extract_domain(domain_code)).visitmode as visitmode
    FROM stg.latest_domain_code
    EXCEPT
    SELECT domain_code, lang, proj, wikimedia_proj, visitmode
    FROM stg.domain_code;
