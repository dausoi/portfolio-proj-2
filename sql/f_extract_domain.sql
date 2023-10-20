create or replace function stg.f_extract_domain(p_domain_code varchar(255))
-- This function takes a wikimedia pageview dumps' domain_code field and returns meaningful description.
-- This function is based on PostgreSQL 14

returns table
	(lang				varchar,
	 proj				stg.proj,
	wikimedia_proj			stg.wmproj,
	visitmode			stg.visitmode)
language plpgsql
as $$
declare
	dm_code				varchar;
	v_parsed_details		record;
	v_clean_details			record;
	WIKIMEDIA_PROJ			varchar := 'advisory|commons|meta|incubator|species|strategy|outreach|usability|quality|wikimania|wikitech';
	ORI_WIKIMEDIA_PATTERN		varchar := '^('|| WIKIMEDIA_PROJ ||')';
	LANG_PATTERN			varchar := '^[0-9a-z\-]+';
	NON_WIKIMEDIA_PATTERN		varchar := '\.(b|d|f|n|q|s|v|voy|w|wd)$';
	WIKIMEDIA_PATTERN		varchar := '^en\.('|| WIKIMEDIA_PROJ ||')\.(m)';
	VISITMODE_PATTERN		varchar := '\.(m)\.(?:b|d|f|m|n|q|s|v|voy|w|wd)$';
	VISITMODE_WP_PATTERN		varchar := '(?<=['|| WIKIMEDIA_PROJ ||']\.m)(\.m)$';
	
begin
	select into dm_code
			-- Wikimedia projects don't have language part of their domains in the original version.
			regexp_replace(p_domain_code, ORI_WIKIMEDIA_PATTERN, 'en.\1');
	select into v_parsed_details
			(regexp_matches(dm_code, LANG_PATTERN))[1]::varchar as lang,
			(regexp_matches(dm_code, NON_WIKIMEDIA_PATTERN))[1]::varchar as proj_1,
			(regexp_matches(dm_code, WIKIMEDIA_PATTERN))[2]::varchar as proj_2,
			(regexp_matches(dm_code, WIKIMEDIA_PATTERN))[1]::varchar as subproj_common,
			(regexp_matches(dm_code, VISITMODE_PATTERN))[1]::varchar as visit_1,
			(regexp_matches(dm_code, VISITMODE_WP_PATTERN))[1]::varchar as visit_2;
	select into v_clean_details
			v_parsed_details.lang as lang,
			case coalesce(v_parsed_details.proj_1, v_parsed_details.proj_2, 'wkp')
				when 'b' then 'wikibooks'
				when 'd' then 'wiktionary'
				when 'f' then 'wikimediafoundation'
				when 'm' then 'wikimedia'
				when 'n' then 'wikinews'
				when 'q' then 'wikiquote'
				when 's' then 'wikisource'
				when 'v' then 'wikiversity'
				when 'voy' then 'wikivoyage'
				when 'w' then 'mediawiki'
				when 'wd' then 'wikidata'
				when 'wkp' then 'wikipedia'
			end as proj,
			v_parsed_details.subproj_common as wikimedia_proj,
			case coalesce(v_parsed_details.visit_1, v_parsed_details.visit_2)
				when 'm' then 'mobile'
				else 'desktop'
			end as visitmode;
	return query
		select v_clean_details.lang::varchar as lang,
				v_clean_details.proj::stg.proj as proj,
				v_clean_details.wikimedia_proj::stg.wmproj as wikimedia_proj,
				v_clean_details.visitmode::stg.visitmode as visitmode;
end;
$$
