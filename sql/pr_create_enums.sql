create or replace procedure stg.pr_create_enums()
language plpgsql
as $$
declare
begin
	DROP FUNCTION IF EXISTS stg.f_extract_domain(character varying);
	DROP TYPE IF EXISTS stg.visitmode;
	DROP TYPE IF EXISTS stg.wmproj;
	DROP TYPE IF EXISTS stg.proj;

	create type stg.visitmode as enum('desktop', 'mobile');
	create type stg.wmproj as enum('advisory', 'commons', 'meta',
								   'incubator', 'species', 'strategy',
								   'outreach', 'usability', 'quality',
								   'wikimania', 'wikitech');
	create type stg.proj as enum('wikibooks', 'wiktionary', 'wikimediafoundation',
								 'wikimedia', 'wikinews', 'wikiquote',
								 'wikisource', 'wikiversity', 'wikivoyage',
								 'mediawiki', 'wikidata', 'wikipedia');
end
$$
