DO $$ 
BEGIN
	CREATE TYPE stg.proj AS ENUM
		('wikibooks', 'wiktionary', 'wikimediafoundation',
		'wikimedia', 'wikinews', 'wikiquote',
		'wikisource', 'wikiversity', 'wikivoyage',
		'mediawiki', 'wikidata', 'wikipedia');
	CREATE TYPE stg.visitmode AS ENUM
		('desktop', 'mobile');
	CREATE TYPE stg.wmproj AS ENUM
		('advisory','commons','meta',
		'incubator','species','strategy',
		'outreach','usability','quality',
		'wikimania','wikitech');
EXCEPTION
	WHEN duplicate_object THEN null;
END $$
;
