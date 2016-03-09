-- db optimization
PRAGMA temp_store_directory = '.';
pragma page_size=16384;
pragma default_cache_size=3000;
pragma journal_size_limit = 10000000;
pragma legacy_file_format = false;
pragma synchronous = 1 ;
pragma auto_vacuum=2; 

-- import and preparation
create temp table datac
as select id as dsetID, idForGivenType as doi, j2s(creatorNames) as creator, titles as title, publisher, publicationYear as year, resourceTypeValue as typegeneral from (setschema 'id, idForGivenType, creatorNames, titles, publisher, publicationYear, resourceTypeValue, referenceType' select jdictsplit(c1, 'id', 'idForGivenType', 'creatorNames', 'titles', 'publisher', 'publicationYear', 'resourceTypeValue', 'referenceType') from stdinput()) where referenceType = 'doi';

create temp table datacite 
as select dsetID, max(doi) as doi, creator, normalizetext(lower(title)) as title, publisher, year, typegeneral from datac where title like "% % %" group by title;


-- single DOIs creation
drop table if exists dois;
create table dois 
as select dsetID,doi,normalizetext(doi) as normaldoi,typegeneral from datac;

create index dois_idx
on dois(normaldoi, dsetID);


-- triple creation
create temp table triples 
as select doi,middle as words, comprspaces(j2s(prev,middle,next)) as title from (setschema 'doi, prev, middle, next' select doi,textwindow2s(title,15,3,15) from datacite) group by doi, words, title;


-- singularly distinguished titles
create temp table uniquesplittitles 
as select words,titles,dois from (select words,jgroup(title) as titles, jgroup(normalizetext(doi)) as dois ,count(distinct(doi)) as num from triples group by words) where num = 1;

create temp table uniquetitleswithtriples 
as select words,titles,dois from (select max(length(words)) as len,words,titles,dois from uniquesplittitles group by dois);

create index uniquetitleswithtriples_index 
on uniquetitleswithtriples(words,titles,dois);

delete from triples where normalizetext(doi) in (select dois as doi from uniquesplittitles group by doi);


-- plurally distinguished titles
create temp table splittitles 
as select words,titles,dois from (select words,jgroup(title) as titles, jgroup(normalizetext(doi)) as dois ,count(distinct(doi)) as num from triples group by words) 
where num <= (select * from (select jgroupuniquelimit(dois,n,(select count(distinct doi) from triples)/2) from (select jgroup(doi) as dois,count(doi) as n from triples group by words order by n asc)));

delete from triples where normalizetext(doi) in (select doi from (select jsplitv(dois) as doi from splittitles) group by doi);

insert into splittitles 
select words,titles,dois from (select words,jgroup(title) as titles, jgroup(normalizetext(doi)) as dois ,count(distinct(doi)) as num from triples group by words) 
where num <= (select * from (select jgroupuniquelimit(dois,n,(select count(distinct doi) from triples)/2) from (select jgroup(doi) as dois,count(doi) as n from triples group by words order by n asc)));

delete from triples where normalizetext(doi) in (select doi from (select jsplitv(dois) as doi from splittitles) group by doi);

insert into splittitles
select words,titles,dois from (select words,jgroup(title) as titles, jgroup(normalizetext(doi)) as dois ,count(distinct(doi)) as num from triples group by words);

create temp table titleswithtriplesmaxwords 
as select words,titles,dois from (select max(length(words)) as len,words,titles,dois from splittitles group by dois);


-- DOI bag of words creation and normalization
create temp table bagofwordsfordois 
as select dsetID,doi,normalizetext(doi) as doi1,title, lower(j2s(publisher,year,creator)) as bag,lower(publisher) as publisher,lower(creator) as creator,typegeneral as generaltype from datacite;

update bagofwordsfordois 
set bag = comprspaces(filterstopwords(regexpr('\b\w{1,3}\b',regexpr('\W|_',bag,' '),''))), publisher = comprspaces(filterstopwords(regexpr('\b\w{1,3}\b',regexpr('\W|_',publisher,' '),''))), creator = comprspaces(filterstopwords(regexpr('\b\w{1,3}\b',regexpr('\W|_',creator,' '),'')));

update bagofwordsfordois 
set bag = jmergeregexp(jset(s2j(bag))) , publisher = jmergeregexp(jset(s2j(publisher))), creator = jmergeregexp(jset(s2j(creator)));

create index bagofwords_index on bagofwordsfordois(doi1,title,bag,publisher,creator,generaltype);


-- Connect singularly distinguished titles with their bag of words
create temp table uniquetitleswithtriples_bagofwords 
as select dsetID,dois,doi,titles,words , bag,publisher,creator,generaltype from uniquetitleswithtriples, bagofwordsfordois where dois = doi1;

create index uniquetitleswithtriples_bagofwords_index 
on uniquetitleswithtriples_bagofwords(words,titles,bag,publisher,creator,generaltype);


-- Extract singularly distinguished titles from multiple distinguished titles, connect with bag of words and insert into unique titles

insert into uniquetitleswithtriples_bagofwords
select dsetID, dois, doi, titles, words , bag, publisher, creator, generaltype 
from (select * from titleswithtriplesmaxwords where jlen(titles)=1), bagofwordsfordois 
where dois = doi1 ;

delete from titleswithtriplesmaxwords where jlen(titles) = 1;


-- Split vertically multiple distinguished titles
create temp table titles 
as select words,titles from titleswithtriplesmaxwords;

create temp table doiswords 
as select words,dois from titleswithtriplesmaxwords;

create temp table jsplittitles
as select words,jsplitv(titles) as titles from titles;

create temp table jsplitdois 
as select words,jsplitv(dois) as dois from doiswords;

create temp table jsplittitlesdois 
as select jsplitdois.words as words,jsplitdois.dois as dois,jsplittitles.titles from jsplitdois,jsplittitles where jsplittitles.rowid = jsplitdois.rowid;

create temp table distincttitlesdoiswords 
as select words,titles,dois from (select max(length(words)),words,titles,dois from jsplittitlesdois group by titles);

create temp table distincttitlesdoiswords_bag 
as select dsetID,distincttitlesdoiswords.dois,doi,titles,words,bag,publisher,creator,generaltype from distincttitlesdoiswords,bagofwordsfordois where distincttitlesdoiswords.dois = bagofwordsfordois.doi1;

create index distincttitlesdoiswords_index 
on distincttitlesdoiswords_bag(words,titles,bag,publisher,creator,generaltype);


-- Merge everything into one table
insert into distincttitlesdoiswords_bag 
select * from uniquetitleswithtriples_bagofwords;

create temp table titlesandtriples1 
as select dsetID,dois,doi,titles,words,bag,publisher,creator,generaltype from (select dsetID, max(length(words)),dois,doi,words,bag,publisher,creator,titles,generaltype from distincttitlesdoiswords_bag group by titles);

drop table if exists titlesandtriples;
create table titlesandtriples 
as select dsetID,dois,doi, lower(titles) as titles, lower(words) as words,bag,publisher,creator,generaltype from (select dsetID, max(length(words)),dois,doi,words,bag,publisher,creator,titles,generaltype from titlesandtriples1  group by dois);

create index titlesandtriples_index 
on titlesandtriples(words,titles,bag,publisher,creator,generaltype);
