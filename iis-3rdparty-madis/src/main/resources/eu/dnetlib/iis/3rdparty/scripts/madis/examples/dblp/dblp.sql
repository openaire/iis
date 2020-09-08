-- First of all download http://dblp.uni-trier.de/xml/dblp.xml.gz

-- To find the XML keys inside above XML and under the "article" tag, execute.

select jgroupunion(jdictkeys(c1)) from (xmlparse root:article fast:1 file 'dblp.xml.gz');

-- ["article/@/mdate","article/@/key","article/author","article/title/*","article/journal","article/volume","article/month","article/year","article/cdrom","article/ee","article/url","article/@/publtype","article/editor","article/publisher","article/pages","article/number","article/cite","article/cite/@/label","article/crossref","article/booktitle","article/note","article/@/rating","article/@/reviewid"]

-- We do the same for "www" and "inproceedings" tags.

select jgroupunion(jdictkeys(c1)) from (xmlparse root:www fast:1 file 'dblp.gz');

--["www/@/mdate","www/@/key","www/author","www/title","www/year","www/url","www/editor","www/booktitle","www/note","www/crossref","www/cite","www/ee","www/note/@/type","www/author/@/bibtex"]

select jgroupunion(jdictkeys(c1)) from (xmlparse root:inproceedings fast:1 file 'dblp.xml.gz');

--["inproceedings/@/mdate","inproceedings/@/key","inproceedings/author","inproceedings/title/*","inproceedings/pages","inproceedings/year","inproceedings/booktitle","inproceedings/month","inproceedings/url","inproceedings/note","inproceedings/cdrom","inproceedings/ee","inproceedings/crossref","inproceedings/cite","inproceedings/editor","inproceedings/cite/@/label","inproceedings/@/publtype","inproceedings/number"]

-- We create the article, www and inproceedings tables.

create table article as
select * from (
xmlparse '["article/@/mdate","article/@/key","article/author","article/title/*","article/journal","article/volume","article/month","article/year","article/cdrom","article/ee","article/url","article/@/publtype","article/editor","article/publisher","article/pages","article/number","article/cite","article/cite/@/label","article/crossref","article/booktitle","article/note","article/@/rating","article/@/reviewid"]' fast:1 file 'dblp.xml.gz' );

create table www as select * from (xmlparse '["www/@/mdate","www/@/key","www/author","www/title","www/year","www/url","www/editor","www/booktitle","www/note","www/crossref","www/cite","www/ee","www/note/@/type","www/author/@/bibtex"]' fast:1 file 'dblp.xml.gz');

create table inproceedings as select * from (xmlparse '["inproceedings/@/mdate","inproceedings/@/key","inproceedings/author","inproceedings/title/*","inproceedings/pages","inproceedings/year","inproceedings/booktitle","inproceedings/month","inproceedings/url","inproceedings/note","inproceedings/cdrom","inproceedings/ee","inproceedings/crossref","inproceedings/cite","inproceedings/editor","inproceedings/cite/@/label","inproceedings/@/publtype","inproceedings/number"]' fast:1 file 'dblp.xml.gz');

-- Lets say that we want to find the author tenure dates from the data. We'll do it by using a heuristic method, where the "oldest" author among a paper's author list is considered to be the "professor". Please NOTE that the used heuristic is very unreliable.

-- We start by creating a table that will contain author and his publication years.

create table authorpubyear as select jsplitv(t2j(author)) as author, year from article;

-- From author's publications we find the oldest one.

create table authorminyear as select author,min(year) as year from authorpubyear group by author;

create index idxauthorminyear on authorminyear(author);

-- For articles that have multiple authors (XMLPARSE uses TAB for multiple values on the same key, this is why we use "like '%TAB%'" below), find their minyear and select the author having the minimum minyear among them.

create table professors as 
select minrow(myear,author) as pauthor,pyear from (
select 
key,
author,
(select authorminyear.year from authorminyear where authorminyear.author=a.author) as myear,
pyear 
from 
(select key,jsplitv(t2j(author)) as author,year as pyear from article where author like '%	%') as a
) group by key;

-- We assume that the first time that an author appeared as a professor in a paper, is his tenure year.

create table tenures as select pauthor,min(pyear) as tyear from professors group by pauthor;

-- Now, lets calculate some other statistics.

-- Number of per author publications before tenure
select pauthor,count(year) from authorpubyear, tenures where authorpubyear.author=tenures.pauthor and authorpubyear.year<tenures.tyear group by pauthor;

-- Distribution of number of publications before tenure
select btyear,count(btyear) from (select pauthor,count(year) as btyear from authorpubyear, tenures where authorpubyear.author=tenures.pauthor and authorpubyear.year<tenures.tyear group by pauthor) group by btyear;

-- Top 100 number of coauthors per author
select author1, coauths from (select author1, count(distinct author2) as coauths from (select jpermutations(t2j(author),2) as author from article where author like '%	%') group by author1) order by coauths desc limit 100;


-- And run 5 steps of the PageRank algorithm on the graph created from author paper collaborations.

--Authorank
create table authorlinks as select distinct * from (select jpermutations(t2j(author),2) as author from article where author like '%	%');
create index idxauthor1 on authorlinks(author1);

create table authorank as select author, 1.0 as rank,(select count(author1) from authorlinks where author1=author) as linkcount from (select distinct(author1) as author from authorlinks);
create index idxauthorank on authorank(author);

update authorank set rank=1.0;
update authorank set rank=0.15+0.85*(select sum(rank/linkcount) from authorlinks al,authorank ar where al.author2=ar.author and al.author1=authorank.author);
update authorank set rank=0.15+0.85*(select sum(rank/linkcount) from authorlinks al,authorank ar where al.author2=ar.author and al.author1=authorank.author);
update authorank set rank=0.15+0.85*(select sum(rank/linkcount) from authorlinks al,authorank ar where al.author2=ar.author and al.author1=authorank.author);
update authorank set rank=0.15+0.85*(select sum(rank/linkcount) from authorlinks al,authorank ar where al.author2=ar.author and al.author1=authorank.author);
update authorank set rank=0.15+0.85*(select sum(rank/linkcount) from authorlinks al,authorank ar where al.author2=ar.author and al.author1=authorank.author);

