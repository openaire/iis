--
-- This file is part of CoAnSys project.
-- Copyright (c) 2012-2015 ICM-UW
-- 
-- CoAnSys is free software: you can redistribute it and/or modify
-- it under the terms of the GNU Affero General Public License as published by
-- the Free Software Foundation, either version 3 of the License, or
-- (at your option) any later version.

-- CoAnSys is distributed in the hope that it will be useful,
-- but WITHOUT ANY WARRANTY; without even the implied warranty of
-- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
-- GNU Affero General Public License for more details.
-- 
-- You should have received a copy of the GNU Affero General Public License
-- along with CoAnSys. If not, see <http://www.gnu.org/licenses/>.
--
%default RAW_DATA '/raw'
%default DOC_TERM_ALL '/term/all'
%default DOC_TERM_KEYWORDS '/term/keywords'
%default DOC_TERM_TITLE '/term/title'
%default TFIDF_NON_WEIGHTED_SUBDIR '/tfidf/nonweighted'
%default TFIDF_TOPN_WEIGHTED_SUBDIR '/tfidf/weighted-topn'
%default TFIDF_TOPN_ALL_TEMP '/tfidf/all-topn-tmp'
%default TFIDF_TOPN_ALL_SUBDIR '/tfidf/all-topn'
%default TFIDF_TF_ALL_SUBDIR '/tfidf/tf-all-topn'
%default SIMILARITY_ALL_DOCS_SUBDIR '/similarity/alldocs'
%default SIMILARITY_TOPN_DOCS_SUBDIR '/similarity/topn'
%default TERM_COUNT '/term-count'
%default WORD_RANK '/ranked-word-count';
%default WORD_RANK_HR '/ranked-word-count-human-readable';
%default WORD_COUNT '/filtered-by-ranked-word-count';
%default WORD_COUNT_NEG '/filtered-by-ranked-word-count_rejected';
%default WORD_COUNT_ACC '/filtered-by-ranked-word-count_accepted';


%default tfidfTopnTermPerDocument 20
%default similarityTopnDocumentPerDocument 20
%default removal_rate 1.1
%default removal_least_used -1
%default threshold_num_of_vector_elems_length 0
%default sample 1.0
%default parallel 40
%default tmpCompressionCodec gz
%default mapredChildJavaOpts -Xmx8000m

%default inputPath 'hdfs://hadoop-master.vls.icm.edu.pl:8020/srv/bwndata/seqfile/springer-metadata/springer-20120419-springer0*.sq'
%default outputPath 'hdfs://hadoop-master.vls.icm.edu.pl:8020/srv/bwndata/seqfile/springer-metadata/springer-20120419-springer0*.sq'

%default jars '*.jar'
%default commonJarsPath 'lib/$jars'
REGISTER '$commonJarsPath'

DEFINE WeightedTFIDF pl.edu.icm.coansys.similarity.pig.udf.TFIDF('weighted');
DEFINE StemmedPairs pl.edu.icm.coansys.similarity.pig.udf.ExtendedStemmedPairs();
DEFINE KeywordSimilarity pl.edu.icm.coansys.similarity.pig.udf.AvgSimilarity('dks');
DEFINE DocsCombinedSimilarity pl.edu.icm.coansys.similarity.pig.udf.AvgSimilarity('dkcs');
DEFINE DocToTupleMap pl.edu.icm.coansys.similarity.pig.udf.DocumentProtobufToTupleMap();

SET default_parallel $parallel
SET mapred.child.java.opts $mapredChildJavaOpts
SET mapreduce.map.java.opts $mapredChildJavaOpts
SET mapreduce.reduce.java.opts $mapredChildJavaOpts
SET pig.tmpfilecompression true
SET pig.tmpfilecompression.codec $tmpCompressionCodec
%DEFAULT scheduler default
SET mapred.fairscheduler.pool $scheduler
IMPORT 'macros.pig';

-------------------------------------------------------
-- business code section
-------------------------------------------------------
--fs -rm -r -f $outputPath
/**********
fs -rm -r -f '$outputPath$DOC_TERM_TITLE';
fs -rm -r -f '$outputPath$DOC_TERM_KEYWORDS';
fs -rm -r -f '$outputPath$DOC_TERM_ALL';
fs -rm -r -f '$outputPath$TERM_COUNT';
fs -rm -r -f '$outputPath$WORD_COUNT';
fs -rm -r -f '$outputPath$TFIDF_NON_WEIGHTED_SUBDIR';
fs -rm -r -f '$outputPath$TFIDF_TOPN_ALL_TEMP';
***********/
docIn = LOAD '$inputPath' USING pl.edu.icm.coansys.commons.pig.udf.
	RichSequenceFileLoader('org.apache.hadoop.io.Text','org.apache.hadoop.io.BytesWritable') 
	as (key:chararray, value:bytearray);
B = SAMPLE docIn $sample;
--B = limit docIn 100;
doc = FOREACH B GENERATE $0 as docId, DocToTupleMap($1) as document;

doc_raportX = foreach doc generate docId, document.title as title, document.abstract as abstract, document.keywords as keywords;
STORE doc_raportX INTO '$outputPath$RAW_DATA';
doc_raport = LOAD '$outputPath$RAW_DATA' as (docId:chararray, title:chararray, abstract:chararray, keywords:{keyword:(value:chararray)});

doc_raw = foreach doc_raport generate docId, title, abstract;
doc_keyword_raw = foreach doc_raport generate docId, FLATTEN(keywords) as keywords;

-- stem, clean, filter out
doc_keyword_all = stem_words(doc_keyword_raw, docId, keywords);
doc_title_all = stem_words(doc_raw, docId, title);
doc_abstract_all = stem_words(doc_raw, docId, abstract);

-- get all words (with duplicates for tfidf)
doc_allX = UNION doc_keyword_all, doc_title_all, doc_abstract_all;
doc_allX1 = foreach doc_allX generate TRIM($0) as docId:chararray, TRIM($1) as term:chararray;
doc_allX2 = filter doc_allX1 by 
 (docId is not null 
  and docId != ''
  and term is not null
  and term != ''); 

-- store document and terms
--STORE doc_title_all INTO '$outputPath$DOC_TERM_TITLE';
--STORE doc_keyword_all INTO '$outputPath$DOC_TERM_KEYWORDS';
STORE doc_allX2 INTO '$outputPath$DOC_TERM_ALL';

doc_all = LOAD '$outputPath$DOC_TERM_ALL' as (docId:chararray, term:chararray);
--**************** term count **********************
terms = foreach doc_all generate term;
group_by_terms = group terms by term;
X = foreach group_by_terms generate group as term;
X1 = group X all; 
tcX = foreach X1 generate COUNT(X) as count;

store tcX into '$outputPath$TERM_COUNT';
--**************** term count **********************

--**************** word count rank *****************
tc = load '$outputPath$TERM_COUNT' as (val:double);
wc = foreach group_by_terms generate COUNT(terms) as count, group as term;

-- #4074 rank operation seems to be bugged
-- https://github.com/openaire/iis/issues/927
-- wc_ranked = rank wc by count asc;
-- store wc_ranked into '$outputPath$WORD_RANK_HR'; 
wc_tmp = order wc by count asc parallel 1;
STORE wc_tmp INTO '$outputPath$WORD_RANK_HR' using pl.edu.icm.coansys.similarity.pig.serializers.RankStorage();
wc_ranked = LOAD '$outputPath$WORD_RANK_HR' as (rank_num:long, count:long, term:chararray);

term_condition_accepted_tmp = filter wc_ranked by ($0 <= (double)tc.val*$removal_rate and $1 >= $removal_least_used);
store term_condition_accepted_tmp into '$outputPath$WORD_COUNT_ACC';

term_condition_rejected_tmp = filter wc_ranked by ($0 > (double)tc.val*$removal_rate or $1 < $removal_least_used);
store term_condition_rejected_tmp into '$outputPath$WORD_COUNT_NEG';

term_condition_accepted_tmp_joined_with_docs = join term_condition_accepted_tmp by term, doc_all by term;
doc_selected_termsX = foreach term_condition_accepted_tmp_joined_with_docs generate doc_all::docId, doc_all::term;
store doc_selected_termsX into '$outputPath$WORD_COUNT';
--**************** word count rank *****************

--****************** tfidf calc ********************
doc_selected_terms = load '$outputPath$WORD_COUNT' as (docId:chararray, term:chararray);
tfidf_allX = calculate_tfidf_nofiltering(doc_selected_terms, docId, term);
-- store tfidf values into separate direcotires
STORE tfidf_allX INTO '$outputPath$TFIDF_NON_WEIGHTED_SUBDIR';
tfidf_all = load '$outputPath$TFIDF_NON_WEIGHTED_SUBDIR' as (docId:chararray, term:chararray, tfidf:float);
-- calculate and store topn terms per document in all results
tfidf_all_topn = get_topn_per_group(tfidf_all, docId, tfidf, 'desc', $tfidfTopnTermPerDocument);
tfidf_all_topn_projectedX = FOREACH tfidf_all_topn GENERATE top::docId AS docId, top::term AS term, top::tfidf AS tfidf;

X = GROUP tfidf_all_topn_projectedX BY docId;
X1 = FOREACH X GENERATE tfidf_all_topn_projectedX, COUNT(tfidf_all_topn_projectedX) as cnt;
X2 = FILTER X1 BY cnt >= $threshold_num_of_vector_elems_length;
X3 = FOREACH X2 GENERATE FLATTEN(tfidf_all_topn_projectedX) as (docId, term, tfidf);
STORE X3 INTO '$outputPath$TFIDF_TOPN_ALL_TEMP';

