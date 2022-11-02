--https://github.com/johnfouf/iis/blob/przemyslawjacewicz_1215_bio_entities_integration/iis-wf/iis-wf-referenceextraction/src/main/resources/eu/dnetlib/iis/wf/referenceextraction/springernature/bioentity/main_sqlite/oozie_app/lib/scripts/springer.sql
--attach database "../omirospubmed.db" as d1;
--create table mydata as select * from (setschema 'docid,text' select * from pmcfulltext);

create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());

------------------------------------------------------------------------------------------------------------------------------------------------------
-- arrayexpress
hidden var 'arrayexpress_prefixes' from
select jmergeregexp(jgroup(prefix))
from ( select "\b"||regexpr("-",prefix,"[\s|\W|-|:|_|.]{0,1}")||'\d+' as prefix
	 from ( select distinct c1 as prefix
		from ( select regexpr("\d", Accession,"") as c1
			from arrayexpress_experiments)));
hidden var 'arrayexpress_negativePrefixes' from select 'EERAD|EBAIR';
--ebi_ac_uk
hidden var 'ebi_ac_uk_prefixes' from select '(?:(?:\b|[^A-Z])EGAD[\s|\W|-|:|_|.]{0,1}\d{6,})|(?:(?:\b|[^A-Z])EGAS[\s|\W|-|:|_|.]{0,1}\d{6,})';
hidden var 'ebi_ac_uk_negativeWords' from select 'ANR';
------------------------------------------------------------------------------------------------------------------------------------------------------
--dbVar
hidden var 'dbvar_prefixes' from select '(?:\b[n|e|d]std\d+)|(?:\b[n|e|d]sv\d+)|(?:\b[n|e|d]ssv\d+)';
hidden var 'dbvar_prefixes2' from select '(?:[n|e|d]std\d+)|(?:[n|e|d]sv\d+)|(?:[n|e|d]ssv\d+)';
hidden var 'dbvar_middleNegativeWords'from select 'https?:|pdf|mail|meeting|@|com|org|res:';
hidden var 'dbvar_middlePositiveWords' from select '\.ncbi\.nlm.|\.ensembl\.org|\.genome\.wisc\.|snp';
hidden var 'dbvar_negativeWords' from select '10\.\d+\/|chongqing|dna res|e-mail|social cognitive and affective neuroscience|soc\.? cogn\.? affect\.? neurosci\.?|mg|kg|j\.? hered\.?';
--ENA
hidden var 'ena_prefixes' from
select '(?:\bPRJ[E|D|N][A-Z][0-9]+\b)|(?:\b[E|D|S]RP[0-9]{6,}\b)|\
(?:\bSAM[E|D|N][A-Z]?[0-9]{4,}\b)|(?:\b[E|D|S]R[S|X|R|Z][0-9]{6,}\b)|(?:\bGCA_[0-9]{9}\.[0-9]+\b)|\
(?:\b[A-Z]{1}[0-9]{5}\.[0-9]+\b)|(?:\b[A-Z]{2}[0-9]{6}\.[0-9]+\b)|(?:\b[A-Z]{2}[0-9]{8}\b)|\
(?:\b[A-Z]{4}[0-9]{2}S?[0-9]{6,8}\b)|(?:\b[A-Z]{6}[0-9]{2}S?[0-9]{7,9}\b)|\
(?:\b[A-Z]{3}[0-9]{5,7}\.[0-9]+\b)';

hidden var 'ena_prefixes_doi' from select '(\b10(\.\d+)+(\/\w+)?)';
hidden var 'ena_NegativeWords' from select "(?:agriculture|environmental protection agency|\bepa\b|patent|\[pii\]|\bgrants?\b)";
hidden var 'ena_NegativeWordsForReferences' from select '\b(j|journal)\b (\b(\w+)\b )*\b\d{4}\b';
hidden var 'ena_NegativeWordsForReferences2' from select '(?:(?:19\d{2,2})|(?:20\d{2,2})) \d{1,3} \d{1,3} \d{1,3}';
hidden var 'ena_NegativeWordsPrev' from select "(?:\b(bio)?ethic(s|al)?\b|\bchangzhou\b|\bjiangsu\b|\bchinese\b|\bprotocols?\b)";
hidden var 'ena_NegativeMiddle' from select "(?:\bMR\d+|\bNY\d+|\bPJ\d+|10\.\d*\/|doi|DOI)";
--EVA
hidden var 'eva_prefixes' from select '(?:\bPRJEB\d+)|(?:\b[n|e]std\d+)';
--FlowRepository
hidden var 'flowrep_prefixes' from select '(?:FR-FCM-\w{4})';
------------------------------------------------------------------------------------------------------------------------------------------------------
--EBIMetagenomics
hidden var 'EBIMetagenomics_prefixes' from select '(?:MGYS\d+)'; 197
--ΕΒΙMetabolights
hidden var 'EBIMetabolights_prefixes' from select '(?:\bMTBL[S|C]\d+)'; 66
--NCBIassembly
hidden var 'NCBIassembly_prefixes' from select '(?:\bGC[A|F]_\d{9}\.\d+)'; 474
--NCBI PubChem BioAssay & NCBI PubChem Substance
hidden var 'NCBIPubChem' from select '(?:pubchem\D+\d+)'; 87
--NCBI Taxonomy
hidden var 'NCBITaxonomy_prefixes' from select '(?:wwwtax\.cgi\D+\d+)|(?:txid\d+)'; 162
--NeuroMorpho
hidden var 'NeuroMorpho_prefixes' from select '(?:neuron_name=[\w+|-]+)|(?:NMO_\d{5,})'; 17
--BioModels
hidden var 'BioModels_prefixes' from select '(?:MODEL\d+)'; 119
hidden var 'BioModels_positivewords'from select '(?:ebi\.ac\.uk)|(?:biomodels?)';

------------------------------------------------------------------------------------------------------------------------------------------------------
--ArrayExpress ELENI
select jdict('documentId', docid,	'entity', 'ArrayExpress','biomedicalId', regexpr("("||var('arrayexpress_prefixes')||")", middle), 'confidenceLevel', 0.8,'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('arrayexpress_prefixes')) from mydata)
where regexprmatches("%{arrayexpress_negativePrefixes}",upper(middle)) = 0
union all
 --ebi_ac_uk ELENI
 select jdict('documentId', docid,'entity', 'ebi_ac_uk', 'biomedicalId', regexpr("("||var('ebi_ac_uk_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
 from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ebi_ac_uk_prefixes'))  from mydata )
 where regexprmatches("%{ebi_ac_uk_negativeWords}",upper(prev||middle)) = 0
union all
-- dbSNP: EL 06/2022 (I need feedback from Harry)!!!!
-- union all
-- flowRepository EL 06/2022
select jdict('documentId', docid, 'entity', "flowrepository", 'biomedicalId', regexpr("("||var('flowrep_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('flowrep_prefixes'))
                from (select docid, text from mydata))
     )
------------------------------------------------------------------------------------------------------------------------------------------
union all
-- EBIMetagenomics EL 06/2022
select jdict('documentId', docid, 'entity', "EBImetagenomics", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('EBIMetagenomics_prefixes'))
	              from mydata )
		 )
 union all
--ΕΒΙMetabolights EL 06/2022
select jdict('documentId', docid, 'entity', "ΕΒΙmetabolights", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('EBIMetabolights_prefixes'))
	              from mydata )
		 )
 union all
--NCBIassembly EL 06/2022
select jdict('documentId', docid, 'entity', "NCBIassembly", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBIassembly_prefixes'))
	              from mydata )
		 )
union all
--NCBI PubChem BioAssay & NCBI PubChem Substance EL 06/2022
select jdict('documentId', docid, 'entity', type, 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (  select docid, prev, middle, next,
				                   case when regexprmatches("bioassay|assay", lower(prev||" "||middle||" "||next)) = 1  then "NCBIPubChemBioassay"
				                        when regexprmatches("substances?", lower(prev||" "||middle||" "||next)) = 1  then "NCBIPubChemBioSubstance" end as type
				from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBIassembly_prefixes'))
							 from mydata )
				where regexprmatches("bioassay|assay", lower(prev||" "||middle||" "||next)) = 1 or
				     regexprmatches("substances?", lower(prev||" "||middle||" "||next)) = 1
	 )
union all
--NCBI Taxonomy EL 06/2022
select jdict('documentId', docid, 'entity', "NCBITaxonomy", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBITaxonomy_prefixes'))
	              from mydata )
		 )
union all
--NeuroMorpho EL 06/2022
select jdict('documentId', docid, 'entity', "NeuroMorpho", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NeuroMorpho_prefixes'))
	              from mydata )
		 )
union all
--BioModels EL 06/2022
 select jdict('documentId', docid, 'entity', "BioModels", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
 from (select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('BioModels_prefixes'))
                 from (select docid, text from mydata
                        where docid in (select distinct docid
                                         from ( select docid, prev, middle, next
                                       	        from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('BioModels_prefixes'))
                                       	              from mydata )
                                                where regexprmatches(var('BioModels_positivewords'), lower(prev||" "||middle||" "||next)) = 1
                                              )
                                       )))
 )
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
union all
--dbgap --Giannhs
select jdict('documentId', docid, 'entity', 'DBGAP', 'biomedicalId', match, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1 from
(select docid, jsplitv(regexprfindall(regexpr("(ph\w{7}\.\w\d\.p\w)",middle))) as match,  prev, middle, next from (setschema 'docid,prev,middle,next' select docid, textwindow2s(lower(regexpr("\n",text," ")), 10,1,5, "ph\w{7}\.\w\d\.p\w") from mydata) group by docid, match)
union all
--chembl -- Giannhs
select jdict('documentId', docid, 'entity', 'CHEMBL', 'biomedicalId', match, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1 from
(select docid, regexpr("(chembl\d{3,})", middle)  as match, prev, middle, next from (setschema 'docid,prev,middle,next' select docid, textwindow2s(lower(keywords(text)), 10,1,5, "chembl\d{3,}") from mydata) group by docid,match);


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
--uniprot  Giannhs
create temp table uniprot_results as select * from (
setschema 'docid, uniprot, prev, middle, next' select docid, case when regexprmatches('uniprot', lower(text)) then 1 else 0 end as uniprot ,textwindow2s(keywords(text),10,1,10,"\b([A-Z])([A-Z]|\d){5}\b") from mydata), uniprots where
middle = id;

select jdict('documentId', docid, 'entity', 'uniprot','biomedicalId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as res from uniprot_results where
((regexprmatches("\b\swiss\b|uniprot|swiss prot|uni prot|sequence|protein",lower(prev||" "||middle||" "||next)) or (regexprmatches("accession",lower(prev||" "||middle||" "||next)) and uniprot))
and not regexprmatches('\bFWF\b|\bARRS\b',(prev||" "||middle||" "||next))) group by docid, id
union
select jdict('documentId', docid, 'entity', 'uniprot','biomedicalId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as res from uniprot_results where docid in (
select  docid from uniprot_results where uniprot = 1 group by docid having count(*)>5) group by docid, id;


----------------------------------------------------------------------------------------------------------
----------------------------------------------------------------------------------------------------------

create temp table results_SRA_dbVar_ENA_EVA as
--SRA  ELENI
select docid as 'documentId',
       'SRA' as 'entity',
			 regexpr('(?:(?:\b|[^A-Z])(SR[A|P|X|R|S|Z][:|-|_|.]{0,1}\d+))', middle) as 'biomedicalId',
			 0.8 as 'confidenceLevel',
			 prev, middle, next
from ( setschema 'docid,prev,middle,next' select docid,textwindow2s(regexpr("\n",text," "), 10, 1, 10,'(?:(?:\b|[^A-Z])SR[A|P|X|R|S|Z][:|-|_|.]{0,1}\d{6})') from mydata)
------------------------------------------------------------------------------------------------------------------------------------------------------
union all
-- dbVar: EL 06/2022
select docid as 'documentId',
       'dbVar' as 'entity',
			 regexpr("("||var('dbvar_prefixes')||")", middle) as 'biomedicalId',
			 0.8 as 'confidenceLevel',
			 prev, middle, next
from ( select docid, prev, middle, next
       from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('dbvar_prefixes'))
                from (select docid, text from mydata))
        where (regexprmatches("%{dbvar_middleNegativeWords}",lower(middle)) = 0 or regexprmatches("%{dbvar_middlePositiveWords}",lower(middle)) = 1)
        and regexprmatches("%{dbvar_negativeWords}",lower(prev||' '||middle||' '||next)) = 0
        and length(regexpr("("||var('dbvar_prefixes2')||")", middle))>5
     )
union all
-- ENA: EL 06/2022
select docid as 'documentId',
       'ENA' as 'entity',
			 regexpr("("||var('ena_prefixes')||")", middle) as 'biomedicalId',
			 0.8 as 'confidenceLevel',
			prev, middle, next
from ( select docid, prev, middle, next
				from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ena_prefixes'))
			 					from (select docid, text from mydata ))
where regexprmatches(var('ena_NegativeMiddle'), middle) = 0 and
      regexprmatches(var('ena_prefixes_doi'), prev||" "||middle||" "||next) = 0 and
      regexprmatches(var('ena_NegativeWords'), lower(prev||" "||middle||" "||next)) = 0 and
      regexprmatches(var('ena_NegativeWordsForReferences'), lower(prev||" "||middle||" "||next)) = 0 and
      regexprmatches(var('ena_NegativeWordsForReferences2'), lower(prev||" "||middle||" "||next)) = 0 and
      regexprmatches(var('ena_NegativeWordsPrev'), lower(prev)) = 0
)
union all
-- EVA EL 06/2022
select docid as 'documentId',
       'EVA' as 'entity',
			 regexpr("("||var('eva_prefixes')||")", middle) as 'biomedicalId',
			 0.8 as 'confidenceLevel',
			 prev, middle, next
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('eva_prefixes'))
                  from (select docid, text from mydata))
     );


--Return the rows that are not duplicates
--create table temp resultsunique as
select jdict('documentId', documentId, 'entity', entity, 'biomedicalId', biomedicalId, 'confidenceLevel', 0.8, 'textsnippet',  (prev||" <<< "||middle||" >>> "||next)) as C1
from (
select documentId, biomedicalId, prev, middle, next, entity
from ( select documentId, biomedicalId, prev, middle, next, entity, count(entity) as size
        from (select * from results_SRA_dbVar_ENA_EVA group by documentId, entity, biomedicalId, prev, middle, next)
       group by documentId,biomedicalId, prev, middle, next)
where size = 1 );

--Return the rows that are duplicated
create temp table resultsduplicates as
select * from  results_SRA_dbVar_ENA_EVA where documentId||biomedicalId||prev||middle||next in
(select documentId||biomedicalId||prev||middle||next
	from (select documentId, biomedicalId, prev, middle, next, count(entity) as size
        from (select * from results_SRA_dbVar_ENA_EVA group by documentId, entity, biomedicalId, prev, middle, next) --Distinct values
        group by documentId,biomedicalId, prev, middle, next)
where size > 1) ;



select jdict('documentId', documentId, 'entity', entity, 'biomedicalId', biomedicalId, 'confidenceLevel', 0.8, 'textsnippet',  (prev||" <<< "||middle||" >>> "||next)) as C1
from (
select documentId, biomedicalId, prev, middle, next, entity
from resultsduplicates
where (
      --if accession ID is embedded within a URL	choose the repository that owns the URL
       regexprmatches("www\.[a-z|\/|\.|0-9]+", lower(prev||" "||middle||" "||next)) = 1
      and regexprmatches(lower(entity), regexpr("(www\.[a-z|\/|\.|0-9]+)", lower(prev||" "||middle||" "||next))) = 1
		  )
or --choose repository based on accession ID	SRP/SRX/SRR/SRS --> SRA
(regexprmatches("SR[P|X|R|S]", upper(middle)) = 1 and entity = 'SRA')
or --choose repository based on accession ID	 ERP/ERX/ERR --> ENA
(regexprmatches("ER[P|X|R]", upper(middle)) = 1 and entity = 'ENA')
or --PRJEB accessions should be assigned to ENA
(regexprmatches("PRJEB", upper(middle)) = 1 and entity = 'ENA')
or
(regexprmatches("dbvar|dgva", lower(prev||middle||next)) = 1 and entity = 'dbVar')
);
