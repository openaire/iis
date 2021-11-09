create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());
hidden var 'prefixes_arrayexpress' from
select jmergeregexp(jgroup(prefix))
from ( select "\b"||regexpr("-",prefix,"[\s|\W|-|:|_|.]{0,1}")||'\d+' as prefix
	 from ( select distinct c1 as prefix
		from ( select regexpr("\d", Accession,"") as c1
			from arrayexpress_experiments)));
hidden var 'prefixes_ebi_ac_uk' from select '(?:(?:\b|[^A-Z])EGAD[\s|\W|-|:|_|.]{0,1}\d+)|(?:(?:\b|[^A-Z])EGAS[\s|\W|-|:|_|.]{0,1}\d+)|(?:(?:\b|[^A-Z])phs\d+[\s|\W|-|:|_|.]{0,1}v\d+[\s|\W|-|:|_|.]{0,1}p\d+)';
hidden var 'dbvar_prefixes' from select '(?:\b[n|e|d]std\d+)|(?:\b[n|e|d]sv\d+)|(?:\b[n|e|d]ssv\d+)';
hidden var 'dbvar_middleNegativeWords'from select 'http:';
hidden var 'dbvar_middlePositiveWords' from select '\.ncbi\.nlm.|\.ensembl\.org|\.genome\.wisc\.|snp';
hidden var 'eva_prefixes' from select '(?:\bPRJEB\d+)|(?:\b[n|e]std\d+)';
hidden var 'flowrep_prefixes' from select '(?:FR-FCM-\w+)';
hidden var 'NCBIassembly_prefixes' from select '(?:\bGC[A|F]_\d{9}\.\d+)';
hidden var 'metabolights_prefixes' from select '(?:\bMTBL[S|C]\d+)';
hidden var 'NCBITaxonomy_prefixes' from select '(?:wwwtax\.cgi\D+\d+)|(?:txid\d+)';
hidden var 'NeuroMorpho_prefixes' from select '(?:neuron_name=[\w+|-]+)|(?:NMO_\d{5,})';
hidden var 'BioModels_prefixes' from select '(?:MODEL\d+)';
hidden var 'BioModels_positivewords'from select '(?:ebi\.ac\.uk)|(?:biomodels?)';
hidden var 'NCBIPubChemBioAssay_prefixes' from select '(?:pubchem\D+\d+)';

--SRA
select jdict('documentId', docid, 'entity' , 'SRA', 'biomedicalId', regexpr('(?:(?:\b|[^A-Z])(SR[A-Z][:|-|_|.]{0,1}\d{6}))', middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid,textwindow2s(regexpr("\n",text," "), 10, 1, 10,'(?:(?:\b|[^A-Z])SR[A-Z][:|-|_|.]{0,1}\d{6})')
	from mydata)

union all
--ArrayExpress
select jdict('documentId', docid, 'entity', 'ArrayExpress', 'biomedicalId', regexpr("("||var('prefixes_arrayexpress')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('prefixes_arrayexpress'))
         from mydata)

union all
--ebi_ac_uk
select jdict('documentId', docid, 'entity', 'ebi_ac_uk', 'biomedicalId', regexpr("("||var('prefixes_ebi_ac_uk')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('prefixes_ebi_ac_uk'))
         from mydata)
union all
--dbgap
select jdict('documentId', docid, 'entity', 'DBGAP', 'biomedicalId', match, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1 from
(select docid,regexpr("(ph\w{7}\.\w\d\.p\w)",middle) as match,  prev, middle, next from (setschema 'docid,prev,middle,next' select docid, textwindow2s(lower(regexpr("\n",text," ")), 10,1,5, "ph\w{7}\.\w\d\.p\w") from mydata) group by docid, match)

union all
--chembl
select jdict('documentId', docid, 'entity', 'CHEMBL', 'biomedicalId', match, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1 from
(select docid, regexpr("(chembl\d{3,})", middle)  as match, prev, middle, next from (setschema 'docid,prev,middle,next' select docid, textwindow2s(lower(keywords(text)), 10,1,5, "chembl\d{3,}") from mydata) group by docid,match)


union all
-- dbVar
select jdict('documentId', docid, 'entity', "dbvar",
  'biomedicalId', regexpr("("||var('dbvar_prefixes')||")", middle),
  'confidenceLevel', 0.8, 'textsnippet',
  (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
       from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('dbvar_prefixes'))
                from (select docid, text from mydata))
       where (regexprmatches("%{dbvar_middleNegativeWords}",lower(middle)) = 0 or regexprmatches("%{dbvar_middlePositiveWords}",lower(middle)) = 1)
     )

union all
-- EVA
select jdict('documentId', docid, 'entity', "EVA", 'biomedicalId', regexpr("("||var('eva_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('eva_prefixes'))
                  from (select docid, text from mydata))
     )

union all
-- lowRepository
select jdict('documentId', docid, 'entity', "flowrepository", 'biomedicalId', regexpr("("||var('flowrep_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('flowrep_prefixes'))
                from (select docid, text from mydata))
     )

union all
-- MetaboLights

select jdict('documentId', docid, 'entity', "MetaboLights", 'biomedicalId', regexpr("("||var('metabolights_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('metabolights_prefixes'))
                 from (select docid, text from mydata))
     )

union all
--NCBI assembly
select jdict('documentId', docid, 'entity', "NCBI Assembly", 'biomedicalId', regexpr("("||var('NCBIassembly_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBIassembly_prefixes'))
                  from (select docid, text from mydata))
      )

union all
-- NCBI PubChem BioAssay & NCBI PubChem Substance

select jdict('documentId', docid, 'entity', entity, 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next,
                    case when regexprmatches("bioassay|assay", lower(prev||" "||middle||" "||next)) = 1  then "NCBI PubChem BioAssay"
                         when regexprmatches("substances?", lower(prev||" "||middle||" "||next)) = 1  then "NCBI PubChem Substance" end as entity
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBIPubChemBioAssay_prefixes'))
                 from (select docid, text from mydata))
        where regexprmatches("bioassay|assay", lower(prev||" "||middle||" "||next)) = 1 or
              regexprmatches("substances?", lower(prev||" "||middle||" "||next)) = 1
    )

union all
-- NCBI Taxonomy
select jdict('documentId', docid, 'entity', "NCBI Taxonomy", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
				 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBITaxonomy_prefixes'))
				         from (select docid, text from mydata))
		 )

union all
--NeuroMorpho_prefixes
select jdict('documentId', docid, 'entity', "NeuroMorpho", 'biomedicalId', regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (  select docid, prev, middle, next
	        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NeuroMorpho_prefixes'))
	                 from (select docid, text from mydata))
			);

--BioModels
create temp table biomodels_pmcids as
select distinct docid
from (select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('BioModels_prefixes'))
                  from (select docid, text from mydata))
        where regexprmatches(var('BioModels_positivewords'), lower(prev||" "||middle||" "||next)) = 1
);

select jdict('documentId', docid, 'entity', "BioModels", 'biomedicalId', regexpr("("||var('BioModels_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
				 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('BioModels_prefixes'))
				         from (select docid, text from mydata where docid in (select * from biomodels_pmcids)))
			);
			
--uniprot
create temp table uniprot_results as select * from (
setschema 'docid, uniprot, prev, middle, next' select docid, case when regexprmatches('uniprot', lower(text)) then 1 else 0 end as uniprot ,textwindow2s(keywords(text),10,1,10,"\b([A-Z])([A-Z]|\d){5}\b") from mydata), uniprots where
middle = id;


select jdict('documentId', docid, 'entity', 'uniprot','biomedicalId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as res from uniprot_results where 
((regexprmatches("\b\swiss\b|uniprot|swiss prot|uni prot|sequence|protein",lower(prev||" "||middle||" "||next)) or (regexprmatches("accession",lower(prev||" "||middle||" "||next)) and uniprot))
and not regexprmatches('\bFWF\b|\bARRS\b',(prev||" "||middle||" "||next))) group by docid, id
union
select jdict('documentId', docid, 'entity', 'uniprot','biomedicalId', id, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as res from uniprot_results where docid in (
select  docid from uniprot_results where uniprot = 1 group by docid having count(*)>5) group by docid, id;


