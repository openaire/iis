--https://github.com/johnfouf/iis/blob/przemyslawjacewicz_1215_bio_entities_integration/iis-wf/iis-wf-referenceextraction/src/main/resources/eu/dnetlib/iis/wf/referenceextraction/springernature/bioentity/main_sqlite/oozie_app/lib/scripts/springer.sql
--attach database "../omirospubmed.db" as d1;
--create table mydata as select * from (setschema 'docid,text' select * from pmcfulltext );

--*****************************************************************************************************************
-- --For testing the results:
-- Ι need this db:  /home/openaire/springer.db
-- cd /storage/eleni/openAIRE/05.Biomedical_Springer
-- cat pubs.json | python madis/src/mexec.py -f query.sql -d data.db > result.json
-- cat pubs.json | python ~/Desktop/openAIRE/madis2/src/mexec.py -f springerFromFouf_EL_v5.sql -d  /home/openaire/springer.db > results.json
-- --Create pubs.json as follows:
-- --a)

--The ids of the documents I used are
-- --1.1 ArrayExpress (line 4)                            PMC2932696, PMC2936545
-- --1.2 European Genome-phenome Archive (lines 27,28)    PMC2945784, PMC3873028
-- --2.1 flowRepository  (line 32)                        PMC4238829, PMC3906045
-- --2.5 DNA Databank of Japan  (line 21)                 PMC3738164, PMC3749161
-- --3.1 EBIMetagenomics (line 24)
-- --3.2 ΕΒΙMetabolights  (line 47)                       PMC4419159, PMC4421934
-- --3.3 NCBIassembly   (line 54)                         PMC3878773, PMC3882889
-- --3.4 NCBI PubChem    (lines 55,56,57)
-- --3.5 NCBI Taxonomy  (line 59)                         PMC3742277, PMC3744899
-- --3.6 NeuroMorpho  (line 66)                           PMC4325909, PMC3324298
-- --3.7 Biomodels (line 9)                               PMC2944785, PMC2950841
-- --3.8 Database of Interacting Proteins (line 17)       PMC2762066, PMC3098085
-- --4.1 FlyBase (line 33)                                PMC2943075, PMC2952598
-- --4.2 Gene Expression Omnibus  (line 36)               PMC2931699, PMC2931704
-- --4.3 IntAct     (line 40)                             PMC2553489, PMC4384031
-- --4.4 openfMRI     (line 71)                           PMC4392315, PMC3703526
-- --4.5 OpenNeuro     (line 73)                          PMC4392315, PMC4412149
-- --4.6 Peptide Atlas  (line 75)                         PMC2481425, PMC549070
-- --4.7 PRIDE Archive  (line 77)                         PMC3879382, PMC3879621
-- --5.1 Clinical Trials   (line 13)                      PMC2931489, PMC2931508
-- --5.2 XenBase  (line 94)                               PMC4159511
-- --5.3 Mouse Genome Informatics (line 49)               PMC4404804, PMC2098773
-- --5.4 Australian Ocean Data Network (line 6)
-- --5.5 British Oceanographic Data Centre (NERC Data Centres) (line 62)
-- --5.6 Centre for Environmental Data Analysis (NERC Data Centres) (line 63)
-- --5.7 National Geoscience Data Centre (NERC Data Centres) (line 64)
-- --5.8 UK Polar Data Centre (NERC Data Centres) (line 65)
-- --5.9 NOAA National Centers for Environmental Information (line 67)
-- -- 6.1 Zebrafish  (line 95)                                      PMC3739779, PMC3750112
-- --6.2 Virtual Skeleton (line 91)                                                      --> 0 results @ Springer
-- --6.3 VectorBase (line 90)
-- --6.4 Protein Circular Dirchroism Data Bank (line 78)           PMC3493636, PMC4004220
-- --6.5 National Database for Autism Research (line 52)           PMC4249945, PMC3651174
-- --6.6 National Addiction & HIV Data Archive Program (line 51)   PMC3704700
-- --6.7 Archaeology Data Service (line 3)                         PMC3493552
-- --6.8 Australian Antarctic Data Centre (line 5)                 PMC3733920, PMC4373865
-- --6.9 caNanoLab (line 10)                                                             --> 0 results @ Springer
-- --6.10 GenomeRNAi (line 37)                                     PMC3749438, PMC4365872
-- --6.11 Japanese Genotype Phenotype Archive (line 42)            PMC4263407, PMC4383935
-- --6.12 Environmental Information Data Centre (line 61)          PMC4277193, PMC4173671
-- --6.13 SIBMAD Astronomical Database (line 83)                                         --> 0 results @ Springer
-- --6.14 Influenza Reseach Database (line 39)                                            --> 0 results @ Springer
-- --6.15 Kinetic Models of Biological Systems (line 43)

-- --1.3. SRA (line 58)                                           PMC2936537, PMC2938879
-- --2.2.dbVar (line 20)                                          PMC3740631, PMC3744852
-- --2.3. ENA (line 29)                                           PMC2933243, PMC2933595
-- --2.4. EVA (line 30)                                           PMC4295026, PMC4295026
-- --5.10 DGVa (line 16)

--Giannis
-- --UniprotKB  (line 89)
-- --ChEMBL (line 12)
-- --dbgap (line 18)
-- --wwPDB (lines 93, 25 TODO)

-- --
-- create table mydata as select * from (setschema 'id,text' select * from pmcfulltext where
--   pmcid ='PMC2932696' or  pmcid ='PMC2936545' or
--   pmcid ='PMC2945784' or  pmcid ='PMC3873028' or
--   pmcid ='PMC4238829' or  pmcid ='PMC3906045' or
--   pmcid ='PMC3738164' or  pmcid ='PMC3749161' or
--   pmcid ='PMC4419159' or  pmcid ='PMC4421934' or
--   pmcid ='PMC3878773' or  pmcid ='PMC3882889' or
--   pmcid ='PMC4249827' or  pmcid ='PMC3537644' or
--   pmcid ='PMC4173839' or  pmcid ='PMC4026626' or
--   pmcid ='PMC3742277' or  pmcid ='PMC3744899' or
--   pmcid ='PMC4325909' or  pmcid ='PMC3324298' or
--   pmcid ='PMC2944785' or  pmcid ='PMC2950841' or
--   pmcid ='PMC2762066' or  pmcid ='PMC3098085' or
--   pmcid ='PMC2943075' or  pmcid ='PMC2952598' or
--   pmcid ='PMC2931699' or  pmcid ='PMC2931704' or
--   pmcid ='PMC2553489' or  pmcid ='PMC4384031' or
--   pmcid ='PMC4392315' or  pmcid ='PMC3703526' or
--   pmcid ='PMC4392315' or  pmcid ='PMC4412149' or
--   pmcid ='PMC2481425' or  pmcid ='PMC549070' or
--   pmcid ='PMC3879382' or  pmcid ='PMC3879621' or
--   pmcid ='PMC2931489' or  pmcid ='PMC2931508' or
--   pmcid ='PMC4159511' or
--   pmcid ='PMC4404804' or  pmcid ='PMC2098773' or
--   pmcid ='PMC3739779' or  pmcid ='PMC3750112' or
--   pmcid ='PMC3493636' or  pmcid ='PMC4004220' or
--   pmcid ='PMC4249945' or  pmcid ='PMC3651174' or
--   pmcid ='PMC3704700' or
--   pmcid ='PMC3493552' or
--   pmcid ='PMC3733920' or  pmcid ='PMC4373865' or
--   pmcid ='PMC3749438' or  pmcid ='PMC4365872' or
--   pmcid ='PMC4263407' or  pmcid ='PMC4383935' or
--   pmcid ='PMC4277193' or  pmcid ='PMC4173671' or
--  pmcid ='PMC2936537' or  pmcid ='PMC2938879' or
--  pmcid ='PMC3740631' or  pmcid ='PMC3744852' or
--  pmcid ='PMC2933243' or  pmcid ='PMC2933595' or
--  pmcid ='PMC4295026' or  pmcid ='PMC4295026'
-- );
-- -- --
--  insert into mydata select "PMC0001","
--   EBIMetagenomics:  my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text MGYS234234242 my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text
--   Australian Ocean Data Network (AODN): by a Seabird conductivity cell associated with the SOMMA (metadata:  https://catalogue-imos.aodn.org.au/geonetwork/srv/eng/metadata.show?uuid=fa93c66e-0e56-7e1d-e043-08114f8c1b76 ). Precision and reproducibility for DIC and A T was
--   British Oceanographic Data Centre: http://www.gebco.net/data_and_products/gridded_bathymetry_data/gebco_one_minute_grid. 45. British Oceanographic Data Centre, The GEBCO_ 2014 grid. http://www.bodc.ac.uk/data/documents/nodb/301801/. 46. Amante C. Eakins B. W. ETOPO1 1 Arc-Minute Global
--   Centre for Environmental Data Analysis (CEDA): ritish Atmospheric Data Centre, 10 March 2018 (Met Office, 2007), http://catalogue.ceda.ac.uk/uuid/7a62862f2f43c0bdf4e7d152b6cb59e4 . 26. MODIS Collection 6 NRT Hotspot/Active Fire Detection MCD14DL
--   National Geoscience Data Center (NGDC): in National Geoscience Data Center (NGDC) via the following link: https://webapps.bgs.ac.uk/services/ngdc/accessions/index.html#item164865 . Competing interests The authors declare no competing interests. References
--   UK Polar Data Centre (NERC Data Centres): dated ESA Baseline-D version of the Baseline-C dataset (available at https://data.bas.ac.uk/full-record.php?id=GB/NERC/BAS/PDC/01257  ). The LARM algorithm accounts for variable sea-ice surface roughness
--   NOAA National Centers for Environmental Information (NCEI): are available from https://www.metoffice.gov.uk/hadobs/hadisst/ The ERSST data are available from  https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00927  . The ERA20C are available from https://www.ecmwf.int/en/forecasts/datasets/reanalysis-datasets/era-20c . The GFEDv4
--   VectorBase: the raw sequencing files were first obtained from VectorBase (https://vectorbase.org/vectorbase/app/record/dataset/DS_1c16f776df ), QCâ€™d using fastqc 0.11.8 and multiqc 1.9, and analyzed
--   Virtual Skeleton my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text https://www.smir.ch/Objects/262017 my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text
--   caNanoLab my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text https://cananolab.nci.nih.gov/caNanoLab/#/sample?sampleId=6782978 my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text
--   SIBMAD my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text http://cds.u-strasbg.fr/cgi-bin/Dic-Simbad?AKARI-FIS-V1 my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text
--   Influenza Reseach Database my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text https://www.bv-brc.org/view/Genome/582419.3 my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text
--   Kinetic Models of Biological Systems my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text https://kimosys.org/repository/66 my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text
--   PubchemBioassay my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text https://identifiers.org/pubchem.bioassay:1259428 my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text
--   Pubchem Substance my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text (http://pubchem.ncbi.nlm.nih.gov/substance/313573313) my text my text my text my text my text my text my text my text my text my text my text my text my text my text my text.";


-- output 'pubs_V4.txt' select jdict('id', id, 'text', text) from mydata;
-- cp pubs_V4.txt pubs_V4.json
-- --b)
-- Create file pubs.json that contains: i


-- Ola ta data pubs_pmcfulltext.txt
--create table mydata_pmcfulltext as select * from (setschema 'id,text' select * from pmcfulltext);
--output 'pubs_pmcfulltext.txt' select jdict('id', id, 'text', text) from mydata_pmcfulltext;
--rename -v 's/.txt/.json/' pubs_pmcfulltext.txt

--**************************************************************************************************************
create temp table mydata as select * from (setschema 'docid,text' select jsonpath(c1,'$.id', '$.text') from stdinput());

------------------------------------------------------------------------------------------------------------------------------------------------------
-------BATCH 1
-- arrayexpress
--https://www.ebi.ac.uk/biostudies/arrayexpress/help#ae-data
hidden var 'arrayexpress_prefixes' from select '((?:E|A)-(?:AFFY|AFMX|AGIL|ATMX|BAIR|BASE|BIOD|BUGS|CAGE|CBIL|DKFZ|DORD|EMBL|ERAD|FLYC|FPMI|GEAD|GEHB|GEOD|GEUV|HCAD|HGMP|IPKG|JCVI|JJRD|LGCL|MANP|MARS|MAXD|MEXP|MIMR|MNIA|MTAB|MUGN|NASC|NCMF|NGEN|RUBN|RZPD|SGRP|SMDB|SNGR|SYBR|TABM|TIGR|TOXM|UCON|UHNC|UMCU|WMIT)-\d+)';
hidden var 'arrayexpress_negativePrefixes' from select 'EERAD|EBAIR';

--European Genome-phenome Archive
hidden var 'ebi_ac_uk_prefixes' from select '(?:(?:\b|[^A-Z])EGAD[\s|\W|-|:|_|.]{0,1}\d{9,})|(?:(?:\b|[^A-Z])EGAS[\s|\W|-|:|_|.]{0,1}\d{9,})';
hidden var 'ebi_ac_uk_prefixes2' from select '(?:EGAD[\s|\W|-|:|_|.]{0,1}\d{9,})|(?:EGAS[\s|\W|-|:|_|.]{0,1}\d{9,})';
hidden var 'ebi_ac_uk_negativeWords' from select 'ANR';


------------------------------------------------------------------------------------------------------------------------------------------------------
-------BATCH 2
--dbVar
hidden var 'dbvar_prefixes' from select '(?:\b[n|e|d]std\d+)|(?:\b[n|e|d]sv\d+)|(?:\b[n|e|d]ssv\d+)';
hidden var 'dbvar_prefixes2' from select '(?:[n|e|d]std\d+)|(?:[n|e|d]sv\d+)|(?:[n|e|d]ssv\d+)';
hidden var 'dbvar_middleNegativeWords'from select 'https?:|pdf|mail|meeting|@|com|org|res:|\.gov|=|~|\besv100\b|\besv115\b|\bdssv08\b';
hidden var 'dbvar_middlePositiveWords' from select '\.ncbi\.nlm.|\.ensembl\.org|\.genome\.wisc\.|snp';
hidden var 'dbvar_negativeWords' from select '10\.\d+\/|chongqing|dna res|e-mail|journal of heredity|scan|neurosc|mg|kg|j\.? hered\.?|astronomy|astrophysics';
--ENA
hidden var 'ena_prefixes' from
select '(?:\bPRJ[E|D|N][A-Z][0-9]+\b)|(?:\b[E|D|S]RP[0-9]{6,}\b)|(?:\bSAM[E|D|N][A-Z]?[0-9]{4,}\b)|(?:\b[E|D|S]R[S|X|R|Z][0-9]{6,}\b)|(?:\bGCA_[0-9]{9}\.[0-9]+\b)|(?:\b[A-Z]{1}[0-9]{5}\.[0-9]+\b)|(?:\b[A-Z]{2}[0-9]{6}\.[0-9]+\b)|(?:\b[A-Z]{2}[0-9]{8}\b)|(?:\b[A-Z]{4}[0-9]{2}S?[0-9]{6,8}\b)|(?:\b[A-Z]{6}[0-9]{2}S?[0-9]{7,9}\b)|(?:\b[A-Z]{3}[0-9]{5,7}\.[0-9]+\b)';

hidden var 'ena_prefixes_doi' from select '(\b10(\.\d+)+(\/\w+)?)';
hidden var 'ena_NegativeWords' from select "(?:agriculture|environmental protection agency|physics|\bepa\b|patent|\[pii\]|\bgrants?\b)";
hidden var 'ena_NegativeWordsForReferences' from select '\b(j|journal)\b (\b(\w+)\b )*\b\d{4}\b';
hidden var 'ena_NegativeWordsForReferences2' from select '(?:(?:19\d{2,2})|(?:20\d{2,2})) \d{1,3} \d{1,3} \d{1,3}';
hidden var 'ena_NegativeWordsPrev' from select "(?:\b(bio)?ethic(s|al)?\b|\bchangzhou\b|\bjiangsu\b|\bchinese\b|\bprotocols?\b)";
hidden var 'ena_NegativeMiddle' from select "(?:\bMR\d+|\bNY\d+|\bPJ\d+|10\.\d*\/|doi|DOI)";
--EVA
hidden var 'eva_prefixes' from select '(?:\bPRJEB\d+)';
--2.1 FlowRepository
hidden var 'flowrep_prefixes' from select '(?:FR-FCM-\w{4})';
--2.5 DNA DataBank of Japan
hidden var 'DNADatabankOfJapan_prefixes1' from select "dna data.?bank of japan|\bddbj\b";
hidden var 'DNADatabankOfJapan_prefixes2' from select "\bDR[P|R|X|S|Z]\d{6}\b";
--dbSNP --> TODO

------------------------------------------------------------------------
-------BATCH 3
--3.1 EBIMetagenomics
hidden var 'EBIMetagenomics_prefixes' from select '(?:MGYS\d{8}})';
--3.2 ΕΒΙMetabolights
hidden var 'EBIMetabolights_prefixes' from select '(?:\bMTBL[S|C]\d+)';
--3.3 NCBIassembly
hidden var 'NCBIassembly_prefixes' from select '(?:\bGC[A|F]_\d{9}\.\d+)';
--3.4a NCBI PubChem BioAssay
hidden var 'NCBIPubChemBioAssey' from select '(?:pubchem\.ncbi\.nlm\.nih\.gov\/bioassay\/\d+)|(?:identifiers\.org\/pubchem\.bioassay:\d+)|(?:pubchem\.ncbi\.nlm\.nih\.gov\/assay\/assay\.cgi\?aid=\d+)';
--3.4b  NCBI PubChem Substance
hidden var 'NCBIPubChemSubstance' from select'(?:pubchem\.ncbi\.nlm\.nih\.gov\/substance\/\d+)';

--3.5 NCBI Taxonomy
hidden var 'NCBITaxonomy_prefixes' from select '(?:wwwtax\.cgi\D+\d+)|(?:txid\d+)';
--3.6 NeuroMorpho
hidden var 'NeuroMorpho_prefixes' from select '(?<=neuron_name=)[\w+|-]+|(?:NMO_\d{5,})';
--3.7 BioModels
hidden var 'BioModels_prefixes' from select '(?:MODEL\d{6,})';
hidden var 'BioModels_positivewords'from select '(?:ebi\.ac\.uk)|(?:biomodels?)';
--3.8 Database of Interacting Proteins
hidden var 'DIP_prefixes1' from select "\bDIP\b|(?:i|I)nteractive (?:p|P)roteins";
hidden var 'DIP_prefixes2' from select "DIP(?: |-|:)\d{1,7}N\b";
------------------------------------------------------------------------
-------BATCH 4
--4.1 FlyBase
hidden var 'FlyBase_prefixes' from select '(\bFB(?:ab|al|ba|cl|gg|gn|hh|im|ig|Ic|mc|ms|pp|rf|sf|sn|st|tc|te|ti|to|tp|tr|an)\d{5,}\b)';
--4.2 Gene Expression Omnibus
hidden var 'GEO_prefixes' from select '((?:GPL|GSM|GSE|GDS)\d+\b)';
--4.3 IntAct
hidden var 'intAct_prefixes' from select '(EBI-\d{5,})';
--4.4 openfMRI
hidden var 'openfMRI_prefixes3' from select "\bds\d{6}[a-f]?\b";
hidden var 'openfMRI_prefixes1' from select "\bopen.?fmri\b";
--4.5 OpenNeuro
hidden var 'OpenNeuro_prefixes3' from select "\bds\d{6}[a-f]?\b";
hidden var 'OpenNeuro_prefixes1' from select "\bopen.?neuro\b|10\.18112";
--4.6 Peptide Atlas
hidden var 'peptideatlas_prefixes' from select '(PAp\d{8}\b)';
--4.7 PRIDE Archive
hidden var 'prideArchive_prefixes' from select '(PXD\d{2,}\b)';

------------------------------------------------------------------------
-------BATCH 5
--5.1 Clinical Trials
hidden var 'clinicaltrial_prefixes' from select 'NCT\d{6,}';
--5.2 XenBase
hidden var 'XenBase_prefixes' from select 'XB-GENE-\d+';
--5.3 Mouse Genome Informatics
hidden var 'mousegenomeinformatics_prefixes' from select 'MGI:\d+';
--5.4 Australian Ocean Data Network
--https://catalogue-imos.aodn.org.au/geonetwork/srv/eng/metadata.show?uuid=ae86e2f5-eaaf-459e-a405-e654d85adb9c
hidden var 'AODN_prefixes' from select 'catalogue-imos\.aodn\.org\.au.+=([a-z0-9-]+)';
--catalogue-imos\.aodn\.org\.au.+=(.+)$
--5.5 British Oceanographic Data Centre (NERC Data Centres)
--https://www.bodc.ac.uk/data/documents/series/1620054/
--https://www.bodc.ac.uk/data/bodc_database/nodb/data_collection/207/
hidden var 'BODC_prefixes' from select 'www\.bodc\.ac\.uk\/data[a-z\/_]+(\d+).+$';
--((?:www\.bodc\.ac\.uk\/data\/published_data_library\/catalogue\/10\.5285\/[a-z0-9-\s]+\/|
--(?:www\.bodc\.ac\.uk\/data\/information_and_inventories\/edmed\/report\/\d+\/)
--(?:www\.bodc\.ac\.uk\/data\/documents\/cruise\/\d+\/)
--(?:www\.bodc\.ac\.uk\/data\/documents\/nodb\/\d+\/)
--(?:www\.bodc\.ac\.uk\/data\/information_and_inventories\/cruise_inventory\/report\/\d+\/))
-- https://www.bodc.ac.uk/data/published_data_library/catalogue/10.5285/89a3a6b8-7223-0b9c-e053-6c86abc0f15d/
-- http://www.bodc.ac.uk/data/information_and_inventories/edmed/report/4806/
-- https://www.bodc.ac.uk/data/documents/cruise/6722/
-- www.bodc.ac.uk/data/documents/nodb/254628/
-- https://www.bodc.ac.uk/data/information_and_inventories/cruise_inventory/report/9359/

--5.6 Centre for Environmental Data Analysis (NERC Data Centres)
--https://catalogue.ceda.ac.uk/uuid/8b5b67edc9c648189d27b6432bdad215
hidden var 'CEDA_prefixes' from select 'catalogue\.ceda\.ac\.uk\/uuid\/([0-9a-z]{32})';
--'catalogue\.ceda\.ac\.uk\/uuid\/(.+)$'
--5.7 National Geoscience Data Centre (NERC Data Centres)
--https://webapps.bgs.ac.uk/services/ngdc/accessions/index.html?simpleText=rock#item38088
hidden var 'NGDC_prefixes' from select 'webapps\.bgs\.ac\.uk\/services\/ngdc\/accessions.+item(\d+)';
--5.8 UK Polar Data Centre (NERC Data Centres)
--https://data.bas.ac.uk/metadata.php?id=GB/NERC/BAS/PDC/00272
hidden var 'BAS_prefixes' from select 'GB\/NERC\/BAS\/PDC\/(\d+)$';
--5.9 NOAA National Centers for Environmental Information
--https://www.ncei.noaa.gov/access/metadata/landing-page/bin/iso?id=gov.noaa.ncdc:C00824
hidden var 'NCEI_prefixes' from select 'www\.ncei\.noaa\.gov.+=gov\.noaa\.ncdc\:(C\d+)';
--'www\.ncei\.noaa\.gov.+=gov\.noaa\.ncdc\:(.+)$'
------------------------------------------------------------------------
--BATCH 6
--6.1 Zebrafish
hidden var 'zebrafish_prefixes' from select '\bZDB-[A-Z]+-\d+-?\d+';

--6.2 Virtual Skeleton
--To id einai apo 2 ews 886000 ara paw me url (https://www.smir.ch/Objects/262017)
hidden var 'VirtualSkeleton_prefixes' from select 'smir\.ch\/Objects\/\d+';
--6.3 VectorBase (https://vectorbase.org/vectorbase/app/record/dataset/DS_e2eead5bd4)
hidden var 'VectorBase_prefixes' from select 'vectorbase\.org\/vectorbase\/app\/record\/dataset\/DS_\w{10}';
--6.4 Protein Circular Dirchroism Data Bank
hidden var 'PCDDB_prefixes1' from select '(?:pcddb)|(?:pdcdb)|(?:protein circular dichroism data bank)|(?:dichroism)';
hidden var 'PCDDB_prefixes2' from select 'CD\d{10}';
hidden var 'PCDDB_negativePrefixes' from select '(?:10/.\d+)|(?:Cochrane)';
--6.5 National Database for Autism Research
hidden var 'NDAR_prefixes1' from '(?:ndar)|(?:national database for autism)';
hidden var 'NDAR_prefixes2' from '(?:NDARCOL\d+)';
              --PMC4379694   These data have been deposited into the National Database for Autism Research (Experiment ID: 182).
              --PMC4270294  The RNA-Seq data have been deposited in the National Database for Autism Research (NDAR) under the accession code NDARCOL0002034.
              --PMC3494744 Research under accession number NDARCOL0001951.
              --PMC4240813  All of the HiSeq data from eight SSC samples have been submitted to the National Database for Autism Research (NDAR) [46] under collection ‘Wigler SSC autism exome families’ (project number: 1936).
              --PMC3651174  As a result, the current study included the following two collection IDs (along with submitters): NDARCOL0001356 (Bradley Peterson); and NDARCOL0001551 (Francisco Castellanos). The age and gender matched healthy subjects were selected from the Pediatric MRI Data Repository of NDAR, including 51 typically developing children (Table 1).
--6.6 National Addiction & HIV Data Archive Program
--hidden var 'NAHDAP_prefixes1' from '(?:nahdap)|(?:national addiction & hiv data)'
hidden var 'NAHDAP_prefixes2' from 'NAHDAP\/studies\/\d+';
--6.7 Archaeology Data Service
hidden var 'ArchaeologyDataService_prefixes' from '(10\.5284\/\w+)|(?:archaeologydataservice\.ac\.uk\/archsearch\/record\?titleId=\d+)';
hidden var 'ArchaeologyDataService_prefixes2' from '(?<=10\.5284\/)\w+|(?<==)\d+';
--6.8 Australian Antarctic Data Centre
hidden var 'AustralianAntarcticDataCentre_prefixes'  from '(10\.4225\/15\/\w+-?\w{0,})|(10\.26179\/\w+-?\w{0,})';
-- '(10\.4225\/15\/\w+)|(10\.26179\/\w+)'
hidden var 'AustralianAntarcticDataCentre_prefixes2' from '(?<=10\.4225\/15\/)\w+-?\w{0,}|(?<=10\.26179\/)\w+-?\w{0,}';
--'(?<=10\.4225\/15\/)\w+|(?<=10\.26179\/)\w+'
      -- PMC3733920 This work was supported by the Australian Antarctic Division (ASAC 3095), www.aad.gov.au
      -- PMC4409151 The work was funded by the Australian Antarctic Division (www.aad.gov.au) for approved AAS project 4088.
      --            All relevant data are available within the paper, its Supporting Information files, and from the Australian Antarctic Data Centre under the DOI: http://dx.doi.org/10.4225/15/54752B4B845C7.
      -- PMC4222203 Trull TW, Bray SG. Subantarctic zone (SAZ) project and sediment trap moorings. Australian Antarctic Data Centre - CAASM Metadata; 2012. Available at http://data.aad.gov.au/aadc/metadata/metadata_redirect.cfm?md=SAZOTS, (accessed on 31 July 2013) [Google Scholar]
      -- PMC2881033 by the Australian Antarctic Science Advisory Committee (AAS Projects 472, 2208, and 2070)
      -- PMC3981711 This study was carried out in strict accordance with the approvals and conditions set by the Australian Antarctic Division’s Antarctic Animal Ethics Committee for this project - Australian Antarctic Science project 2941.
      -- PMC4373865 Data for this project are currently housed at the Australian Antarctic Data Centre (DOIs http://dx.doi.org/10.4225/15/531FD86AAF564; http://dx.doi.org/10.4225/15/531FEC9077D6E) and are publicly available.
      -- PMC3867902 Sightings database of Macquarie Island Elephant Seals: Australian Antarctic Data Centre (https://data.aad.gov.au/), entry ID: AADC-00102.

--6.9 caNanoLab
hidden var 'caNanoLab_prefixes' from 'cananolab\.nci\.nih\.gov\/caNanoLab\/#\/sample\?sampleId=\d+';
--6.10 GenomeRNAi
hidden var 'GenomeRNAi_prefixes1' from 'genomernai|genome rnai|genome-rnai';
hidden var 'GenomeRNAi_prefixes2' from select '(GR\d{5}-(?:A|S|C)(?:-\d+)?)';
          -- NOTE: Entrez ID vs Gene ID - the Gene ID column contains the gene identifier as provided by the authors of the screen and
          -- can therefore come from various sources when comparing different screens. We have attempted to map all Gene IDs to Entrez Gene IDs
          -- in order to provide one data column with uniform identifiers.

          -- NOTE: Stable IDs are assigned according to the following convention:
          -- GR00123-A = core ID for a screen curated from the literature
          -- GR00123-S = core ID for a screen submitted directly by the author
          -- GR00123-C = core ID for a screen imported via cellHTS2
          -- GR00123-A-0 = indicates that the data from this screen still need to be
          -- updated to the current annotation standard (pre-existing data)
          -- GR00123-A-1, GR00123-A-2, etc. = indicates that this is one screen out of
          -- several, published within the same publication
--6.11 Japanese Genotype Phenotype Archive'
hidden var 'Japanese_prefixes1' from '(?:japanese genotype-phenotype)|(?:ddbj)|(?:dna databank of japan)';
hidden var 'Japanese_prefixes2' from 'JGA[A-Z]\d+';
--6.12 Environmental Information Data Centre
--hidden var 'EnvironmentalInformationDataCentre_prefixes'  from '10\.5285\/[\w|-]+'
hidden var 'EnvironmentalInformationDataCentre_prefixes' from '(?:10\.5285\/)((?:[a-z0-9-]{1,})+(?:[\s|-]?)(?:[a-z0-9-]{4,})){0,1}(?:(?:[A-Z0-9-]{1,})+(?:[\s|-]?)(?:[A-Z0-9-]{4,})){0,1}';


--(?:10\.5285\/)((?:[a-z0-9-]{1,})+(?:[\s|-]?)(?:[a-z0-9-]{4,})){0,1}(?:(?:[A-Z0-9-]{1,})+(?:[\s|-]?)(?:[A-Z0-9-]{4,})){0,1}


--'(?<=10\.5285\/)[\w|-]+'
--6.13 SIBMAD
hidden var 'Sibmad_prefixes' from 'cds\.u-strasbg\.fr\/cgi-bin\/Dic-Simbad\?[\w|-]+';
hidden var  'Sibmad_prefixes2' from '(?<=cds\.u-strasbg\.fr\/cgi-bin\/Dic-Simbad\?)[\w|-]+';
--6.14 Influenza Reseach Database
hidden var 'Influenza_prefixes' from  'www\.bv-brc\.org\/view\/Genome\/[\d|\.]+';
hidden var 'Influenza_prefixes2' from  'www\.bv-brc\.org\/view\/Genome\/([\d\.]+\d)';
--6.15 Kinetic Models of Biological Systems
hidden var 'KiMoSys_prefixes' from 'kimosys\.org\/repository\/\d+';

------------------------------------------------------------------------------------------------------------------------------------------------------
--1.1 ArrayExpress ELENI
select jdict('documentId', docid,	'entity', 'ArrayExpress','biomedicalId', regexpr(var('arrayexpress_prefixes'), middle), 'confidenceLevel', 0.8,'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid,prev,middle,next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('arrayexpress_prefixes')) from mydata)
        where regexprmatches("%{arrayexpress_negativePrefixes}",upper(middle)) = 0)
union all
--1.2. ebi_ac_uk ELENI
select jdict('documentId', docid,'entity', 'European Genome-phenome Archive', 'biomedicalId', regexpr("("||var('ebi_ac_uk_prefixes2')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ebi_ac_uk_prefixes'))  from mydata )
where regexprmatches("%{ebi_ac_uk_negativeWords}",upper(prev||middle)) = 0
union all
-- dbSNP: EL 06/2022 (I need feedback from Harry)!!!!
-- union all
-- 2.1 flowRepository EL 06/2022
select jdict('documentId', docid, 'entity', "flowrepository", 'biomedicalId', regexpr("("||var('flowrep_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
        from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('flowrep_prefixes'))
                from (select docid, text from mydata))
     )
union all
--2.5 DNA Databank of Japan
select jdict('documentId', docid,'entity', 'DNA databank of Japan', 'biomedicalId', regexpr("("||var('DNADatabankOfJapan_prefixes2')||")", middle) , 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('DNADatabankOfJapan_prefixes2'))
          from (select docid, text from mydata where regexprmatches(var('DNADatabankOfJapan_prefixes1'), lower(text)) = 1))
------------------------------------------------------------------------------------------------------------------------------------------
union all
--3.1 EBIMetagenomics EL 06/2022
select jdict('documentId', docid, 'entity', "EBIMetagenomics", 'biomedicalId', regexpr("("||var('EBIMetagenomics_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('EBIMetagenomics_prefixes'))
	              from mydata )
		 )
 union all
--3.2 ΕΒΙMetabolights EL 06/2022
select jdict('documentId', docid, 'entity', "EBIMetabolights", 'biomedicalId', regexpr("("||var('EBIMetabolights_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('EBIMetabolights_prefixes'))
	              from mydata )
		 )
 union all
--3.3 NCBIassembly EL 06/2022
select jdict('documentId', docid, 'entity', "NCBIassembly", 'biomedicalId', regexpr("("||var('NCBIassembly_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBIassembly_prefixes'))
	              from mydata )
		 )
union all
--3.4a NCBI PubChem BioAssay
select jdict('documentId', docid, 'entity', "NCBIPubChemBioassay", 'biomedicalId',  regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (  setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBIPubChemBioAssey'))
				from mydata )
union all
--3.4b NCBI PubChem Substance
select jdict('documentId', docid, 'entity', "NCBIPubChemSubstance", 'biomedicalId',  regexpr("(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (  setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBIPubChemSubstance'))
				from mydata )
union all
--3.5 NCBI Taxonomy EL 06/2022
select jdict('documentId', docid, 'entity', "NCBITaxonomy", 'biomedicalId', regexpr("id=?(\d+)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
	       from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCBITaxonomy_prefixes'))
	              from mydata )
        where    length(regexpr("id=?(\d+)", middle))>=3 or
                (length(regexpr("id=?(\d+)", middle))<3 and regexprmatches("(?:wwwtax\.cgi\D+\d+)",middle))
      )
union all
--3.6 NeuroMorpho EL 06/2022
select jdict('documentId', docid, 'entity', "NeuroMorpho", 'biomedicalId',regexpr("("||var('NeuroMorpho_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NeuroMorpho_prefixes'))
	     from mydata )
union all
--3.7 BioModels EL 06/2022
 select jdict('documentId', docid, 'entity', "BioModels", 'biomedicalId', regexpr("("||var('BioModels_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
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
union all
--3.8 Database of Interacting Proteins
select  jdict('documentId', docid,'entity', 'Database of Interacting Proteins', 'biomedicalId', regexpr("(\d{1,7}N)", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('DIP_prefixes2'))
          from (select docid, text from mydata where regexprmatches(var('DIP_prefixes1'), text) = 1))
union all
------------------------------------------------------------------------------------------------------------------------------------------
--4.1 FlyBase
select jdict('documentId', docid, 'entity', "FlyBase", 'biomedicalId', regexpr(var('FlyBase_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('FlyBase_prefixes'))
      from mydata)
union all
--4.2 Gene Expression Omnibus
select jdict('documentId', docid, 'entity', "Gene Expression Omnibus", 'biomedicalId', regexpr(var('GEO_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from
(select  docid, prev, middle, next
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('GEO_prefixes'))
          from mydata)
where ( regexprmatches('(?<!\w)GPL|(?<!\w)GSM|(?<!\w)GSE|(?<!\w)GDS',middle) = 1 and
regexprmatches('license|licence|lizenz|gnu|depression|cognitive|uv illumination|imaging system|image analysis system|frequency|frequencies|radiation|open source',lower(prev||' '||' '||middle||' '||next)) = 0
         and regexprmatches('\bgds1\b|\bgds2\b|\bgds3\b|\bgds4\b|\bgds5\b|\bgds6\b|\bgds8000\b|\bgsm1800\b|\bgsm900\b',lower(middle)) = 0)
or regexprmatches('ncbi\.nlm\.nih\.gov(\/projects)?\/geo',prev||middle||next) = 1)
union all
--4.3 IntAct
select jdict('documentId', docid, 'entity', "IntAct", 'biomedicalId', regexpr(var('intAct_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('intAct_prefixes'))
         from (select docid, text from mydata))
where regexprmatches('[a-zA-Z1-9]EBI-\d+',middle) = 0 and regexprmatches('NSF',prev||middle||next) = 0
union all
--4.4 openfMRI
select jdict('documentId', docid,'entity', 'OpenfMRI', 'biomedicalId', regexpr("("||var('openfMRI_prefixes3')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('openfMRI_prefixes3'))
          from (select docid, text from mydata where regexprmatches(var('openfMRI_prefixes1'), lower(text)) = 1))
union all
--4.5 OpenNeuro
select jdict('documentId', docid,'entity', 'OpenNeuro', 'biomedicalId', regexpr("("||var('OpenNeuro_prefixes3')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
 from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('OpenNeuro_prefixes3'))
          from (select docid, text from mydata where regexprmatches(var('OpenNeuro_prefixes1'), lower(text)) = 1))
union all
--4.6 Peptide Atlas
select jdict('documentId', docid,'entity', 'Peptide Atlas', 'biomedicalId', regexpr(var('peptideatlas_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('peptideatlas_prefixes'))
         from (select docid, text from mydata))
where regexprmatches('(?<!\w)PAp\d+',middle) = 1
union all
--4.7 PRIDE Archive
select jdict('documentId', docid,'entity', 'PRIDE Archive', 'biomedicalId', regexpr(var('prideArchive_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('prideArchive_prefixes'))
         from (select docid, text from mydata))
where regexprmatches('(?<!\w)PXD\d+',middle) = 1
-- ------------------------------------------------------------------------------------------------------------------------------------------
union all
--5.1 Clinical Trials
select jdict('documentId', docid,'entity', 'Clinical Trials', 'biomedicalId', regexpr("("||var('clinicaltrial_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( select docid, prev, middle, next
         from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('clinicaltrial_prefixes'))  from mydata)
        where regexprmatches('[A-Z1-9]NCT\d{6,}',middle) = 0 and length(regexpr("(NCT\d+)", middle))>=7
      )
union all
--5.2 XenBase
select jdict('documentId', docid,'entity', 'Xenbase', 'biomedicalId', regexpr("("||var('XenBase_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('XenBase_prefixes')) from mydata)
union all
--5.3 Mouse Genome Informatics
select jdict('documentId', docid,'entity', 'Mouse Genome Informatics', 'biomedicalId', regexpr("("||var('mousegenomeinformatics_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (select docid, prev, middle, next
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('mousegenomeinformatics_prefixes')) from mydata)
where regexprmatches('[A-Z1-9]MGI.\d',middle) = 0)
union all
--5.4 Australian Ocean Data Network
select jdict('documentId', docid,'entity', 'Australian Ocean Data Network (AODN)', 'biomedicalId', regexpr(var('AODN_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('AODN_prefixes')) from mydata)
union all
--5.5 British Oceanographic Data Centre (NERC Data Centres)
select jdict('documentId', docid,'entity', 'British Oceanographic Data Centre (BODC)', 'biomedicalId',biomedicalid, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from
(select docid,prev,middle,next, case when regexprmatches('documents|online_delivery', middle) then regexpr("www\.bodc\.ac\.uk\/data\/[^\/]+\/[^\/]+\/([^\s|\/|\.|)|(|A-Z]+)", middlenext)
else regexpr("www\.bodc\.ac\.uk\/data\/[^\/]+\/[^\/]+\/[^\/]+\/\s?\s?([^\s|\/|\.|)|(|A-Z]+)", middlenext) end as biomedicalid
from
(select docid,prev,middle,next, case when next is null then middle else middle||next end as middlenext
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('BODC_prefixes')) from mydata)
where regexprmatches("\.pdf", middle) = 0))
union all
--5.6 Centre for Environmental Data Analysis (NERC Data Centres)
select jdict('documentId', docid,'entity', 'Centre for Environmental Data Analysis (CEDA)', 'biomedicalId', regexpr(var('CEDA_prefixes'), middle),  'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text,""), 10, 1, 10, var('CEDA_prefixes')) from mydata) -- replace \n with ""
union all
--5.7 National Geoscience Data Centre (NERC Data Centres)
select jdict('documentId', docid,'entity', 'National Geoscience Data Centre (NGDC)', 'biomedicalId', regexpr(var('NGDC_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NGDC_prefixes')) from mydata)
union all
--5.8 UK Polar Data Centre (NERC Data Centres)
select jdict('documentId', docid,'entity', 'UK Polar Data Centre (NERC Data Centres)', 'biomedicalId', regexpr(var('BAS_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('BAS_prefixes')) from mydata)
union all
--5.9 NOAA National Centers for Environmental Information
select jdict('documentId', docid,'entity', 'NOAA National Centers for Environmental Information (NCEI)', 'biomedicalId', regexpr(var('NCEI_prefixes'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NCEI_prefixes')) from mydata)
union all
------------------------------------------------------------------------------------------------------------------------------------------
--6.1 Zebrafish
select jdict('documentId', docid,'entity', 'Zebrafish', 'biomedicalId', regexpr("("||var('zebrafish_prefixes')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('zebrafish_prefixes')) from mydata)
where regexprmatches('chinese',lower(prev||middle||next)) = 0
union all
--6.2 Virtual Skeleton
select jdict('documentId', docid,'entity', 'Virtual Skeleton Database', 'biomedicalId', regexpr("("||"\d+"||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('VirtualSkeleton_prefixes')) from  mydata)
union all
--6.3 VectorBase
select jdict('documentId', docid,'entity', 'VectorBase', 'biomedicalId', regexpr("("||"DS_\w{10}"||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('VectorBase_prefixes')) from  mydata)
union all
--6.4 Protein Circular Dirchroism Data Bank
select jdict('documentId', docid,'entity', 'Protein Circular Dirchoism Data Bank', 'biomedicalId', regexpr("("||var('PCDDB_prefixes2')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('PCDDB_prefixes2'))
         from (select docid, text from mydata where regexprmatches(var('PCDDB_prefixes1'), lower(text)) = 1 )
      )
where
regexprmatches("%{PCDDB_negativePrefixes}",upper(prev||' '||middle||' '||next)) = 0
union all
--6.5 National Database for Autism Research
select jdict('documentId', docid,'entity', 'National Database for Autism Research', 'biomedicalId', regexpr("("||var('NDAR_prefixes2')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NDAR_prefixes2'))
         from (select docid, text from mydata where regexprmatches(var('NDAR_prefixes1'), lower(text)) = 1 )
      )
where regexprmatches("[a-zA-Z1-9_-]NDARCOL",middle) = 0 and
      regexprmatches("NDARCOL[a-zA-Z]",middle) = 0
union all
--6.6 National Addiction & HIV Data Archive Program
select jdict('documentId', docid,'entity', 'National Addiction & HIV Data Archive Program', 'biomedicalId', regexpr("("||"\d+"||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('NAHDAP_prefixes2')) from mydata)
union all
--6.7 Archaeology Data Service
select jdict('documentId', docid,'entity', 'Archaeology Data Service', 'biomedicalId',  regexpr("("||var("ArchaeologyDataService_prefixes2")||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ArchaeologyDataService_prefixes')) from mydata)
where length(regexpr("("||var("ArchaeologyDataService_prefixes2")||")", middle))>=7
union all
--6.8 Australian Antarctic Data Centre
select jdict('documentId', docid,'entity', 'Australian Antarctic Data Centre', 'biomedicalId', regexpr("("||var('AustralianAntarcticDataCentre_prefixes2')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from ( setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('AustralianAntarcticDataCentre_prefixes')) from mydata)
-- union all
-- --6.9 caNanoLab --> I get 0 results
-- select jdict('documentId', docid,'entity', 'caNanoLab', 'biomedicalId', regexpr("("||"\d+"||")", middle) , 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
-- from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('caNanoLab_prefixes')) from mydata)
union all
--6.10 GenomeRNAi
select jdict('documentId', docid,'entity', 'Genome RNai', 'biomedicalId', regexpr(var('GenomeRNAi_prefixes2'), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('GenomeRNAi_prefixes2'))
         from (select docid, text from mydata where regexprmatches(var('GenomeRNAi_prefixes1'), lower(text)) = 1 )
      )
union all
--6.11 Japanese Genotype Phenotype Archive
select jdict('documentId', docid,'entity', 'Japanese Genotype Phenotype Archive', 'biomedicalId', regexpr("("||var('Japanese_prefixes2')||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('Japanese_prefixes2'))
         from (select docid, text from mydata where regexprmatches(var('Japanese_prefixes1'), lower(text)) = 1 )
      )
union all
--6.12 Environmental Information Data Centre
select jdict('documentId', docid,'entity', 'Environmental Information Data Centre', 'biomedicalId',  regexpr(var("EnvironmentalInformationDataCentre_prefixes"), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (
select docid,prev,middle,next, case when  regexpr("10\.5285\/([a-zA-Z0-9]{8}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{12})",middle) is not null
then regexpr("10\.5285\/([a-zA-Z0-9]{8}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{12})",middle)
else regexpr("10\.5285\/([a-zA-Z0-9]{8}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{4}-?[a-zA-Z0-9]{12})",middle||next) end as id
from  (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('EnvironmentalInformationDataCentre_prefixes')) from mydata)
where regexprmatches("o-bib", middle) = 0  and length(middle>20) and id is not null)
union all
--6.13 SIBMAD Astronomical Database
select jdict('documentId', docid,'entity', 'SIMBAD Astronomical Database', 'biomedicalId',  regexpr("("||var("Sibmad_prefixes2")||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('Sibmad_prefixes')) from mydata)
union all
--6.14 Influenza Reseach Database
select jdict('documentId', docid,'entity', 'Influenza Reseach Database', 'biomedicalId',  regexpr(var("Influenza_prefixes2"), middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
from (setschema 'docid,prev,middle,next' select  docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('Influenza_prefixes')) from mydata)
-- union all -- I get 0 results
-- --6.15 Kinetic Models of Biological Systems
-- select jdict('documentId', docid,'entity', 'Kinetic Models of Biological Systems', 'biomedicalId', regexpr("("||"\d+"||")", middle), 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1
-- from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('KiMoSys_prefixes')) from mydata)
---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
union all
--dbgap --Giannhs
select jdict('documentId', docid, 'entity', 'DBGAP', 'biomedicalId', match, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1 from
(select docid,jsplitv(regexprfindall("(ph\w{7}\.\w\d\.p\w)",middle)) as match, prev, middle, next from (setschema 'docid,prev,middle,next' select docid, textwindow2s(lower(regexpr("\n",text," ")), 10,1,5, "ph\w{7}\.\w\d\.p\w") from mydata) group by docid, match)
union all
--chembl -- Giannhs
select jdict('documentId', docid, 'entity', 'CHEMBL', 'biomedicalId', match, 'confidenceLevel', 0.8, 'textsnippet', (prev||" <<< "||middle||" >>> "||next)) as C1 from
(select docid, regexpr("(chembl\d{3,})", middle)  as match, prev, middle, next from (setschema 'docid,prev,middle,next' select docid, textwindow2s(lower(keywords(text)), 10,1,5, "chembl\d{3,}") from mydata) group by docid,match);


---------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------
-- --uniprot  Giannhs
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

-- create temp table remove_ids_ENA as
-- select * from  (file  header:t 'remove.tsv');


create temp table results_SRA_dbVar_ENA_EVA as
--1.3. SRA  ELENI
select docid as 'documentId',
       'SRA' as 'entity',
			 regexpr('(?:(?:\b|[^A-Z])(SR[A|P|X|R|S|Z][:|-|_|.]{0,1}\d+))', middle) as 'biomedicalId',
			 0.8 as 'confidenceLevel',
			 prev, middle, next
from ( setschema 'docid,prev,middle,next' select docid,textwindow2s(regexpr("\n",text," "), 10, 1, 10,'(?:(?:\b|[^A-Z])SR[A|P|X|R|S|Z][:|-|_|.]{0,1}\d{6})') from mydata )
where length(biomedicalId)<20
union all
--2.2.  dbVar: EL 06/2022
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
--2.3. ENA: EL 06/2022
select docid as 'documentId',
       'ENA' as 'entity',
			 regexpr("("||var('ena_prefixes')||")", middle) as 'biomedicalId',
			 0.8 as 'confidenceLevel',
			prev, middle, next
from ( select docid, prev, middle, next
				from (setschema 'docid,prev,middle,next' select docid, textwindow2s(regexpr("\n",text," "), 10, 1, 10, var('ena_prefixes'))
			 					from (select docid, text from mydata where regexprmatches('\bembl\b|\bebi\b|european bioinformatics institute|european nucleotide archive|sequence read archive|\bena\b|uniprot|rnacentral|ebi metagenomics|ensembl|genome|arrayexpress|ncbi|gene|genomic|genbank|chromosome', lower(text)) = 1  ))
      where regexprmatches(var('ena_NegativeMiddle'), middle) = 0 and
            regexprmatches(var('ena_prefixes_doi'), prev||" "||middle||" "||next) = 0 and
            regexprmatches(var('ena_NegativeWords'), lower(prev||" "||middle||" "||next)) = 0 and
            regexprmatches(var('ena_NegativeWordsForReferences'), lower(prev||" "||middle||" "||next)) = 0 and
            regexprmatches(var('ena_NegativeWordsForReferences2'), lower(prev||" "||middle||" "||next)) = 0 and
            regexprmatches(var('ena_NegativeWordsPrev'), lower(prev)) = 0
)
where middle is not null and middle not in (select * from remove_ids_ENA )
union all
--2.4. EVA EL 06/2022
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


--5.10 DGVa etc
select jdict('documentId', documentId, 'entity', entity, 'biomedicalId', biomedicalId, 'confidenceLevel', 0.8, 'textsnippet',  (prev||" <<< "||middle||" >>> "||next)) as C1
from (
        select documentId, biomedicalId, prev, middle, next, entity
          from resultsduplicates
          where ( --if accession ID is embedded within a URL	choose the repository that owns the URL
                    regexprmatches("www\.[a-z|\/|\.|0-9]+", lower(prev||" "||middle||" "||next)) = 1
                    and regexprmatches(lower(entity), regexpr("(www\.[a-z|\/|\.|0-9]+)", lower(prev||" "||middle||" "||next))) = 1 )
                or --choose repository based on accession ID	SRP/SRX/SRR/SRS --> SRA
                (regexprmatches("SR[P|X|R|S]", upper(middle)) = 1 and entity = 'SRA')
                or --choose repository based on accession ID	 ERP/ERX/ERR --> ENA
                (regexprmatches("ER[P|X|R]", upper(middle)) = 1 and entity = 'ENA')
                or --PRJEB accessions should be assigned to ENA
                (regexprmatches("PRJEB", upper(middle)) = 1 and entity = 'ENA')
                or
                (regexprmatches("dbvar", lower(prev||middle||next)) = 1 and entity = 'dbVar')
                or
                (regexprmatches("dgva", lower(prev||middle||next)) = 1 and entity = 'DGVa')
);
