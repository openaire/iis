drop table if exists grants;

create temp table jsoninp as select * from stdinput();
update jsoninp set c1=regexpr('"jsonextrainfo":"{}"',c1,'"jsonextrainfo":"{\"dossiernr\":\"\",\"NWOgebied\":\"\"}"');
create table grants as select acronym,normalizedacro,case when fundingclass1="FCT" then acronym else grantid end as grantid,fundingclass1,fundingclass2,id,c1 as nwo_opt2,c2 as nwo_opt1,case when c3='' then '_^' else c3 end as nih_orgname,c4 as nih_activity,c5 as nih_administeringic,case when c6='' then regexpr('0*(\d+)$', c7) else c6 end as nih_serialnumber,c7 as nih_coreprojectnum from (setschema 'acronym,normalizedacro,grantid,fundingclass1,fundingclass2,id,c1,c2,c3,c4,c5,c6,c7' select case when c1 is null then "UNKNOWN" else c1 end as acronym, case when c1 is not null then regexpr("[_\s]",normalizetext(lower(c1)),"[_\s]") else "unknown" end as normalizedacro, c3 as grantid,strsplit(c4,"delimiter:::") as fundingClass,c2 as id, jsonpath(c5,'$.NWOgebied', '$.dossiernr', '$.orgname', '$.activity', '$.administeringic', '$.serialnumber', '$.coreprojectnum') from (select * from (setschema 'c1,c2,c3,c4,c5' select jsonpath(c1, '$.projectAcronym', '$.id' , '$.projectGrantId','$.fundingClass','$.jsonextrainfo') from jsoninp) where regexprmatches("::",c4))) where fundingclass1!='NIH' OR (nih_coreprojectnum!='' AND nih_activity!='' AND nih_administeringic!='' AND nih_serialnumber!='0');




CREATE INDEX grants_index on grants (grantid,normalizedacro,acronym,fundingClass1,fundingClass2,id,nwo_opt2);
create index grants2_index on grants(nwo_opt1,acronym,fundingClass1,fundingClass2,id,nwo_opt2);
create index grants3_index on grants(nih_serialnumber,acronym,fundingClass1,id,nih_activity,nih_administeringic,nih_coreprojectnum,nih_orgname);


DROP TABLE IF EXISTS nihposnamesshort;
CREATE TABLE nihposnamesshort(word);
INSERT INTO nihposnamesshort VALUES
('(?:\b|_)NIH(?:\b|_|\d)'), ('(?:\b|_)NCI(?:\b|_|\d)'), ('(?:\b|_)NEI(?:\b|_|\d)'), ('(?:\b|_)NHLBI(?:\b|_|\d)'), ('(?:\b|_)NHGRI(?:\b|_|\d)'), ('(?:\b|_)NIA(?:\b|_|\d)'), ('(?:\b|_)NIAAA(?:\b|_|\d)'), ('(?:\b|_)NIAID(?:\b|_|\d)'), ('(?:\b|_)NIAMS(?:\b|_|\d)'), ('(?:\b|_)NIBIB(?:\b|_|\d)'), ('(?:\b|_)NICHD(?:\b|_|\d)'), ('(?:\b|_)NIDCD(?:\b|_|\d)'), ('(?:\b|_)NIDCR(?:\b|_|\d)'), ('(?:\b|_)NIDDK(?:\b|_|\d)'), ('(?:\b|_)NIDA(?:\b|_|\d)'), ('(?:\b|_)NIEHS(?:\b|_|\d)'), ('(?:\b|_)NIGMS(?:\b|_|\d)'), ('(?:\b|_)NIMH(?:\b|_|\d)'), ('(?:\b|_)NIMHD(?:\b|_|\d)'), ('(?:\b|_)NINDS(?:\b|_|\d)'), ('(?:\b|_)NINR(?:\b|_|\d)'), ('(?:\b|_)NLM(?:\b|_|\d)'), ('(?:\b|_)CIT(?:\b|_|\d)'), ('(?:\b|_)CSR(?:\b|_|\d)'), ('(?:\b|_)FIC(?:\b|_|\d)'), ('(?:\b|_)NCATS(?:\b|_|\d)'), ('(?:\b|_)NCCIH(?:\b|_|\d)');

DROP TABLE IF EXISTS nihposnamesfull;
CREATE TABLE nihposnamesfull(word);
INSERT INTO nihposnamesfull VALUES
('(?:\b|_)National Institutes of Health(?:\b|_)'), ('(?:\b|_)National Cancer Institute(?:\b|_)'), ('(?:\b|_)National Heart, Lung and Blood Institute(?:\b|_)'), ('(?:\b|_)National Human Genome Research Institute(?:\b|_)'), ('(?:\b|_)National Institute on Aging(?:\b|_)'), ('(?:\b|_)National Institute on Alcohol Abuse and Alcoholism(?:\b|_)'), ('(?:\b|_)National Institute of Allergy and Infectious Diseases(?:\b|_)'), ('(?:\b|_)National Institute of Arthritis and Musculoskeletal and Skin(?:\b|_)'), ('(?:\b|_)National Institute of Biomedical Imaging and Bioengineering(?:\b|_)'), ('(?:\b|_)National Institute of Child Health and Human Development(?:\b|_)'), ('(?:\b|_)National Institute on Deafness and Other Communication Disorders(?:\b|_)'), ('(?:\b|_)National Institute of Dental and Craniofacial Research(?:\b|_)'), ('(?:\b|_)National Institute of Diabetes and Digestive and Kidney(?:\b|_)'), ('(?:\b|_)National Institute on Drug Abuse(?:\b|_)'), ('(?:\b|_)National Institute of Environmental Health Sciences(?:\b|_)'), ('(?:\b|_)National Institute of General Medical Sciences(?:\b|_)'), ('(?:\b|_)National Institute on Minority Health and Health Disparities(?:\b|_)'), ('(?:\b|_)National Institute of Neurological Disorders and Stroke(?:\b|_)'), ('(?:\b|_)National Institute of Nursing Research(?:\b|_)'), ('(?:\b|_)National Library of Medicine(?:\b|_)'), ('(?:\b|_)Center for Information Technology(?:\b|_)'), ('(?:\b|_)Center for Scientific Review(?:\b|_)'), ('(?:\b|_)Fogarty International Center(?:\b|_)'), ('(?:\b|_)National Center for Advancing Translational Sciences(?:\b|_)'), ('(?:\b|_)National Center for Complementary and Integrative Health(?:\b|_)'), ('(?:\b|_)Clinical Center(?:\b|_)'), ('(?:\b|_)Dietary Supplements(?:\b|_)'), ('(?:\b|_)National Eye Institute(?:\b|_)'), ('(?:\b|_)The National Institute of Mental Health(?:\b|_)');

DROP table if exists nihpositives;
CREATE TABLE nihpositives(word);
INSERT INTO nihpositives VALUES
('\bnumber\b'), ('\bresearch\b'), ('\baward\b'), ('\b[Gg]rant[s]?\b'), ('\bHealth\b'), ('\b[Aa]ward\b'), ('\b(?:[Ss]upported|[Ff]unded) (?:by|from)\b');

DROP table if exists nihnegatives;
CREATE TABLE nihnegatives(word);
INSERT INTO nihnegatives VALUES
('\b1000 Genomes Project\b'), ('\bSwedish\b'), ('\bFinland\b'), ('\bFinnish\b'), ('\bSpain\b'), ('\bSpanish\b'), ('\bUK\b'), ('\b[GTACU]{8,}\b'), ('\b[Ee]. coli\b'), ('\b[Pp]lasmids\b'), ('\b[Cc]orporis\b'), ('https?://www\.'), ('\b[Pp]rotein\b'), ('\b[Ww]ellcome [Tt]rust\b'), ('\bde la [Rr]echerche\b'), ('\bLos Angeles\b'), ('\bFrance\b'), ('\bANR\b'), ('\bFAPESP\b'), ('\bCIRM\b'), ('\bAust(?:ralian?)?\b');