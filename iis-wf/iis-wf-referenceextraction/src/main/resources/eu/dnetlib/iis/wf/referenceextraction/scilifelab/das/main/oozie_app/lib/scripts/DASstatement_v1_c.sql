
create temp table ukrn as select * from (setschema 'doi,text' select jsonpath(c1,'$.id', '$.text') from stdinput());

hidden var 'UKRN4prefixes1' from '\bdata\b|\bcode\b|\bmaterial\b|\bsoftware\b|\bstatement\b|\bartifact\b';
hidden var 'full_and_good' from 'freely available|public domain|data are open source|publicly available|accessible through the link|(available (?:at|via|through|to))|((?:have been|are|was|were) deposited)|available without restriction|(can be found(?:,? in the)? online)|can be downloaded|(access(?:ed|ible) (f?from|at))|available online from';
hidden var 'supplementary' from '(in appendi(?:x|ces))|study are included|in supplement(al (mat|info)|ary (mat|file))|within the (article|manuscript|paper)|are included in (the (article|paper)|this( published article)?)|supporting info|provided within|contained within|appear in the submitted article|all relevant data are within|data are included';
hidden var 'on_request' from 'made available by the authors|((?:upon|by|on|reasonable) request)|available from the corresponding author';
hidden var 'no_immediate_access' from '(third part(?:y|ies))|confidential|will be available following approval|are ?n.t shared publicly|cannot be shared publicly|do ?n.t have permission|personal information|sensitive info|sensitive nature|written consent|patient consent|data protection|protect anonym|protected data|cannot\sbe shared|after securing relevant permissions';
hidden var 'no_data' from'no (?:data|dataset|datasets|data ?sets) (?:was|were)? (?:used|produced|generated|collected|created|available)|not\sapplicable|no new data|data availability (?:statement)?[:\.]?\s*n\/a';


create temp table myDAStexts as
select doi, middle, next, middle||" "||regexpr("\..+$",next,"") as textsnippet, middle||" "|| next as totaltext
from (setschema 'doi,prev,middle,next'
      select doi, textwindow2s(regexpr("\n",lower(text)," "), 0, 1, 20, var('UKRN4prefixes1'))
      from (select * from ukrn))
where (regexprmatches("%{full_and_good}",middle||next) = 1 or
       regexprmatches("%{supplementary}",middle||next) = 1 or
       regexprmatches("%{on_request}",middle||next) = 1 or
       regexprmatches("%{no_immediate_access}",middle||next) = 1 or
       regexprmatches("%{no_data}",middle||next) = 1)
       and length(middle)-length(regexpr("("||var('UKRN4prefixes1')||")",middle)) < 3;



 select jdict('documentId', doi, 'prev', prev, 'middlenext', middlenext, 'DAS_statements', statement) as C1
 from (
          select doi, prev, middlenext, statement
          from (
                    select doi, prev, middlenext, regexpr("((?ims)(?<!\w)data availability.+?(?=key points|^\s*\d+\.?\s*\n\s*\d+\.?\s*$|\(?\d+ of \d+\)?|ethics this|^\s*statistics\s*$|ethics approval|ethical animal research|^appendix\s?\w?.?$|^\s*references\s*$|^\s*results\s*$|authors?'? contribution|credit authorship contribution statement|declarations|declaration of competing interest|^\s*supporting information\s*$|funding|received:|acknowledge?ments?|keywords|\s*\xA9\s*202\d|(?<=\.)\s*\n(?=^[a-z]{3,}(?:\s+[a-z]{3,}){0,2}\s*$)|(?:\n[a-z]{3,}\n)?\nreferences\s*\n?.*))",middlenext) as statement
                    from (  select doi, prev, middle, next, middle||' '||next as middlenext
                              from (setschema 'doi,prev,middle,next'
                                    select doi, textwindow2s(lower(text), 10, 2, 200, "data avail.bility")
                                    from (select * from ukrn where doi in (select distinct doi from myDAStexts))
                                    ))
                    where  length(statement) > 5 --delete null
                )
);
