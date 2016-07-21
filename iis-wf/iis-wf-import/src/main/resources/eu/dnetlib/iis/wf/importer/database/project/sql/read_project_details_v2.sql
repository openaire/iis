SELECT  p.id                         as projectid,
        p.acronym                    as acronym,
        p.code                       as code,
        p.optional1                  as optional1,
        p.optional2                  as optional2,
        array_agg(fp.path)           as fundingpath

FROM projects p
     left outer join project_fundingpath pf on (pf.project = p.id)
     left outer join fundingpaths fp on (fp.id = pf.funding)

GROUP BY p.id