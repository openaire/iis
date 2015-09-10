SELECT  p.id                         as projectid,
        p.acronym                    as acronym,
        p.code                       as code,
        array_agg(fp.path)           as fundingpath

FROM projects p
     left outer join project_fundingpath pf on (pf.project = p.id)
     left outer join fundingpaths fp on (fp.id = pf.funding)

GROUP BY p.id