WITH RECURSIVE fundingtree(id, parent_id, payload, depth)
AS (
  SELECT  f.id,
           ff.funding1,
           '{"funding_level_0":{"class":"' || f.semanticclass || '", "id":"' || f.id || '", "description":"' || f.description || '", "name":"' || f.name || '", "parent":{}}}',
           1
           FROM fundings f LEFT OUTER JOIN funding_funding ff ON f.id = ff.funding1
           WHERE ff.funding2 IS NULL
   UNION
   SELECT  f1.id,
           ff.funding2,
           '{"funding_level_'||depth||'":{"class":"' || f1.semanticclass || '", "id":"' || f1.id || '", "description":"' || f1.description || '", "name":"' || f1.name || '", "parent":'|| ft.payload ||'}}',
           ft.depth + 1
           FROM funding_funding ff, fundingtree ft, fundings f2, fundings f1
           WHERE ft.id = ff.funding2 AND f2.id = ft.id AND ff.funding1 = f1.id AND depth <= 10)

SELECT  p.id                         as projectid,
        p.acronym                    as acronym,
        p.code                       as code,
        array_agg(ft.payload)        as fundingtree

FROM projects p
     left outer join project_funding pf on (pf.project = p.id)
     left outer join fundingtree ft on (ft.id = pf.funding)

GROUP BY p.id