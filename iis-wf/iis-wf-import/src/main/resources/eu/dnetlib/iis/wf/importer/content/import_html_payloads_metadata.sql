select pid, pid_type, dedupid, id, archive_s3_location, html_filename, html_hash, html_size
from (
  select pub.pid, pub.pid_type, pub.dedupid, p.id, p.original_url,
    SUBSTR(p.`location`, 1, INSTR(p.`location`, '.tar.gz') + LENGTH('.tar.gz') - 1) AS archive_s3_location,
    SUBSTR(p.`location`, INSTR(p.`location`, '.tar.gz/') + LENGTH('.tar.gz/')) AS html_filename,
    p.`hash` as html_hash, p.`size` as html_size
    , row_number() over (partition by p.id, p.original_url, p.`location` order by
      (case when pub.pid_type = 'doi' then 2 when pub.pid_type is not null then 1 else 0 end) desc
    ) as rn
  from ${databaseName}.html_payload p
  left outer join ${databaseName}.publication pub
    on pub.id = p.id and pub.url = p.original_url
) subq
where subq.rn = 1
