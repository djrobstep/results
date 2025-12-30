select
  collname as name,
  n.nspname as schema,
  case collprovider
    when 'd' then 'database default'
    when 'i' then 'icu'
    when 'c' then 'libc'
  end
  as provider,
  collencoding as encoding,
  collcollate as lc_collate,
  collctype as lc_ctype,
  collversion as version,
  -- 17_AND_LATER colllocale as locale
  -- 15_AND_16 colliculocale as locale
  -- 14_AND_EARLIER collcollate as locale
from
pg_collation c
INNER JOIN pg_namespace n
    ON n.oid=c.collnamespace
    -- SKIP_INTERNAL where nspname not in ('pg_internal', 'pg_catalog', 'information_schema', 'pg_toast')
    -- SKIP_INTERNAL and nspname not like 'pg_temp_%' and nspname not like 'pg_toast_temp_%'
order by 2, 1
