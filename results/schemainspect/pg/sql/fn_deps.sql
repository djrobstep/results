-- Dependencies where a table/view column default (or generated expression)
-- references a user-defined function.
--
-- pg_attrdef rows record column defaults; pg_depend links them to the
-- functions they call.  We surface these as "table depends on function" edges
-- so that the diff engine knows to create the function before the table and
-- drop the table's default (or the table itself) before dropping the function.

with extension_objids as (
  select objid as extension_objid
  from pg_depend
  where refclassid = 'pg_extension'::regclass
),
user_functions as (
  select
    p.oid,
    n.nspname as schema,
    p.proname as name,
    pg_get_function_identity_arguments(p.oid) as identity_arguments
  from pg_proc p
  join pg_namespace n on n.oid = p.pronamespace
  where
    n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
    and n.nspname not like 'pg_temp_%'
    and p.oid not in (select extension_objid from extension_objids)
),
user_relations as (
  select
    c.oid,
    n.nspname as schema,
    c.relname as name,
    c.relkind as kind
  from pg_class c
  join pg_namespace n on n.oid = c.relnamespace
  where
    c.relkind in ('r', 'v', 'm', 'p')
    and n.nspname not in ('pg_catalog', 'information_schema', 'pg_toast')
    and n.nspname not like 'pg_temp_%'
    and c.oid not in (select extension_objid from extension_objids)
)
select distinct
  r.schema        as schema,
  r.name          as name,
  null::text      as identity_arguments,
  r.kind          as kind,
  f.schema        as schema_dependent_on,
  f.name          as name_dependent_on,
  f.identity_arguments as identity_arguments_dependent_on,
  'f'             as kind_dependent_on
from pg_attrdef ad
join pg_class c   on c.oid = ad.adrelid
join user_relations r on r.oid = c.oid
join pg_depend d  on d.objid = ad.oid
                 and d.classid = 'pg_attrdef'::regclass
                 and d.refclassid = 'pg_proc'::regclass
join user_functions f on f.oid = d.refobjid
order by schema, name, schema_dependent_on, name_dependent_on, identity_arguments_dependent_on
