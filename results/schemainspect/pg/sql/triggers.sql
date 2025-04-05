with extension_oids as (
  -- EXTOIDS
)
select
    tg.tgname "name",
    nsp.nspname "schema",
    cls.relname table_name,
    pg_get_triggerdef(tg.oid) full_definition,
    proc.proname proc_name,
    nspp.nspname proc_schema,
    tg.tgenabled enabled,
    tg.oid in (select e.objid from extension_oids e) as extension_owned,
    tg.oid as oid
from pg_trigger tg
join pg_class cls on cls.oid = tg.tgrelid
join pg_namespace nsp on nsp.oid = cls.relnamespace
join pg_proc proc on proc.oid = tg.tgfoid
join pg_namespace nspp on nspp.oid = proc.pronamespace
where not tg.tgisinternal
-- SKIP_INTERNAL and not tg.oid in (select e.objid from extension_oids e)
order by schema, table_name, name;
