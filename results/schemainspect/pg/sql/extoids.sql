with RECURSIVE tree(objid, refobjid, classid, refclassid, is_cycle, path) AS (
   SELECT
      d.objid,
      d.refobjid,
      d.classid::regclass,
      d.refclassid::regclass,
      false,
      ARRAY[d.refobjid]
   FROM
      pg_depend d where d.refclassid = 'pg_extension'::regclass
      --and
      --d.classid = 'pg_class'::regclass
      and d.deptype in ('e')
   UNION all
   SELECT
      d.objid,
      d.refobjid,
      d.classid::regclass,
      d.refclassid::regclass,
      d.objid = ANY(path),
      path || d.refobjid
   FROM
      pg_depend d
    JOIN tree
        on d.refobjid = tree.objid
        and not is_cycle
        and d.deptype in ('i')

  )
  SELECT * from tree