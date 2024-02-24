


## Inspect page density and TOAST usage
Shows how many rows fit on pages and how TOAST is used.

The related tunings are:
* configure `toast_tuple_target`
* configure ```STORAGE PLAIN/MAIN/EXTENDED/EXTERNAL```
* order of columns in the table

```sql
SELECT
  pgsut.schemaname as schema_name,
  pgsut.relname as table_name,
  pg_size_pretty(pg_relation_size(pg_class.oid)) main_size,
  pg_size_pretty(pg_relation_size(reltoastrelid)) toast_size,
  n_live_tup,
  pg_relation_size(pg_class.oid)/(n_live_tup+n_dead_tup) main_avg_size,
  pg_relation_size(reltoastrelid)/(n_live_tup+n_dead_tup) toast_avg_size,
  (select option_value::int FROM pg_options_to_table(reloptions) WHERE option_name='toast_tuple_target') as toast_tuple_target
FROM pg_class
INNER JOIN pg_namespace ns on relnamespace = ns.oid   
INNER JOIN pg_stat_user_tables pgsut ON pgsut.relid=pg_class.oid
WHERE (n_live_tup+n_dead_tup)>0
AND nspname='banking_stage_results_2'
ORDER BY 1,2
```