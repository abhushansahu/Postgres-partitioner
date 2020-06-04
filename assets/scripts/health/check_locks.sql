select pid,
       usename,
       pg_blocking_pids(pid) as blocked_by,
       query as blocked_query
from pg_stat_activity
where cardinality(pg_blocking_pids(pid)) > 0;



SELECT * FROM pg_stat_activity;

select t.relname,l.locktype,page,virtualtransaction,pid,mode,granted
from pg_locks l, pg_stat_all_tables t where l.relation=t.relid order by relation asc;

select * from pg_locks;
