drop table if exists {0};
create table {0} (like {1});
alter table {0} add column {2} timestamp;
CREATE INDEX if not exists idx_{0}_{2} ON {0} ({2});