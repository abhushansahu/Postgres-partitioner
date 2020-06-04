insert into {1}_inconsistent_data
select * from {2} where {4} between {0} and {5}
and coalesce({3}) !~ '^[\d]{4}-[0-1][1-9]-[0-3][0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9]$';
-- delete from {2} where {4} = any(array(select {4} from {1}_inconsistent_data)); Removing as delete doesn't happen at every loop