insert into {1} ({6}, {7} {4})
select {6},
{7}
to_timestamp(coalesce({5}), 'YYYY-MM-DD HH24:MI:SS')
{4}
from {2}
where {6} between {0} and {8}-1;
|
insert into {3}_2020_01 select * from {1} where {4} between '2020-01-01 00:00:00' and '2020-01-31 23:59:59' ;
-- delete from {1} where {4} between '2020-01-01 00:00:00' and '2020-01-31 23:59:59' ;
insert into {3}_2019_12 select * from {1} where {4} between '2019-12-01 00:00:00' and '2019-12-31 23:59:59' ;
-- delete from {1} where {4} between '2019-12-01 00:00:00' and '2019-12-31 23:59:59' ;
insert into {3}_2019_11 select * from {1} where {4} between '2019-11-01 00:00:00' and '2019-11-30 23:59:59' ;
-- delete from {1} where {4} between '2019-11-01 00:00:00' and '2019-11-30 23:59:59' ;
insert into {3}_2019_10 select * from {1} where {4} between '2019-10-01 00:00:00' and '2019-10-31 23:59:59' ;
-- delete from {1} where {4} between '2019-10-01 00:00:00' and '2019-10-31 23:59:59' ;
insert into {3}_2019_09 select * from {1} where {4} between '2019-09-01 00:00:00' and '2019-09-30 23:59:59' ;
-- delete from {1} where {4} between '2019-09-01 00:00:00' and '2019-09-30 23:59:59' ;
insert into {3}_2019_08 select * from {1} where {4} between '2019-08-01 00:00:00' and '2019-08-31 23:59:59' ;
-- delete from {1} where {4} between '2019-08-01 00:00:00' and '2019-08-31 23:59:59' ;
insert into {3}_2019_07 select * from {1} where {4} between '2019-07-01 00:00:00' and '2019-07-31 23:59:59' ;
-- delete from {1} where {4} between '2019-07-01 00:00:00' and '2019-07-31 23:59:59' ;
insert into {3}_2019_06 select * from {1} where {4} between '2019-06-01 00:00:00' and '2019-06-30 23:59:59' ;
-- delete from {1} where {4} between '2019-06-01 00:00:00' and '2019-06-30 23:59:59' ;
insert into {3}_2019_05 select * from {1} where {4} between '2019-05-01 00:00:00' and '2019-05-31 23:59:59' ;
-- delete from {1} where {4} between '2019-05-01 00:00:00' and '2019-05-31 23:59:59' ;
insert into {3}_2019_04 select * from {1} where {4} between '2019-04-01 00:00:00' and '2019-04-30 23:59:59' ;
-- delete from {1} where {4} between '2019-04-01 00:00:00' and '2019-04-30 23:59:59' ;
insert into {3}_2019_03 select * from {1} where {4} between '2019-03-01 00:00:00' and '2019-03-30 23:59:59' ;
-- delete from {1} where {4} between '2019-03-01 00:00:00' and '2019-03-31 23:59:59' ;
insert into {3}_2019_02 select * from {1} where {4} between '2019-02-01 00:00:00' and '2019-02-28 23:59:59' ;
-- delete from {1} where {4} between '2019-02-01 00:00:00' and '2019-02-28 23:59:59' ;
insert into {3}_2019_01 select * from {1} where {4} between '2019-01-01 00:00:00' and '2019-01-31 23:59:59' ;
-- delete from {1} where {4} between '2019-01-01 00:00:00' and '2019-01-31 23:59:59' ;
insert into {3}_2018 select * from {1} where {4} between '2018-01-01 00:00:00' and '2018-12-31 23:59:59' ;
-- delete from {1} where {4} between '2018-01-01 00:00:00' and '2019-12-31 23:59:59' ;
insert into {3}_2017 select * from {1} where {4} between '2017-01-01 00:00:00' and '2019-12-31 23:59:59' ;
-- delete from {1} where {4} between '2017-01-01 00:00:00' and '2017-12-31 23:59:59' ;
insert into {3}_2017_before select * from {1} where {4} < '2017-01-01 00:00:00' ;
-- delete from {1} where {4} < '2017-01-01 00:00:00';
delete from {1};
