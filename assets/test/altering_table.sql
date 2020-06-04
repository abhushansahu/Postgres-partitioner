alter table test_table rename to test_table_with_data;
create table test_table (like test_table_with_data);
insert into test_table
select * from test_table_with_data where id = (select max(id) from test_table_with_data);
ALTER TABLE public.test_table ALTER COLUMN id SET DEFAULT nextval('test_table_id_seq'::regclass);
select * from test_table;