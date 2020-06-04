CREATE OR REPLACE FUNCTION function_{0}()
RETURNS TRIGGER AS $$
DECLARE
	partition_date TEXT;
	partition_name TEXT;
	start_of_month TEXT;
	end_of_next_month TEXT;
	{1} TIMESTAMP;
BEGIN
date_update_transaction := cast(coalesce(new.updated_time, new.transaction_date) as timestamp);
partition_date := to_char({1},'YYYY_MM');
partition_name := '{2}_' || partition_date;
start_of_month := to_char(({1}),'YYYY_MM') || '-01';
end_of_next_month := to_char(({1} + interval '1 month'),'YYYY_MM') || '-01';
IF NOT EXISTS
	(SELECT 1
   	 FROM   information_schema.tables 
   	 WHERE  table_name = partition_name) 
THEN
	RAISE NOTICE 'A partition has been created %', partition_name;
	EXECUTE format(E'CREATE TABLE %I (CHECK ( date_trunc(\'day\', {1}) >= ''%s'' AND date_trunc(\'day\', {1}) < ''%s'')) INHERITS (public.{6})', partition_name, start_of_month,end_of_next_month);
	-- EXECUTE format('GRANT SELECT ON TABLE %I TO readonly', partition_name); -- use this if you use role based permission
END IF;
EXECUTE format('INSERT INTO %I ({3}, {1})
VALUES ({7})',
partition_name) using 
{4}, NEW.{1}
;
RETURN NULL;
exception when others
then
EXECUTE format('INSERT INTO {2}_inconsistent_data ({3})
VALUES ({5})',
partition_name) using
{4}
;
RETURN NULL;
END
$$
LANGUAGE plpgsql;
