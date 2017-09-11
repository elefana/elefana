CREATE OR REPLACE FUNCTION es2pgsql_schedule_index_mapping() RETURNS trigger AS  
$$  
BEGIN  
	INSERT INTO es2pgsql_index_mapping_tracking(_table_name, _last_insert_time, _last_mapping_time)  
         VALUES (TG_TABLE_NAME, (extract(epoch from now()) * 1000), 0) ON CONFLICT (_table_name) DO UPDATE SET _last_insert_time = EXCLUDED._last_insert_time;  
   
    RETURN NULL;
END;
$$  
LANGUAGE 'plpgsql';  