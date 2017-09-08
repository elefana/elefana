CREATE TABLE IF NOT EXISTS es2pgsql_index_mapping_tracking (
	table_name VARCHAR(255) PRIMARY KEY,
	remaining_time BIGINT,
	last_check_time BIGINT
);

CREATE OR REPLACE FUNCTION generate_index_mapping() RETURNS trigger AS  
$$  
BEGIN  
         INSERT INTO emp_log(emp_id,salary,edittime)  
         VALUES(NEW.employee_id,NEW.salary,current_date);  
   
    RETURN NEW;  
END;
$$  
LANGUAGE 'plpgsql';  