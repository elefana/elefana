CREATE SEQUENCE IF NOT EXISTS elefana_bulk_ingest_table_id MINVALUE -9223372036854775807 START -9223372036854775807 CYCLE;
CREATE SEQUENCE IF NOT EXISTS elefana_bulk_index_queue_id MINVALUE -9223372036854775807 START -9223372036854775807 CYCLE;

CREATE TABLE IF NOT EXISTS elefana_bulk_tables (_index VARCHAR(255), _ingestTableName VARCHAR(255) UNIQUE);
CREATE TABLE IF NOT EXISTS elefana_bulk_worker_index_queue (_queue_id VARCHAR(255) PRIMARY KEY, _targetTableName VARCHAR(255), _workerTableName VARCHAR(255), _timestamp BIGINT, _workerHost VARCHAR(255), _workerPort INT, _shardOffset INT);
CREATE TABLE IF NOT EXISTS elefana_file_deletion_queue (_filepath VARCHAR(255), _timestamp BIGINT);
CREATE TABLE IF NOT EXISTS elefana_delayed_table_index_queue (_tableName VARCHAR(255), _timestamp BIGINT, _generationMode VARCHAR(255));
CREATE TABLE IF NOT EXISTS elefana_delayed_field_index_queue (_tableName VARCHAR(255), _fieldName VARCHAR(255), _timestamp BIGINT, _generationMode VARCHAR(255));
 
CREATE OR REPLACE FUNCTION select_shard(_distributedTable VARCHAR, _offset INT) RETURNS bigint AS $$
DECLARE
  shard_id bigint;
  num_small_shards int;
BEGIN
  SELECT results.shardid, results.total INTO shard_id, num_small_shards
  FROM (SELECT shardid, count(*) OVER () AS total FROM pg_dist_shard JOIN pg_dist_placement USING (shardid)
  WHERE logicalrelid = _distributedTable::regclass
  GROUP BY shardid ORDER BY shardid ASC OFFSET _offset) AS results;

  WHILE num_small_shards IS NULL OR num_small_shards < _offset LOOP
   SELECT master_create_empty_shard(_distributedTable) INTO shard_id;

   SELECT results.shardid, results.total INTO shard_id, num_small_shards
    FROM (SELECT shardid, count(*) OVER () AS total FROM pg_dist_shard JOIN pg_dist_placement USING (shardid)
    WHERE logicalrelid = _distributedTable::regclass
    GROUP BY shardid ORDER BY shardid ASC OFFSET _offset) AS results;
  END LOOP;

  RETURN shard_id;
END;
$$ LANGUAGE plpgsql;

CREATE OR REPLACE FUNCTION elefana_delete_tmp_file(_filepath text) RETURNS INT AS $$
DECLARE
	_command text;
BEGIN
	_command := CONCAT('COPY (SELECT 1) TO PROGRAM ', '''rm -f ', _filepath, '''');
	EXECUTE _command;
	RETURN 0;
END;
$$
LANGUAGE 'plpgsql';

CREATE OR REPLACE FUNCTION elefana_json_field(_json_column jsonb, _json_field text) RETURNS text AS $$
	select _json_column->>_json_field
$$
LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION elefana_json_field_nat(_json_column jsonb, _json_field text) RETURNS jsonb AS $$
select _json_column->_json_field
			 $$
			 LANGUAGE SQL IMMUTABLE PARALLEL SAFE;

CREATE OR REPLACE FUNCTION elefana_next_bulk_ingest_table() RETURNS text AS
$$
DECLARE
	table_name text;
BEGIN
	SELECT CONCAT('elefana_bulk_stg_', nextval('elefana_bulk_ingest_table_id')) INTO table_name;
	RETURN table_name;
END;
$$
LANGUAGE 'plpgsql';

-- Workaround for PSQL not supporting ON CONFLICT on partition tables
CREATE OR REPLACE FUNCTION elefana_create(_op_index VARCHAR, _op_type VARCHAR, _op_id VARCHAR, _op_timestamp BIGINT, _op_bucket1s BIGINT, _op_bucket1m BIGINT, _op_bucket1h BIGINT, _op_bucket1d BIGINT, _op_source json) RETURNS INT AS
$$
BEGIN
	BEGIN
		INSERT INTO elefana_data(_index, _type, _id, _timestamp, _bucket1s, _bucket1m, _bucket1h, _bucket1d, _source) VALUES (_op_index, _op_type, _op_id, _op_timestamp, _op_bucket1s, _op_bucket1m, _op_bucket1h, _op_bucket1d, _op_source);
		RETURN 1;
	EXCEPTION WHEN unique_violation THEN
		RETURN 0;
	END;
END;
$$
LANGUAGE 'plpgsql';

-- Workaround for PSQL not supporting ON CONFLICT on partition tables
CREATE OR REPLACE FUNCTION elefana_overwrite(_op_index VARCHAR, _op_type VARCHAR, _op_id VARCHAR, _op_timestamp BIGINT, _op_bucket1s BIGINT, _op_bucket1m BIGINT, _op_bucket1h BIGINT, _op_bucket1d BIGINT, _op_source json) RETURNS INT AS
$$
BEGIN
	UPDATE elefana_data SET _source = _op_source, _timestamp = _op_timestamp, _bucket1s = _op_bucket1s, _bucket1m = _op_bucket1m, _bucket1h = _op_bucket1h, _bucket1d = _op_bucket1d WHERE _index = _op_index AND _type = _op_type AND _id = _op_id;
	IF found THEN
		RETURN 1;
	END IF;
	BEGIN
		INSERT INTO elefana_data(_index, _type, _id, _timestamp, _bucket1s, _bucket1m, _bucket1h, _bucket1d, _source) VALUES (_op_index, _op_type, _op_id, _op_timestamp, _op_bucket1s, _op_bucket1m, _op_bucket1h, _op_bucket1d, _op_source);
		RETURN 1;
	EXCEPTION WHEN unique_violation THEN
		RETURN 0;
	END;
END;
$$
LANGUAGE 'plpgsql';

-- Workaround for PSQL not supporting ON CONFLICT on partition tables
CREATE OR REPLACE FUNCTION elefana_update(_op_index VARCHAR, _op_type VARCHAR, _op_id VARCHAR, _op_timestamp BIGINT, _op_bucket1s BIGINT, _op_bucket1m BIGINT, _op_bucket1h BIGINT, _op_bucket1d BIGINT, _op_source json) RETURNS INT AS
$$
DECLARE
	existing_source jsonb;
BEGIN
	existing_source := (SELECT _source FROM elefana_data WHERE _index = _op_index AND _type = _op_type AND _id = _op_id);
	UPDATE elefana_data SET _source = (existing_source || _op_source::jsonb), _timestamp = _op_timestamp, _bucket1s = _op_bucket1s, _bucket1m = _op_bucket1m, _bucket1h = _op_bucket1h, _bucket1d = _op_bucket1d WHERE _index = _op_index AND _type = _op_type AND _id = _op_id;
	IF found THEN
		RETURN 1;
	END IF;
	BEGIN
		INSERT INTO elefana_data(_index, _type, _id, _timestamp, _bucket1s, _bucket1m, _bucket1h, _bucket1d, _source) VALUES (_op_index, _op_type, _op_id, _op_timestamp, _op_bucket1s, _op_bucket1m, _op_bucket1h, _op_bucket1d, _op_source);
		RETURN 1;
	EXCEPTION WHEN unique_violation THEN
		RETURN 0;
	END;
END;
$$
LANGUAGE 'plpgsql';