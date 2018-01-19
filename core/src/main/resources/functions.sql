CREATE OR REPLACE FUNCTION select_shard(_distributedTable VARCHAR) RETURNS bigint AS $$
DECLARE
  shard_id bigint;
BEGIN
  SELECT shardid INTO shard_id
  FROM pg_dist_shard JOIN pg_dist_placement USING (shardid)
  WHERE logicalrelid = _distributedTable::regclass AND shardlength < 1024*1024*1024;

  IF shard_id IS NULL THEN
    /* no shard smaller than 1GB, create a new one */
    SELECT master_create_empty_shard(_distributedTable) INTO shard_id;
  END IF;

  RETURN shard_id;
END;
$$ LANGUAGE plpgsql;

-- Workaround for PSQL not supporting ON CONFLICT on partition tables
CREATE OR REPLACE FUNCTION elefana_create(_op_index VARCHAR, _op_type VARCHAR, _op_id VARCHAR, _op_timestamp BIGINT, _op_source json) RETURNS INT AS
$$
BEGIN
	BEGIN
		INSERT INTO elefana_data(_index, _type, _id, _timestamp, _source) VALUES (_op_index, _op_type, _op_id, _op_timestamp, _op_source);
		RETURN 1;
	EXCEPTION WHEN unique_violation THEN
		RETURN 0;
	END;
END;
$$
LANGUAGE 'plpgsql';

-- Workaround for PSQL not supporting ON CONFLICT on partition tables
CREATE OR REPLACE FUNCTION elefana_overwrite(_op_index VARCHAR, _op_type VARCHAR, _op_id VARCHAR, _op_timestamp BIGINT, _op_source json) RETURNS INT AS
$$
BEGIN
	UPDATE elefana_data SET _source = _op_source, _timestamp = _op_timestamp WHERE _index = _op_index AND _type = _op_type AND _id = _op_id;
	IF found THEN
		RETURN 1;
	END IF;
	BEGIN
		INSERT INTO elefana_data(_index, _type, _id, _timestamp, _source) VALUES (_op_index, _op_type, _op_id, _op_timestamp, _op_source);
		RETURN 1;
	EXCEPTION WHEN unique_violation THEN
		RETURN 0;
	END;
END;
$$
LANGUAGE 'plpgsql';

-- Workaround for PSQL not supporting ON CONFLICT on partition tables
CREATE OR REPLACE FUNCTION elefana_update(_op_index VARCHAR, _op_type VARCHAR, _op_id VARCHAR, _op_timestamp BIGINT, _op_source json) RETURNS INT AS
$$
DECLARE
	existing_source jsonb;
BEGIN
	existing_source := (SELECT _source FROM elefana_data WHERE _index = _op_index AND _type = _op_type AND _id = _op_id);
	UPDATE elefana_data SET _source = (existing_source || _op_source::jsonb), _timestamp = _op_timestamp WHERE _index = _op_index AND _type = _op_type AND _id = _op_id;
	IF found THEN
		RETURN 1;
	END IF;
	BEGIN
		INSERT INTO elefana_data(_index, _type, _id, _timestamp, _source) VALUES (_op_index, _op_type, _op_id, _op_timestamp, _op_source);
		RETURN 1;
	EXCEPTION WHEN unique_violation THEN
		RETURN 0;
	END;
END;
$$
LANGUAGE 'plpgsql';