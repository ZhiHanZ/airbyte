def presign_upload(stage_name, stage_path, file_name):
     return f"PRESIGN UPLOAD @{stage_name}/{stage_path}{file_name};"

def copy_into_table(schema, table, stage_name, stage_path, files):
    return f"COPY INTO {schema}.{table} FROM @{stage_name}/{stage_path} {files} file_format = (type = 'csv' compression = auto );"
def copy_table(schema, src_table, dst_table):
    return f"INSERT INTO {schema}.{dst_table} SELECT * FROM {schema}.{src_table}"
def create_table(schema, table):
    return f"CREATE TABLE IF NOT EXISTS {schema}.{table} ( _airbyte_ab_id String, _airbyte_data JSON, _airbyte_emitted_at Timestamp DEFAULT now()) CLUSTER BY(_airbyte_ab_id) ;"
def drop_table(schema, table):
    return f"DROP TABLE IF EXISTS {schema}.{table}"

def truncate_table(schema, table):
    return f"TRUNCATE TABLE {schema}.{table}"

def create_database(schema):
    return f"CREATE DATABASE IF NOT EXISTS {schema}"
def create_stage(stage_name):
    return f"CREATE STAGE IF NOT EXISTS {stage_name}"
def drop_stage(stage_name):
    return f"DROP STAGE IF EXISTS {stage_name}"
def remove_stage(stage_name):
    return f"REMOVE @{stage_name}"