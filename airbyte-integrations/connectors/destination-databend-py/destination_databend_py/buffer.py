import csv
import datetime
import imp
import json
import logging
import os
import tempfile
import uuid
import sys

from airbyte_cdk.models import AirbyteRecordMessage
import requests
from .config import NamespaceStreamPair, WriterConfig
from .sqls import *
from .connector import ClickhouseConnector

class CSVBuffer():
    def __init__(self, config : WriterConfig, databend_cli : ClickhouseConnector, max_buffer_size = 128 * 1024 * 1024 ):
        self.max_buffer_size = max_buffer_size
        self.buffer_size = 0
        self.stage_id = 0
        self.config : WriterConfig = config
        self.uploaded_files = []
        self.buffer = []
        self.pair = config.get_pair()
        self.directory = tempfile.mkdtemp(prefix=str(self.pair))
        self.cli = databend_cli
        self.logger = databend_cli.logger
    def get_stage_id(self):
        return self.stage_id

    def add(self, message: AirbyteRecordMessage):
        row = [ str(uuid.uuid4()), json.dumps(message.data), datetime.datetime.fromtimestamp(float(message.emitted_at) / 1000.0).strftime('%Y-%m-%d %H:%M:%S.%f') ]
        self.buffer_size += sys.getsizeof(row) 
        self.buffer.append(row)
        if self.buffer_size > self.max_buffer_size:
            self.flush()
    def flush(self):
        full_name = f"{self.directory}/{self.pair.name}_{self.pair.namespace}_{self.stage_id}.csv"
        self.logger.info(f"Flushing buffer to {full_name} size {self.buffer_size}")
        with open(f"{full_name}", "w") as f:
            writer = csv.writer(f, doublequote=True, quoting=csv.QUOTE_ALL, escapechar='\\')
            writer.writerows(self.buffer)
            
            self.buffer.clear()
            self.buffer = []
            self.buffer_size = 0
        self.upload_file_to_stage(file_name=full_name, stage=self.config.get_stage(), stage_path=self.config.get_stage_path())
        if os.path.isfile(full_name):
            os.remove(full_name)
    def clean(self):
        self.buffer.clear()
        self.buffer = []
        self.buffer_size = 0
        for file in self.uploaded_files:
            if os.path.isfile(f"{self.directory}/{file}"):
                os.remove(f"{self.directory}/{file}")
        self.uploaded_files.clear()
        self.uploaded_files = []
        self.stage_id = 0
    def get_uploaded_files_str(self):
        return " files = ( " + ", ".join(f"'{file}'" for file in self.uploaded_files) + " )"
    def upload_file_to_stage(self, file_name, stage, stage_path):
            
        base = os.path.basename(file_name)
        size = os.path.getsize(filename=file_name)
        if size == 0:
            self.logger.info(f"no need to upload file {file_name}, the size is empty")
            return base
        attempts = 0
        success = False
        exceptions = []
        while attempts < 3 and not success:
            try:
                res = self.cli.fetch_all(presign_upload(stage, stage_path, base))
                headers = res[0][1]
                presigned_url = res[0][2]
                json_acceptable_string = headers.replace("'", "\"")
                d = json.loads(json_acceptable_string)
                self.logger.info(f"Uploading {file_name} size: {size} to {presigned_url}, headers {d}")
                r = requests.put(presigned_url, data=open(file_name, 'rb'), headers=d)
                if r.status_code < 200 or r.status_code >= 400:
                    raise Exception(f"Upload failed with status code {r.status_code}")
                self.logger.info(f"Uploaded {file_name} to {stage}/{stage_path}/{base}")
                success = True
            except Exception as e:
                self.logger.error(f"Upload failed with exception , retry {e}")
                exceptions.append(e)
                attempts += 1
                if attempts == 3:
                    break
        if not success:
            raise Exception(f"Failed to upload file {file_name} to stage {stage}/{stage_path} after {attempts} attempts, last exception {exceptions[-1]}")
        else:
            self.uploaded_files.append(base)
            self.stage_id += 1
    
        return base

    def get_buffer(self):
        return self.buffer

    def get_buffer_size(self):
        return self.buffer_size