import datetime
import re
import random
import string
import time
import uuid
from airbyte_cdk.models import ConfiguredAirbyteStream

# convert all non-alphanumeric or non-underscore characters to underscores
def get_alphanumeric_underscore_str(s) -> str:
    return re.sub(r'[^a-zA-Z0-9_]','_', s)

def truncate_str(s):
    if len(s) < 44:
        return s
    return s[:22] + s[len(s) - 22:]

def random_suffix(n):
   return ''.join(random.choices(string.ascii_lowercase +
                             string.digits, k=n))
def make_ns_pair(namespace, name):
    a = NamespaceStreamPair()
    a.name = name
    a.namespace =  namespace if namespace else "default"
    return a
class NamespaceStreamPair():
    def __init__(self, stream: ConfiguredAirbyteStream = None):
        if stream == None:
            return
        self.name = stream.stream.name
        self.namespace = stream.stream.namespace if stream.stream.namespace else "default"
    def __str__(self) -> str:
        return f"{self.name}:{self.namespace}"
    def __repr__(self) -> str:
        return f"{self.name}:{self.namespace}"
    def __hash__(self):
        return hash((self.name, self.namespace))

    def __eq__(self, other):
        return (self.name, self.namespace) == (other.name, other.namespace)

    def __ne__(self, other):
        # Not strictly necessary, but to avoid having both x==y and x!=y
        # True at the same time
        return not(self == other)

class WriterConfig():
    # constructor
    def __init__(self, stream: ConfiguredAirbyteStream):
        self.stream = stream
        self.stream_name = stream.stream.name
        self.stream_namespace = stream.stream.namespace if stream.stream.namespace else "default"
        self.date_time = datetime.datetime.utcnow()
        self.connection_id = uuid.uuid4()
        self.random = random_suffix(4)
    def __str__(self) -> str:
        return f"tmp_table: {self.get_tmp_table_name()}, dst_table: {self.get_dst_table_name()}, stage: {self.get_stage()}, stage_path: {self.get_stage_path()}"
    # getters
    def get_pair(self):
        return make_ns_pair(self.stream_namespace, self.stream_name)
    def get_stream_name(self):
        return get_alphanumeric_underscore_str(self.stream_name).lower()
    def get_stream_namespace(self):
        return get_alphanumeric_underscore_str(self.stream_namespace).lower()
    def get_tmp_table_name(self):
        return "_airbyte_tmp_" + self.random + "_" + self.get_stream_name()
    def get_dst_table_name(self):
        return "_airbyte_raw_" + self.get_stream_name()
    def get_stage(self):
        return f"{self.get_stream_namespace()}_{self.get_stream_name()}"
    def get_stage_path(self):
        return '{}/{:02d}/{:02d}/{:02d}/{:02d}/{}/'.format(self.get_stream_name(), self.date_time.year, self.date_time.month, self.date_time.day, self.date_time.hour, str(self.connection_id))