#
# Copyright (c) 2022 Airbyte, Inc., all rights reserved.
#


import logging
from typing import Any, Iterable, Mapping

from airbyte_cdk import AirbyteLogger
from airbyte_cdk.destinations import Destination
from airbyte_cdk.models import AirbyteConnectionStatus, AirbyteMessage, ConfiguredAirbyteCatalog, Status, Type, DestinationSyncMode
from .connector import ClickhouseConnector
from .config import WriterConfig, NamespaceStreamPair, make_ns_pair
from .sqls import *
from .buffer import CSVBuffer

class DestinationDatabendPy(Destination):
    def write(
        self, config: Mapping[str, Any], configured_catalog: ConfiguredAirbyteCatalog, input_messages: Iterable[AirbyteMessage]
    ) -> Iterable[AirbyteMessage]:

        """
        Reads the input stream of messages, config, and catalog to write data to the destination.

        This method returns an iterable (typically a generator of AirbyteMessages via yield) containing state messages received
        in the input message stream. Outputting a state message means that every AirbyteRecordMessage which came before it has been
        successfully persisted to the destination. This is used to ensure fault tolerance in the case that a sync fails before fully completing,
        then the source is given the last state message output from this method as the starting point of the next sync.

        :param config: dict of JSON configuration matching the configuration declared in spec.json
        :param configured_catalog: The Configured Catalog describing the schema of the data being received and how it should be persisted in the
                                    destination
        :param input_messages: The stream of input messages received from the source
        :return: Iterable of AirbyteStateMessages wrapped in AirbyteMessage structs
        """
        cfgs = dict(zip(list(map(NamespaceStreamPair, configured_catalog.streams)), list(map(WriterConfig, configured_catalog.streams))))
        connector = ClickhouseConnector()
        connector.connect(**config)
        schema = config["database"]
        connector.fetch_all(create_database(schema))
        buffers = {}
        for k, v in cfgs.items():
            connector.fetch_all(create_stage((v.get_stage())))
            buffers[k] = CSVBuffer(v, connector)
        for message in input_messages:
            if message.type == Type.STATE:
                # Emitting a state message indicates that all records which came before it have been written to the destination. So we flush
                # the queue to ensure writes happen, then output the state message to indicate it's safe to checkpoint state
                for k, v in buffers.items():
                    logging.getLogger("airbyte").info(f"Flushing buffer for {v.config}")
                    v.flush()
                    connector.fetch_all(create_table(schema, v.config.get_tmp_table_name()))
                    connector.fetch_all(create_table(schema, v.config.get_dst_table_name()))
                    connector.fetch_all(copy_into_table(schema, v.config.get_tmp_table_name(), v.config.get_stage(), v.config.get_stage_path(), v.get_uploaded_files_str()))
                    connector.fetch_all(remove_stage(v.config.get_stage()))
                    v.clean()
                    if v.config.stream.destination_sync_mode == DestinationSyncMode.overwrite:
                        logging.getLogger("airbyte").info(f"Overwrite mode detected Dropping table {schema}.{v.config.get_dst_table_name()}")
                        connector.fetch_all(truncate_table(schema, v.config.get_dst_table_name()))
                    
                    connector.fetch_all(copy_table(schema, v.config.get_tmp_table_name(), v.config.get_dst_table_name()))
                    connector.fetch_all(drop_table(schema, v.config.get_tmp_table_name()))
                yield message
            elif message.type == Type.RECORD:
                record = message.record
                p = make_ns_pair(record.namespace, record.stream)
                if p in buffers:
                    buffers[p].add(record)
                else :
                    logging.getLogger("airbyte").warn(f"Received record for unknown stream {p}")
        # pair = map(NamespaceStreamPair, )
        # for stream in pair:
        # cfg = map(WriterConfig, configured_catalog.streams)
        # for c in cfg:
        #     
        # pairs = dict(zip(, map(WriterConfig, configured_catalog.streams)))
        # for k, v in pairs:
        

    def check(self, logger: AirbyteLogger, config: Mapping[str, Any]) -> AirbyteConnectionStatus:
        """
        Tests if the input configuration can be used to successfully connect to the destination with the needed permissions
            e.g: if a provided API token or password can be used to connect and write to the destination.

        :param logger: Logging object to display debug/info/error to the logs
            (logs will not be accessible via airbyte UI if they are not passed to this logger)
        :param config: Json object containing the configuration of this destination, content of this json is as specified in
        the properties of the spec.json file

        :return: AirbyteConnectionStatus indicating a Success or Failure
        """
        try:
            logging.getLogger("airbyte").info("Checking connection to databend destination")
            connector = ClickhouseConnector()
            connector.connect(**config)
            connector.fetch_all(create_database(config["database"]))
            connector.fetch_all(create_table(config["database"], "__airbyte_tmp_test_123"))
            connector.fetch_all(drop_table(config["database"], "__airbyte_tmp_test_123"))
            connector.fetch_all(create_stage("__airbyte_tmp_test_123"))
            connector.fetch_all(drop_stage("__airbyte_tmp_test_123"))
            return AirbyteConnectionStatus(status=Status.SUCCEEDED)
        except Exception as e:
            return AirbyteConnectionStatus(status=Status.FAILED, message=f"An exception occurred: {repr(e)}")
