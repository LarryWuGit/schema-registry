package io.confluent.kafka.schemaregistry.storage.hbase;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

public class SchemaIdCounterTableMock extends SchemaIdCounterTable {
  public SchemaIdCounterTableMock(SchemaRegistryConfig schemaRegistryConfig,
                                  HbaseTable hbaseTable) {
    super(schemaRegistryConfig, hbaseTable);
  }
}
