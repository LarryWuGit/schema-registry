package io.confluent.kafka.schemaregistry.storage.hbase;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

public class SchemasTableMock extends SchemasTable {
  public SchemasTableMock(SchemaRegistryConfig schemaRegistryConfig,
                          HbaseTable hbaseTable) {
    super(schemaRegistryConfig, hbaseTable);
  }
}
