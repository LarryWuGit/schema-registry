package io.confluent.kafka.schemaregistry.storage.hbase;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;

public class SubjectVersionsTableMock extends SubjectVersionsTable {
  public SubjectVersionsTableMock(SchemaRegistryConfig schemaRegistryConfig,
                                  HbaseTable hbaseTable) {
    super(schemaRegistryConfig, hbaseTable);
  }
}
