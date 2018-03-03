package io.confluent.kafka.schemaregistry.storage.hbase;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.hbase.HbaseTables;
import io.confluent.kafka.schemaregistry.storage.hbase.SchemaIdCounterTable;
import io.confluent.kafka.schemaregistry.storage.hbase.SchemasTable;
import io.confluent.kafka.schemaregistry.storage.hbase.SubjectVersionsTable;

public class HbaseTablesMock extends HbaseTables {

  public HbaseTablesMock(SchemaRegistryConfig config) {
    super(config);

    schemaIdCounterTable = new SchemaIdCounterTableMock(config, new HbaseTableMock());
    schemasTable = new SchemasTableMock(config, new HbaseTableMock());
    subjectVersionsTable = new SubjectVersionsTableMock(config, new HbaseTableMock());
  }
}
