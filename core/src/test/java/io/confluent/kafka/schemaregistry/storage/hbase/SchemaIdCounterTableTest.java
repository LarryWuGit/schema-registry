package io.confluent.kafka.schemaregistry.storage.hbase;

import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.rest.RestConfigException;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class SchemaIdCounterTableTest {

  @Test
  public void testSchemaIdCounterTableTest_create() {
    try {

      SchemaIdCounterTable schemaIdCounterTable =
              new SchemaIdCounterTable(new SchemaRegistryConfig(new Properties()),
                      new HbaseTableMock());

      assertTrue("Does instance look ok?", schemaIdCounterTable!=null);

    } catch (RestConfigException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSchemaIdCounterTableTest_incrementAndGet() {
    try {
      SchemaIdCounterTable schemaIdCounterTable =
              new SchemaIdCounterTable(new SchemaRegistryConfig(new Properties()),
                      new HbaseTableMock());

      int id = schemaIdCounterTable.incrementAndGetNextAvailableSchemaId();
      assertTrue("Does schema id look ok?", id > 0);

    } catch (RestConfigException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSchemaIdCounterTableTest_incrementAndGet2() {
    try {

      SchemaIdCounterTable schemaIdCounterTable =
              new SchemaIdCounterTable(new SchemaRegistryConfig(new Properties()),
                      new HbaseTableMock());

      int id = schemaIdCounterTable.incrementAndGetNextAvailableSchemaId();
      assertTrue("Does schema id look ok?", id > 0);

      int nextId = schemaIdCounterTable.incrementAndGetNextAvailableSchemaId();
      assertTrue("Does getting next schema id look correct?", id+1 == nextId);

    } catch (RestConfigException e) {
      e.printStackTrace();
      fail();
    }
  }

}
