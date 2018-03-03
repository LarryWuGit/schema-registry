package io.confluent.kafka.schemaregistry.storage.hbase;

import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.rest.RestConfigException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class SchemasTableTest {

  @Test
  public void testSchemasTable_create() {
    try {

      SchemasTable schemasTable = new SchemasTable(new SchemaRegistryConfig(new Properties()),
              new HbaseTableMock());
      assertTrue("Does instance look ok?", schemasTable!=null);

    } catch (RestConfigException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSchemasTable_get() {
    try {

      int schemaId = 987;
      String jsonSchema = "{\"type\": \"string\"}";

      SchemasTable schemasTable = new SchemasTable(new SchemaRegistryConfig(new Properties()),
              new HbaseTableMock());

      schemasTable.put(schemaId, new SchemaString(jsonSchema));

      SchemaString schemaString = schemasTable.get(schemaId);
      Assert.assertTrue("Is valid schema string from get()", jsonSchema.equals(schemaString.getSchemaString()));

    } catch (RestConfigException | StoreException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSchemasTable_put() {
    try {
      int schemaId = 1234;
      String jsonSchema = "{\"type\": \"string\"}";

      SchemasTable schemasTable = new SchemasTable(new SchemaRegistryConfig(new Properties()),
              new HbaseTableMock());

      schemasTable.put(schemaId, new SchemaString(jsonSchema));

      SchemaString schemaString = schemasTable.get(schemaId);

      Assert.assertTrue("Is valid schema string from get()", jsonSchema.equals(schemaString.getSchemaString()));

    } catch (RestConfigException | StoreException e) {
      e.printStackTrace();
      fail();
    }
  }


}
