package io.confluent.kafka.schemaregistry.storage;

import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.storage.hbase.HbaseTablesMock;
import io.confluent.rest.RestConfigException;
import org.junit.Assert;
import org.junit.Test;

import java.util.Properties;

import static org.junit.Assert.fail;


public class HBaseSchemaRegistryTest {

  @Test
  public void testCreateHBaseSchemaRegistryInstance() {
    try {
      SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(new Properties());
      HBaseSchemaRegistry hBaseSchemaRegistry =
              new HBaseSchemaRegistry(schemaRegistryConfig,
                      new HbaseTablesMock(schemaRegistryConfig));

    } catch (RestConfigException e) {
      e.printStackTrace();
      fail();
    }
  }


  @Test
  public void testHBaseSchemaRegistry_register() {
    try {
      SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(new Properties());
      HBaseSchemaRegistry hBaseSchemaRegistry =
              new HBaseSchemaRegistry(schemaRegistryConfig,
                      new HbaseTablesMock(schemaRegistryConfig));

      String jsonSchema = "{\"type\": \"string\"}";
      Schema schema = new Schema("testSubject", 0, 0, jsonSchema);
      int schemaId = hBaseSchemaRegistry.register("testSubject", schema);

      Assert.assertTrue("Check valid schema Id", schemaId > 0);

    } catch (RestConfigException | SchemaRegistryException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testHBaseSchemaRegistry_getSubjectVersion() {
    try {
      SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(new Properties());
      HBaseSchemaRegistry hBaseSchemaRegistry =
              new HBaseSchemaRegistry(schemaRegistryConfig,
                      new HbaseTablesMock(schemaRegistryConfig));

      String subject = "testSubject";
      String jsonSchema = "{\"type\": \"string\"}";
      Schema schema = new Schema(subject, 0, 0, jsonSchema);
      int schemaId = hBaseSchemaRegistry.register(subject, schema);
      Assert.assertTrue("Check valid schema Id from register()", schemaId > 0);

      Schema schemaOutput = hBaseSchemaRegistry.get(subject, 1, false);

      Assert.assertTrue("Is valid subject from get()", subject.equals(schemaOutput.getSubject()));
      Assert.assertTrue("Is valid schema Id from get()", schemaOutput.getId() == schemaId);
      Assert.assertTrue("Is valid version from get()", schemaOutput.getVersion() == 1);
      Assert.assertTrue("Is valid schema string from get()", "\"string\"".equals(schemaOutput.getSchema()));

    } catch (RestConfigException | SchemaRegistryException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testHBaseSchemaRegistry_getBySchemaId() {
    try {
      SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(new Properties());
      HBaseSchemaRegistry hBaseSchemaRegistry =
              new HBaseSchemaRegistry(schemaRegistryConfig,
                      new HbaseTablesMock(schemaRegistryConfig));

      String jsonSchema = "{\"type\": \"string\"}";
      Schema schema = new Schema("testSubject", 0, 0, jsonSchema);
      int schemaId = hBaseSchemaRegistry.register("testSubject", schema);
      Assert.assertTrue("Check valid schema Id", schemaId > 0);

      SchemaString schemaOutput = hBaseSchemaRegistry.get(schemaId);

      Assert.assertTrue("Is valid schema string from get()", "\"string\"".equals(schemaOutput.getSchemaString()));

    } catch (RestConfigException | SchemaRegistryException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testHBaseSchemaRegistry_getLatestVersion() {
    try {
      SchemaRegistryConfig schemaRegistryConfig = new SchemaRegistryConfig(new Properties());
      HBaseSchemaRegistry hBaseSchemaRegistry =
              new HBaseSchemaRegistry(schemaRegistryConfig,
                      new HbaseTablesMock(schemaRegistryConfig));


      String subject = "testSubject";
      String jsonSchema = "{\"type\": \"string\"}";
      Schema schema = new Schema(subject, 0, 0, jsonSchema);

      // Create first version
      int schemaId = hBaseSchemaRegistry.register("testSubject", schema);
      Assert.assertTrue("Check valid schema Id", schemaId > 0);

      // Create second version
      schemaId = hBaseSchemaRegistry.register("testSubject", schema);
      Assert.assertTrue("Check valid schema Id", schemaId > 0);

      Schema schemaOutput = hBaseSchemaRegistry.getLatestVersion(subject);

      Assert.assertTrue("Is correct version from getLatestVersion() (should be 2)?", schemaOutput.getVersion() == 2);
      Assert.assertTrue("Is valid schema string from getLatestVersion()?", "\"string\"".equals(schemaOutput.getSchema()));
      Assert.assertTrue("Is valid subject from getLatestVersion()?", subject.equals(schemaOutput.getSubject()));
      Assert.assertTrue("Is correct schemaId from getLatestVersion()?", schemaOutput.getId() == schemaId);

    } catch (RestConfigException | SchemaRegistryException e) {
      e.printStackTrace();
      fail();
    }
  }
}
