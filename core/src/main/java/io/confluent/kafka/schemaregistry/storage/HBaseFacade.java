/**
 * Copyright 2014 Confluent Inc.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.storage;

import com.google.protobuf.ServiceException;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class HBaseFacade {
  private static final String SCHEMA_ID_COUNTER_TABLE_NAME = "schemaRegistry_schemaIdCounter";
  private static final String SCHEMAS_TABLE_NAME = "schemaRegistry_schemas";
  private static final String SUBJECT_VERSIONS_TABLE_NAME = "schemaRegistry_subjectVersions";

  private static final String FAMILY_NAME = "sr";
  private static final String SCHEMA_ID_COUNTER_COLUMN_NAME = "schemaIdCounter";

  private TableName schemasTableName = null;
  private TableName schemaIdCounterTableName = null;
  private TableName subjectVersionsTableName = null;

  private Configuration hbaseConfig = null;

  public HBaseFacade() {
    schemasTableName = TableName.valueOf(SCHEMAS_TABLE_NAME);
    schemaIdCounterTableName = TableName.valueOf(SCHEMA_ID_COUNTER_TABLE_NAME);
    subjectVersionsTableName = TableName.valueOf(SUBJECT_VERSIONS_TABLE_NAME);

    hbaseConfig = HBaseConfiguration.create();

    String path = this.getClass()
        .getClassLoader()
        .getResource("hbase-site.xml")
        .getPath();
    hbaseConfig.addResource(new Path(path));

    try {
      HBaseAdmin.checkHBaseAvailable(hbaseConfig);
    } catch (ServiceException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("HBase not available", e);
    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("HBase not available", e);
    }

    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig)) {
      Admin admin = connection.getAdmin();

      // Does main Schema Strings table exists?
      if (!admin.tableExists(schemasTableName)) {
        HTableDescriptor desc = new HTableDescriptor(schemasTableName);
        desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
        admin.createTable(desc);
      }

      // Does Schema ID Counter table exists?
      if (!admin.tableExists(schemaIdCounterTableName)) {
        HTableDescriptor desc = new HTableDescriptor(schemaIdCounterTableName);
        desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
        admin.createTable(desc);

        Table schemaIdCounterTable = connection.getTable(schemaIdCounterTableName);
        byte[] row = Bytes.toBytes(SCHEMA_ID_COUNTER_COLUMN_NAME);
        Put p = new Put(row);
        p.addImmutable(FAMILY_NAME.getBytes(), Bytes.toBytes("currentSchemaId"), Bytes.toBytes(0));
        schemaIdCounterTable.put(p);

        schemaIdCounterTable.close();
      }

      // Does main Subject Versions table exists?
      if (!admin.tableExists(subjectVersionsTableName)) {
        HTableDescriptor desc = new HTableDescriptor(subjectVersionsTableName);
        desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
        admin.createTable(desc);
      }


    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  public int incrementAndGetNextAvailableSchemaId() {
    Increment increment;

    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table schemaIdCounterTable = connection.getTable(schemaIdCounterTableName)) {
      byte[] row = Bytes.toBytes(SCHEMA_ID_COUNTER_COLUMN_NAME);
      increment = new Increment(row);
      increment.addColumn(Bytes.toBytes(FAMILY_NAME),
          Bytes.toBytes("currentSchemaId"), 1);

      Result result = schemaIdCounterTable.increment(increment);
      System.out.println(result);

      byte[] newSchemaIdBytes =
          result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("currentSchemaId"));

      int newSchemaId = Bytes.toInt(newSchemaIdBytes);
      return newSchemaId;
    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Error accessing HBase", e);
    }
  }

  public void addSchemaString(int schemaId, String schemaString) {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table schemasTable = connection.getTable(schemasTableName)) {
      byte[] row = Bytes.toBytes("" + schemaId);
      Put p = new Put(row);
      p.addImmutable(FAMILY_NAME.getBytes(),
          Bytes.toBytes("schemaString"), Bytes.toBytes(schemaString));
      schemasTable.put(p);

    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  public SchemaString getSchemaString(int schemaId) {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table schemasTable = connection.getTable(schemasTableName)) {
      Result result = schemasTable.get(new Get(Bytes.toBytes("" + schemaId)));
      byte[] schemaStringBytes =
          result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("schemaString"));

      System.out.println(result);

      SchemaString schemaString = new SchemaString();
      schemaString.setSchemaString(Bytes.toString(schemaStringBytes));

      return schemaString;
    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  public int isSchemaStringInHBase(String schemaString) {
    return 1;
  }

  /*public static class SubjectVersionsRecord {
    public String subject;
    public JsonObject versions;
  }*/

  public void getSubject(String subject) {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table subjectVersionsTable = connection.getTable(subjectVersionsTableName)) {
      subjectVersionsTable.get(new Get(Bytes.toBytes(subject)));
    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  public void addVersionToSubject() {
  }
}
