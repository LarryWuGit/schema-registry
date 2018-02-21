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

package io.confluent.kafka.schemaregistry.storage.hbase;

import com.google.protobuf.ServiceException;
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
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

public class SchemaIdCounterTable {
  private static final String HBASE_TABLE_NAME = "schemaRegistry_schemaIdCounter";
  private static final String FAMILY_NAME = "sr";
  private static final String SCHEMA_ID_COUNTER_COLUMN_NAME = "schemaIdCounter";

  private TableName tableName = null;

  private Configuration hbaseConfig = null;

  public SchemaIdCounterTable() {
    tableName = TableName.valueOf(HBASE_TABLE_NAME);
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

      // Does Schema ID Counter table exists?
      if (!admin.tableExists(tableName)) {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
        admin.createTable(desc);

        Table schemaIdCounterTable = connection.getTable(tableName);
        byte[] row = Bytes.toBytes(SCHEMA_ID_COUNTER_COLUMN_NAME);
        Put p = new Put(row);
        p.addImmutable(FAMILY_NAME.getBytes(),
            Bytes.toBytes("currentSchemaId"), Bytes.toBytes(0L));

        schemaIdCounterTable.put(p);

        schemaIdCounterTable.close();
      }
    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  public int incrementAndGetNextAvailableSchemaId() {
    Increment increment;

    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table schemaIdCounterTable = connection.getTable(tableName)) {
      byte[] row = Bytes.toBytes(SCHEMA_ID_COUNTER_COLUMN_NAME);
      increment = new Increment(row);
      increment.addColumn(Bytes.toBytes(FAMILY_NAME),
          Bytes.toBytes("currentSchemaId"), 1L);

      Result result = schemaIdCounterTable.increment(increment);
      System.out.println(result);

      byte[] newSchemaIdBytes =
          result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("currentSchemaId"));

      long newSchemaId = Bytes.toLong(newSchemaIdBytes);
      return (int) newSchemaId;
    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Error accessing HBase", e);
    }
  }
}
