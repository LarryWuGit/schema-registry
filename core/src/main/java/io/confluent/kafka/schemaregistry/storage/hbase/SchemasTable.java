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
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.Store;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG;
import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.HBASE_ZOOKEEPER_QUORUM_CONFIG;

public class SchemasTable implements Store<Integer, SchemaString> {
  private static final String HBASE_TABLE_NAME = "schemaRegistry_schemas";
  private static final String FAMILY_NAME = "sr";

  private TableName tableName = null;
  private Configuration hbaseConfig = null;
  SchemaRegistryConfig schemaRegistryConfig;

  public SchemasTable(SchemaRegistryConfig schemaRegistryConfig) {
    this.schemaRegistryConfig = schemaRegistryConfig;
  }

  @Override
  public void init() throws StoreInitializationException {
    tableName = TableName.valueOf(HBASE_TABLE_NAME);

    hbaseConfig = HBaseConfiguration.create();

    hbaseConfig.set(HBASE_ZOOKEEPER_QUORUM_CONFIG,
            schemaRegistryConfig.getString(HBASE_ZOOKEEPER_QUORUM_CONFIG));
    hbaseConfig.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG,
            schemaRegistryConfig.getString(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG));

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
      if (!admin.tableExists(tableName)) {
        HTableDescriptor desc = new HTableDescriptor(tableName);
        desc.addFamily(new HColumnDescriptor(FAMILY_NAME));
        admin.createTable(desc);
      }

    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  @Override
  public SchemaString get(Integer key) throws StoreException {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table schemasTable = connection.getTable(tableName)) {
      Result result = schemasTable.get(new Get(Bytes.toBytes("" + key)));
      byte[] schemaStringBytes =
          result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("schemaString"));

      SchemaString schemaString = new SchemaString();
      schemaString.setSchemaString(Bytes.toString(schemaStringBytes));

      return schemaString;
    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  @Override
  public void put(Integer key, SchemaString value) throws StoreException {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table schemasTable = connection.getTable(tableName)) {
      byte[] row = Bytes.toBytes("" + key);
      Put p = new Put(row);
      p.addImmutable(
          FAMILY_NAME.getBytes(),
          Bytes.toBytes("schemaString"),
          Bytes.toBytes(value.getSchemaString()));
      schemasTable.put(p);
    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  @Override
  public Iterator<SchemaString> getAll(Integer key1, Integer key2) throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public void putAll(Map<Integer, SchemaString> entries) throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public void delete(Integer key) throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<Integer> getAllKeys() throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
  }

  public Integer doesSchemaStringExist(String schemaString) {
    return null;
  }
}
