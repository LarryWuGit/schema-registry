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
  private Configuration hbaseConfig = null;
  SchemaRegistryConfig schemaRegistryConfig;

  HbaseTable hbaseTable;

  public SchemasTable(SchemaRegistryConfig schemaRegistryConfig) {
    this.schemaRegistryConfig = schemaRegistryConfig;
    this.hbaseTable = null;
  }

  // This constructor create for test dependency injection.
  public SchemasTable(SchemaRegistryConfig schemaRegistryConfig,
                      HbaseTable hbaseTable) {
    this.schemaRegistryConfig = schemaRegistryConfig;
    this.hbaseTable = hbaseTable;
  }


  @Override
  public void init() throws StoreInitializationException {

    hbaseConfig = HBaseConfiguration.create();

    hbaseConfig.set(HBASE_ZOOKEEPER_QUORUM_CONFIG,
            schemaRegistryConfig.getString(HBASE_ZOOKEEPER_QUORUM_CONFIG));
    hbaseConfig.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG,
            schemaRegistryConfig.getString(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG));

    try {
      // If hbaseTable wasn't passed in, then create it.
      if (hbaseTable == null) {
        hbaseTable = new HbaseTable(hbaseConfig, HBASE_TABLE_NAME);
      }

      hbaseTable.checkHBaseAvailable();
    } catch (ServiceException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("HBase not available", e);
    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("HBase not available", e);
    }

    try {
      if (!hbaseTable.tableExists()) {
        hbaseTable.createTable(FAMILY_NAME);
      }

    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  @Override
  public SchemaString get(Integer key) throws StoreException {
    try {

      byte[] schemaStringBytes = hbaseTable.get(FAMILY_NAME, "schemaString", "" + key);

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
    try {
      hbaseTable.put(FAMILY_NAME, "schemaString", "" + key, value.getSchemaString());


    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.storeException("Cannot create connection to HBase", e);
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

}
