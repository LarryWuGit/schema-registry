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
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG;
import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.HBASE_ZOOKEEPER_QUORUM_CONFIG;

public class SchemaIdCounterTable {
  private static final String HBASE_TABLE_NAME = "schemaRegistry_schemaIdCounter";
  private static final String FAMILY_NAME = "sr";
  private static final String SCHEMA_ID_COUNTER_COLUMN_NAME = "schemaIdCounter";

  //private TableName tableName;
  private Configuration hbaseConfig;
  SchemaRegistryConfig schemaRegistryConfig;

  HbaseTable hbaseTable;

  public SchemaIdCounterTable(SchemaRegistryConfig schemaRegistryConfig) {
    this.schemaRegistryConfig = schemaRegistryConfig;
    this.hbaseTable = null;
  }

  // This constructor create for test dependency injection.
  public SchemaIdCounterTable(SchemaRegistryConfig schemaRegistryConfig,
                              HbaseTable hbaseTable) {
    this.schemaRegistryConfig = schemaRegistryConfig;
    this.hbaseTable = hbaseTable;
  }

  public void init() {
    hbaseConfig = HBaseConfiguration.create();

    hbaseConfig.set(HBASE_ZOOKEEPER_QUORUM_CONFIG,
            schemaRegistryConfig.getString(HBASE_ZOOKEEPER_QUORUM_CONFIG));
    hbaseConfig.set(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG,
            schemaRegistryConfig.getString(HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG));

    try {
      hbaseTable = new HbaseTable(hbaseConfig, HBASE_TABLE_NAME);
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
        hbaseTable.put(FAMILY_NAME, SCHEMA_ID_COUNTER_COLUMN_NAME, "currentSchemaId", 0L);
      }
    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  public int incrementAndGetNextAvailableSchemaId() {
    try {
      int newSchemaId = hbaseTable.incrementAndGet(FAMILY_NAME, SCHEMA_ID_COUNTER_COLUMN_NAME,
              "currentSchemaId");
      return newSchemaId;
    } catch (IOException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Error accessing HBase", e);
    }
  }
}
