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
import org.apache.hadoop.conf.Configuration;
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

public class HbaseTable {
  Configuration configuration;
  TableName tableName;

  public HbaseTable(Configuration configuration, String tableNameString) {
    this.configuration = configuration;
    this.tableName = TableName.valueOf(tableNameString);
  }

  public byte[] get(String familyName, String columnName, String key)
          throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(configuration);
         Table table = connection.getTable(tableName)) {
      Result result = table.get(new Get(Bytes.toBytes(key)));
      byte[] schemaStringBytes =
              result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(columnName));

      return schemaStringBytes;
    }
  }

  public void put(String familyName, String columnName, String key, String value)
          throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(configuration);
         Table table = connection.getTable(tableName)) {
      byte[] row = Bytes.toBytes(key);
      Put p = new Put(row);
      p.addImmutable(
              familyName.getBytes(),
              Bytes.toBytes(columnName),
              Bytes.toBytes(value));
      table.put(p);
    }
  }

  public void put(String familyName, String columnName, String key, long value)
          throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(configuration);
         Table table = connection.getTable(tableName)) {
      byte[] row = Bytes.toBytes(key);
      Put p = new Put(row);
      p.addImmutable(
              familyName.getBytes(),
              Bytes.toBytes(columnName),
              Bytes.toBytes(value));
      table.put(p);
    }
  }

  public void checkHBaseAvailable()
          throws ServiceException, IOException {
    HBaseAdmin.checkHBaseAvailable(configuration);
  }

  public int incrementAndGet(String familyName, String columnName, String key)
          throws IOException {
    Increment increment;

    try (Connection connection = ConnectionFactory.createConnection(configuration);
         Table schemaIdCounterTable = connection.getTable(tableName)) {
      byte[] row = Bytes.toBytes(columnName);
      increment = new Increment(row);
      increment.addColumn(Bytes.toBytes(familyName),
              Bytes.toBytes(key), 1L);

      Result result = schemaIdCounterTable.increment(increment);

      byte[] newIncrementedIdBytes =
              result.getValue(Bytes.toBytes(familyName), Bytes.toBytes(key));

      long newIncrementedId = Bytes.toLong(newIncrementedIdBytes);
      return (int) newIncrementedId;
    }
  }


  public boolean tableExists() throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(configuration);
         Admin admin = connection.getAdmin()) {

      // Does Schema ID Counter table exists?
      return admin.tableExists(tableName);
    }
  }

  public void createTable(String familyName) throws IOException {
    try (Connection connection = ConnectionFactory.createConnection(configuration);
         Admin admin = connection.getAdmin()) {
      HTableDescriptor desc = new HTableDescriptor(tableName);
      desc.addFamily(new HColumnDescriptor(familyName));
      admin.createTable(desc);
    }
  }
}
