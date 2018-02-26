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

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.ServiceException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
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
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.HBASE_ZOOKEEPER_PROPERTY_CLIENTPORT_CONFIG;
import static io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig.HBASE_ZOOKEEPER_QUORUM_CONFIG;

public class SubjectVersionsTable
    implements Store<String, SubjectVersionsTable.SubjectVersionsRow> {
  private static final String HBASE_TABLE_NAME = "schemaRegistry_subjectVersions";
  private static final String FAMILY_NAME = "sr";

  private TableName tableName = null;
  private Configuration hbaseConfig = null;
  SchemaRegistryConfig schemaRegistryConfig;

  public SubjectVersionsTable(SchemaRegistryConfig schemaRegistryConfig) {
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

      // Does Subject Versions table exists?
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
  public SubjectVersionsRow get(String key) throws StoreException {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table table = connection.getTable(tableName)) {
      Result result = table.get(new Get(Bytes.toBytes(key)));
      byte[] jsonSubjectVersions =
          result.getValue(Bytes.toBytes(FAMILY_NAME), Bytes.toBytes("jsonSubjectVersions"));

      String json = Bytes.toString(jsonSubjectVersions);
      System.out.println(result);
      SubjectVersionsRow versionsRow = new SubjectVersionsRow(json);
      return versionsRow;
    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  public SubjectVersion getSubjectVersion(String subject, VersionId versionId)
      throws StoreException {
    SubjectVersion subjectVersion;
    SubjectVersionsRow row = this.get(subject);

    if (versionId.isLatest()) {
      subjectVersion = row.getLatestVersion();
    } else {
      subjectVersion = row.getVersion(versionId.getVersionId());
    }
    return subjectVersion;
  }

  @Override
  public void put(String key, SubjectVersionsRow value)
      throws StoreException {
    try (Connection connection = ConnectionFactory.createConnection(hbaseConfig);
         Table schemasTable = connection.getTable(tableName)) {
      byte[] row = Bytes.toBytes("" + key);
      Put p = new Put(row);
      p.addImmutable(
          FAMILY_NAME.getBytes(),
          Bytes.toBytes("jsonSubjectVersions"),
          Bytes.toBytes(value.toJson()));
      schemasTable.put(p);

    } catch (IOException e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Cannot create connection to HBase", e);
    }
  }

  @Override
  public Iterator<SubjectVersionsRow> getAll(String key1, String key2)
      throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public void putAll(Map<String, SubjectVersionsRow> entries)
      throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public void delete(String key) throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<String> getAllKeys() throws StoreException {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
  }

  public void addNewVersion(String subject, int schemaId)
      throws StoreException {
    SubjectVersionsRow row = this.get(subject);
    row.addVersion(schemaId);
    this.put(subject, row);
  }

  public static class SubjectVersionsRow {
    List<SubjectVersion> subjectVersions;
    ObjectMapper objectMapper;

    public SubjectVersionsRow(String jsonString) {
      try {
        objectMapper = new ObjectMapper();

        if (jsonString != null && jsonString.trim().length() > 0) {
          TypeReference<List<SubjectVersion>> mapType =
              new TypeReference<List<SubjectVersion>>(){};
          subjectVersions = objectMapper.readValue(jsonString, mapType);
        } else {
          subjectVersions = new ArrayList<>();
        }
      } catch (IOException e) {
        e.printStackTrace();

        throw Errors.schemaRegistryException(
            "Unable to parse JSON when reading Subject Version table: " + jsonString, e);
      }
    }

    public SubjectVersion get(int version) {
      try {
        for (SubjectVersion subjectVersion : subjectVersions) {
          if (subjectVersion.version == version) {
            return subjectVersion;
          }
        }
        throw Errors.invalidVersionException();
      } catch (Exception e) {
        e.printStackTrace();
        throw Errors.schemaRegistryException("Unable to parser subject entry", e);
      }
    }

    public void addVersion(int schemaId) {
      int version = 0;

      // Find latest version number
      for (SubjectVersion subjectVersion : subjectVersions) {
        if (subjectVersion.version > version) {
          version = subjectVersion.version;
        }
      }
      version++;
      subjectVersions.add(new SubjectVersion(version, schemaId));
    }

    public SubjectVersion getLatestVersion() {
      SubjectVersion latestSubjectVersion = null;
      int version = 0;

      for (SubjectVersion subjectVersion : subjectVersions) {
        if (subjectVersion.version > version) {
          version = subjectVersion.version;
          latestSubjectVersion = subjectVersion;
        }
      }
      return latestSubjectVersion;
    }

    public SubjectVersion getVersion(int version) {
      for (SubjectVersion subjectVersion : subjectVersions) {
        if (subjectVersion.version == version) {
          return subjectVersion;
        }
      }

      throw Errors.versionNotFoundException();
    }


    public String toJson() {
      try {
        String json = objectMapper.writeValueAsString(subjectVersions);
        return json;
      } catch (JsonProcessingException e) {
        e.printStackTrace();
        throw Errors.storeException("Unable to create JSON string for SubjectVersion", e);
      }
    }
  }

  public static class SubjectVersion {
    public int schemaId;
    public int version;

    public SubjectVersion() {
    }

    public SubjectVersion(int version, int schemaId) {
      this.version = version;
      this.schemaId = schemaId;
    }

  }

}
