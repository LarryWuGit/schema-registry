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

  private Configuration hbaseConfig = null;
  SchemaRegistryConfig schemaRegistryConfig;

  HbaseTable hbaseTable;

  public SubjectVersionsTable(SchemaRegistryConfig schemaRegistryConfig) {
    this.schemaRegistryConfig = schemaRegistryConfig;
    this.hbaseTable = null;
  }

  // This constructor create for test dependency injection.
  public SubjectVersionsTable(SchemaRegistryConfig schemaRegistryConfig,
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
  public SubjectVersionsRow get(String key) throws StoreException {
    try {
      byte[] jsonSubjectVersions = hbaseTable.get(FAMILY_NAME, "jsonSubjectVersions", key);
      String json = Bytes.toString(jsonSubjectVersions);
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
    try {
      hbaseTable.put(FAMILY_NAME, "jsonSubjectVersions", key, value.toJson());
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

    public SubjectVersionsRow() {
      objectMapper = new ObjectMapper();
      subjectVersions = new ArrayList<>();
    }


    public SubjectVersionsRow(String jsonString) {
      try {
        objectMapper = new ObjectMapper();

        if (jsonString != null && jsonString.trim().length() > 0) {
          TypeReference<List<SubjectVersion>> mapType =
              new TypeReference<List<SubjectVersion>>()
            {};
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
