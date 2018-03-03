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

import io.confluent.kafka.schemaregistry.avro.AvroSchema;
import io.confluent.kafka.schemaregistry.avro.AvroUtils;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.SchemaString;
import io.confluent.kafka.schemaregistry.exceptions.InvalidSchemaException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.hbase.HbaseTables;
import io.confluent.kafka.schemaregistry.storage.hbase.SubjectVersionsTable;
import sun.reflect.generics.reflectiveObjects.NotImplementedException;

import java.util.Iterator;
import java.util.List;
import java.util.Set;

public class HBaseSchemaRegistry implements SchemaRegistry {
  HbaseTables hbaseTables;
  SchemaRegistryConfig schemaRegistryConfig;

  public HBaseSchemaRegistry(SchemaRegistryConfig schemaRegistryConfig) {
    this.schemaRegistryConfig = schemaRegistryConfig;

    this.hbaseTables = new HbaseTables(schemaRegistryConfig);
  }

  // This constructor create for testing dependency injection.
  public HBaseSchemaRegistry(SchemaRegistryConfig schemaRegistryConfig,
                             HbaseTables hbaseTables) {
    this.schemaRegistryConfig = schemaRegistryConfig;
    this.hbaseTables = hbaseTables;
  }

  @Override
  public void init() throws SchemaRegistryException {
    try {
      hbaseTables.init();
    } catch (Exception e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Unable to init HBase store", e);
    }
  }

  @Override
  public int register(String subject, Schema schema)
          throws SchemaRegistryException {
    try {
      canonicalizeSchema(schema);

      // Check if schema string is already in database, and get associated SchemaId
      //Integer schemaId = hbaseTables.schemasTable.doesSchemaStringExist(schema.getSchema());
      Integer schemaId = null;
      if (schemaId != null) {
        return schemaId;
      } else {
        // If not in database then add it including get new schema id;
        schemaId = hbaseTables.getSchemaIdCounterTable().incrementAndGetNextAvailableSchemaId();
        hbaseTables.getSchemasTable().put(schemaId, new SchemaString(schema.getSchema()));
      }

      hbaseTables.getSubjectVersionsTable().addNewVersion(schema.getSubject(), schemaId);

      return schemaId;
    } catch (StoreException se) {
      throw new SchemaRegistryException(se);
    }
  }

  private AvroSchema canonicalizeSchema(Schema schema)
          throws InvalidSchemaException {
    AvroSchema avroSchema = AvroUtils.parseSchema(schema.getSchema());
    if (avroSchema == null) {
      throw new InvalidSchemaException("Invalid schema " + schema.toString());
    }
    schema.setSchema(avroSchema.canonicalString);
    return avroSchema;
  }

  @Override
  public Schema get(String subject, int version, boolean returnDeletedSchema)
          throws SchemaRegistryException {
    Schema schema;
    try {
      SubjectVersionsTable.SubjectVersion subjectVersion =
              hbaseTables.getSubjectVersionsTable().getSubjectVersion(subject,
                      new VersionId(version));

      if (subjectVersion == null) {
        throw Errors.subjectNotFoundException();
      }


      SchemaString schemaString = hbaseTables.getSchemasTable().get(subjectVersion.schemaId);

      schema =
              new Schema(
                      subject,
                      subjectVersion.version,
                      subjectVersion.schemaId,
                      schemaString.getSchemaString());
    } catch (StoreException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Unable to get latest schema version", e);
    }
    return schema;
  }

  @Override
  public SchemaString get(int id) throws SchemaRegistryException {
    try {
      SchemaString schemaString = hbaseTables.getSchemasTable().get(id);
      return schemaString;
    } catch (StoreException e) {
      e.printStackTrace();

      throw new SchemaRegistryException(e);
    }
  }

  @Override
  public Set<String> listSubjects() throws SchemaRegistryException {
    throw new NotImplementedException();
  }

  @Override
  public Iterator<Schema> getAllVersions(String subject, boolean filterDeletes)
          throws SchemaRegistryException {
    throw new NotImplementedException();
  }

  @Override
  public Schema getLatestVersion(String subject) throws SchemaRegistryException {
    Schema schema;
    try {
      SubjectVersionsTable.SubjectVersion subjectVersion =
              hbaseTables.getSubjectVersionsTable().getSubjectVersion(subject, new VersionId(-1));

      SchemaString schemaString = hbaseTables.getSchemasTable().get(subjectVersion.schemaId);

      schema =
              new Schema(
                      subject,
                      subjectVersion.version,
                      subjectVersion.schemaId,
                      schemaString.getSchemaString());
    } catch (StoreException e) {
      e.printStackTrace();

      throw Errors.schemaRegistryException("Unable to get latest schema version", e);
    }
    return schema;
  }

  @Override
  public List<Integer> deleteSubject(String subject) throws SchemaRegistryException {
    throw new NotImplementedException();
  }

  @Override
  public Schema lookUpSchemaUnderSubject(String subject, Schema schema, boolean lookupDeletedSchema)
          throws SchemaRegistryException {
    throw new NotImplementedException();
  }

  @Override
  public boolean isCompatible(String subject, String inputSchema, String targetSchema)
          throws SchemaRegistryException {
    throw new NotImplementedException();
  }

  @Override
  public boolean isCompatible(String subject, String newSchema, List<String> previousSchemas)
          throws SchemaRegistryException {
    throw new NotImplementedException();
  }

  @Override
  public void close() {
  }

  @Override
  public void deleteSchemaVersion(String subject, Schema schema)
          throws SchemaRegistryException {
    throw new NotImplementedException();
  }
}
