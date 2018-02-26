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

import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;

public class HbaseTables {
  public static SchemaIdCounterTable schemaIdCounterTable;
  public static SchemasTable schemasTable;
  public static SubjectVersionsTable subjectVersionsTable;

  SchemaRegistryConfig schemaRegistryConfig;

  public HbaseTables(SchemaRegistryConfig schemaRegistryConfig) {
    this.schemaRegistryConfig = schemaRegistryConfig;
  }

  public void init() throws SchemaRegistryException {
    try {
      schemaIdCounterTable = new SchemaIdCounterTable(schemaRegistryConfig);

      schemasTable = new SchemasTable(schemaRegistryConfig);
      schemasTable.init();

      subjectVersionsTable = new SubjectVersionsTable(schemaRegistryConfig);
      subjectVersionsTable.init();
    } catch (Exception e) {
      e.printStackTrace();
      throw Errors.schemaRegistryException("Unable to init HBase store", e);
    }
  }
}
