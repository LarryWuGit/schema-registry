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

package io.confluent.kafka.schemaregistry.rest.resources;

import io.confluent.kafka.schemaregistry.client.rest.Versions;
import io.confluent.kafka.schemaregistry.client.rest.entities.Schema;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaRequest;
import io.confluent.kafka.schemaregistry.client.rest.entities.requests.RegisterSchemaResponse;
import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryStoreException;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.rest.exceptions.Errors;
import io.confluent.kafka.schemaregistry.storage.HBaseSchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.rest.annotations.PerformanceMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.validation.constraints.NotNull;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.container.AsyncResponse;
import javax.ws.rs.container.Suspended;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.HttpHeaders;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

@Path("/subjects/{subject}/versions")
@Produces({Versions.SCHEMA_REGISTRY_V1_JSON_WEIGHTED,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON_WEIGHTED,
    Versions.JSON_WEIGHTED})
@Consumes({Versions.SCHEMA_REGISTRY_V1_JSON,
    Versions.SCHEMA_REGISTRY_DEFAULT_JSON,
    Versions.JSON, Versions.GENERIC_REQUEST})
public class SubjectVersionsResource {

  private static final Logger log = LoggerFactory.getLogger(SubjectVersionsResource.class);

  //private final KafkaSchemaRegistry schemaRegistry;
  private final SchemaRegistry schemaRegistry;


  private final RequestHeaderBuilder requestHeaderBuilder = new RequestHeaderBuilder();

  public SubjectVersionsResource(SchemaRegistry registry) {
    this.schemaRegistry = registry;
  }

  @GET
  @Path("/{version}")
  @PerformanceMetric("subjects.versions.get-schema")
  public Schema getSchema(@PathParam("subject") String subject,
                          @PathParam("version") String version) {
    VersionId versionId = null;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException();
    }
    Schema schema = null;
    try {
      schema = ((HBaseSchemaRegistry) schemaRegistry).get(subject, versionId.getVersionId(), false);
    } catch (SchemaRegistryException e) {
      //e.printStackTrace();
      String errorMessage =
          "Error while retrieving schema for subject "
              + subject
              + " with version "
              + version
              + " from the schema registry";
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    return schema;
  }

  @GET
  @Path("/{version}/schema")
  @PerformanceMetric("subjects.versions.get-schema.only")
  public String getSchemaOnly(@PathParam("subject") String subject,
                              @PathParam("version") String version) {
    return getSchema(subject, version).getSchema();
  }

  @GET
  @PerformanceMetric("subjects.versions.list")
  public List<Integer> list(@PathParam("subject") String subject) {
    // check if subject exists. If not, throw 404
    Iterator<Schema> allSchemasForThisTopic = null;
    List<Integer> allVersions = new ArrayList<Integer>();
    String errorMessage = "Error while validating that subject "
        + subject
        + " exists in the registry";
    try {
      if (!schemaRegistry.listSubjects().contains(subject)) {
        throw Errors.subjectNotFoundException();
      }
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    errorMessage = "Error while listing all versions for subject "
        + subject;
    try {
      //return only non-deleted versions for the subject
      allSchemasForThisTopic = schemaRegistry.getAllVersions(subject, false);
    } catch (SchemaRegistryStoreException e) {
      throw Errors.storeException(errorMessage, e);
    } catch (SchemaRegistryException e) {
      throw Errors.schemaRegistryException(errorMessage, e);
    }
    while (allSchemasForThisTopic.hasNext()) {
      Schema schema = allSchemasForThisTopic.next();
      allVersions.add(schema.getVersion());
    }
    return allVersions;
  }

  @POST
  @PerformanceMetric("subjects.versions.register")
  public void register(final @Suspended AsyncResponse asyncResponse,
                       @Context HttpHeaders headers,
                       @PathParam("subject") String subjectName,
                       @NotNull RegisterSchemaRequest request) {

    Map<String, String> headerProperties = requestHeaderBuilder.buildRequestHeaders(headers);

    int schemaId;
    try {
      Schema schema = new Schema(subjectName, 0, 0, request.getSchema());
      schemaId = schemaRegistry.register(subjectName, schema);
    } catch (SchemaRegistryException e) {
      e.printStackTrace();

      throw Errors.storeException("Register schema operation failed while writing"
          + " to the Kafka store", e);
    }
    RegisterSchemaResponse registerSchemaResponse = new RegisterSchemaResponse();
    registerSchemaResponse.setId(schemaId);
    asyncResponse.resume(registerSchemaResponse);
  }

  @DELETE
  @Path("/{version}")
  @PerformanceMetric("subjects.versions.deleteSchemaVersion-schema")
  public void deleteSchemaVersion(final @Suspended AsyncResponse asyncResponse,
                                  @Context HttpHeaders headers,
                                  @PathParam("subject") String subject,
                                  @PathParam("version") String version) {
    VersionId versionId = null;
    try {
      versionId = new VersionId(version);
    } catch (InvalidVersionException e) {
      throw Errors.invalidVersionException();
    }
    Schema schema = null;
    schema = new Schema("test subject", 1, 1, "test schema");
    asyncResponse.resume(schema.getVersion());
  }
}
