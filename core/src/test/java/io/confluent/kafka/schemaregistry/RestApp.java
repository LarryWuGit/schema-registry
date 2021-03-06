/**
 * Copyright 2014 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package io.confluent.kafka.schemaregistry;


import org.eclipse.jetty.server.Server;

import java.util.Properties;

import io.confluent.kafka.schemaregistry.avro.AvroCompatibilityLevel;
import io.confluent.kafka.schemaregistry.client.rest.RestService;
import io.confluent.kafka.schemaregistry.exceptions.SchemaRegistryException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryRestApplication;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistry;
import io.confluent.kafka.schemaregistry.storage.SchemaRegistryIdentity;

public class RestApp {

  public final Properties prop;
  public RestService restClient;
  public SchemaRegistryRestApplication restApp;
  public Server restServer;
  public String restConnect;

  public RestApp(int port, String zkConnect, String kafkaTopic) {
    this(port, zkConnect, kafkaTopic, AvroCompatibilityLevel.NONE.name, null);
  }

  public RestApp(int port, String zkConnect, String kafkaTopic, String compatibilityType, Properties schemaRegistryProps) {
    this(port, zkConnect, null, kafkaTopic, compatibilityType, true, schemaRegistryProps);
  }

  public RestApp(int port,
                 String zkConnect, String kafkaTopic,
                 String compatibilityType, boolean masterEligibility, Properties schemaRegistryProps) {
    this(port, zkConnect, null, kafkaTopic, compatibilityType,
         masterEligibility, schemaRegistryProps);
  }

  public RestApp(int port,
                 String zkConnect, String bootstrapBrokers,
                 String kafkaTopic, String compatibilityType, boolean masterEligibility,
                 Properties schemaRegistryProps) {
    prop = new Properties();
    if (schemaRegistryProps != null) {
      prop.putAll(schemaRegistryProps);
    }
    prop.setProperty(SchemaRegistryConfig.PORT_CONFIG, ((Integer) port).toString());
    if (zkConnect != null) {
      prop.setProperty(SchemaRegistryConfig.KAFKASTORE_CONNECTION_URL_CONFIG, zkConnect);
    }
    if (bootstrapBrokers != null) {
      prop.setProperty(SchemaRegistryConfig.KAFKASTORE_BOOTSTRAP_SERVERS_CONFIG, bootstrapBrokers);
    }
    prop.put(SchemaRegistryConfig.KAFKASTORE_TOPIC_CONFIG, kafkaTopic);
    prop.put(SchemaRegistryConfig.COMPATIBILITY_CONFIG, compatibilityType);
    prop.put(SchemaRegistryConfig.MASTER_ELIGIBILITY, masterEligibility);
  }

  public void start() throws Exception {
    restApp = new SchemaRegistryRestApplication(prop);
    restServer = restApp.createServer();
    restServer.start();
    restConnect = restServer.getURI().toString();
    if (restConnect.endsWith("/"))
      restConnect = restConnect.substring(0, restConnect.length()-1);
    restClient = new RestService(restConnect);
  }

  public void stop() throws Exception {
    restClient = null;
    if (restServer != null) {
      restServer.stop();
      restServer.join();
    }
  }
}
