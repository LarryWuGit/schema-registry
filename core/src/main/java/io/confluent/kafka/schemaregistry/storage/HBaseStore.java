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

import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreInitializationException;

import java.util.Iterator;
import java.util.Map;

public class HBaseStore<K, V> implements Store<K, V> {
  // Hand out this id during the next schema registration. Indexed from 1.
  private int nextAvailableSchemaId = 1;
  HBaseFacade hbaseFacade;

  //HashMap<K,V> map;
  public HBaseStore() {
  }

  @Override
  public void init() throws StoreInitializationException {
    try {
      hbaseFacade = new HBaseFacade();
    } catch (Exception e) {
      e.printStackTrace();

      throw new StoreInitializationException(e);
    }
  }

  public long getNextAvailableSchemaId() {
    long nextSchemaId = hbaseFacade.incrementAndGetNextAvailableSchemaId();
    return nextSchemaId;
  }

  @Override
  public V get(K key) throws StoreException {
    try {
      return (V) hbaseFacade.getSchemaString((Integer) key);
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  @Override
  public void put(K key, V value) throws StoreException {
    try {
      //hbaseFacade.addSchema((Integer)key, (Schema)value );
      //map.put(key, value);
    } catch (Exception e) {
      throw new StoreException(e);
    }
  }

  @Override
  public Iterator<V> getAll(K key1, K key2) throws StoreException {
    return null;
  }

  @Override
  public void putAll(Map<K, V> entries) throws StoreException {

  }

  @Override
  public void delete(K key) throws StoreException {

  }

  @Override
  public Iterator<K> getAllKeys() throws StoreException {
    return null;
  }

  @Override
  public void close() {

  }
}
