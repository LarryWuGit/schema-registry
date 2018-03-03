package io.confluent.kafka.schemaregistry.storage.hbase;

import com.google.protobuf.ServiceException;
import io.confluent.kafka.schemaregistry.storage.hbase.HbaseTable;
import org.apache.hadoop.conf.Configuration;

import java.io.IOException;
import java.util.HashMap;

public class HbaseTableMock extends HbaseTable {

  HashMap<String, String> map = new HashMap<>();

  int counter = 0;

  public HbaseTableMock() {
    super(new Configuration(), "testtable");

  }

  public byte[] get(String familyName, String columnName, String key)
          throws IOException {
    if (map.get(key) != null) {
      return map.get(key).getBytes();
    }

    return null;
  }

  public void put(String familyName, String columnName, String key, String value)
          throws IOException {
    map.put(key, value);
  }

  public void checkHBaseAvailable(Configuration hbaseConfig)
          throws ServiceException, IOException {
    // No exception mean Success.
  }

  public int incrementAndGet(String familyName, String columnName, String key)
          throws IOException {
    return ++counter;
  }
}
