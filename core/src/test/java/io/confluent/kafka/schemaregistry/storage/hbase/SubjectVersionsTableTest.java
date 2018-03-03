package io.confluent.kafka.schemaregistry.storage.hbase;

import io.confluent.kafka.schemaregistry.exceptions.InvalidVersionException;
import io.confluent.kafka.schemaregistry.rest.SchemaRegistryConfig;
import io.confluent.kafka.schemaregistry.rest.VersionId;
import io.confluent.kafka.schemaregistry.storage.exceptions.StoreException;
import io.confluent.rest.RestConfigException;
import org.junit.Test;

import java.util.Properties;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.fail;

public class SubjectVersionsTableTest {

  @Test
  public void testSubjectVersionsTable_create() {
    try {

      SubjectVersionsTable subjectVersionsTable = new SubjectVersionsTable(new SchemaRegistryConfig(new Properties()),
              new HbaseTableMock());

    } catch (RestConfigException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSubjectVersionsTable_get() {
    try {
      int schemaId = 456;
      String subject = "testSubject";

      SubjectVersionsTable subjectVersionsTable =
              new SubjectVersionsTable(new SchemaRegistryConfig(new Properties()),
                      new HbaseTableMock());

      SubjectVersionsTable.SubjectVersionsRow subjectVersionRow = new SubjectVersionsTable.SubjectVersionsRow();
      subjectVersionRow.addVersion(schemaId);
      subjectVersionsTable.put(subject, new SubjectVersionsTable.SubjectVersionsRow(subjectVersionRow.toJson()));

      SubjectVersionsTable.SubjectVersionsRow subjectVersionsRow = subjectVersionsTable.get(subject);

      SubjectVersionsTable.SubjectVersion subjectVersion = subjectVersionsRow.getVersion(1);

      assertTrue("Is output not null?", subjectVersionsRow != null);
      assertTrue("Is valid schemaId?", subjectVersion.schemaId == schemaId);
      assertTrue("Is valid version?", subjectVersion.version == 1);

    } catch (RestConfigException | StoreException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSubjectVersionsTable_put() {
    try {
      int schemaId = 456;
      String subject = "testSubject";

      SubjectVersionsTable subjectVersionsTable = new SubjectVersionsTable(new SchemaRegistryConfig(new Properties()),
              new HbaseTableMock());
      SubjectVersionsTable.SubjectVersionsRow subjectVersionRow = new SubjectVersionsTable.SubjectVersionsRow();
      subjectVersionRow.addVersion(schemaId);
      subjectVersionsTable.put(subject, new SubjectVersionsTable.SubjectVersionsRow(subjectVersionRow.toJson()));

      SubjectVersionsTable.SubjectVersion subjectVersion = subjectVersionsTable.getSubjectVersion(subject, new VersionId(1));
      assertTrue("Is valid schemaId?", subjectVersion.schemaId == schemaId);
      assertTrue("Is valid version?", subjectVersion.version == 1);

    } catch (RestConfigException | StoreException | InvalidVersionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSubjectVersionsTable_getSubjectVersion() {
    try {
      int schemaId = 456;
      String subject = "testSubject";

      SubjectVersionsTable subjectVersionsTable = new SubjectVersionsTable(new SchemaRegistryConfig(new Properties()),
              new HbaseTableMock());
      SubjectVersionsTable.SubjectVersionsRow subjectVersionRow = new SubjectVersionsTable.SubjectVersionsRow();
      subjectVersionRow.addVersion(schemaId);
      subjectVersionsTable.put(subject, new SubjectVersionsTable.SubjectVersionsRow(subjectVersionRow.toJson()));

      SubjectVersionsTable.SubjectVersion subjectVersion = subjectVersionsTable.getSubjectVersion(subject, new VersionId(1));
      assertTrue("Is valid schemaId?", subjectVersion.schemaId == schemaId);
      assertTrue("Is valid version?", subjectVersion.version == 1);

    } catch (RestConfigException | StoreException | InvalidVersionException e) {
      e.printStackTrace();
      fail();
    }
  }

  @Test
  public void testSubjectVersionsTable_addNewVersion() {
    try {
      int schemaId1 = 456;
      int schemaId2 = 1347;
      int schemaId3 = 2868;
      String subject = "testSubject";

      SubjectVersionsTable subjectVersionsTable = new SubjectVersionsTable(new SchemaRegistryConfig(new Properties()),
              new HbaseTableMock());

      subjectVersionsTable.addNewVersion(subject, schemaId1);
      subjectVersionsTable.addNewVersion(subject, schemaId2);
      subjectVersionsTable.addNewVersion(subject, schemaId3);

      SubjectVersionsTable.SubjectVersion subjectVersion = subjectVersionsTable.getSubjectVersion(subject, new VersionId(3));
      assertTrue("Is valid schemaId?", subjectVersion.schemaId == schemaId3);
      assertTrue("Is valid version?", subjectVersion.version == 3);

    } catch (RestConfigException | StoreException | InvalidVersionException e) {
      e.printStackTrace();
      fail();
    }
  }
}
