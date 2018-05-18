package io.druid.data.input.parquet.tests;

import io.druid.data.input.InputRow;
import io.druid.data.input.parquet.models.TestSuiteEntity;
import io.druid.indexer.HadoopDruidIndexerConfig;
import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.avro.util.Utf8;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapreduce.Job;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertTrue;

/**
 * Created by vrparikh on 5/17/18.
 */
public class GenericRecordAsMapTest {

  @Test
  public void testGenericRecordWithOneField() throws IOException {
    Schema schema = SchemaBuilder.record("test_record")
            .fields()
            .name("field1").type().doubleType().noDefault()
            .endRecord();

    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    genericRecordBuilder.set("field1", 1D);
    GenericData.Record record = genericRecordBuilder.build();

    Map<String, TestSuiteEntity> testSuites = TestSuiteEntity.fromFile(new File(
            "example/parser/test_suit_for_different_events.json"));

    TestSuiteEntity testSuiteEntity = testSuites.get("test_generic_record_builder");
    HadoopDruidIndexerConfig config = testSuiteEntity.getDruidSpec();

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }
  }

  @Test
  public void testGenericRecordWithNesedField() throws IOException {
    Schema schema = SchemaBuilder.record("test_record")
            .fields()
            .name("field1").type()
            .record("field1_record")
            .fields()
            .name("field2").type().doubleType().noDefault()
            .endRecord().noDefault()
            .endRecord();

    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);

    GenericRecordBuilder childRecordBuilder = new GenericRecordBuilder(schema.getField("field1").schema());
    childRecordBuilder.set("field2", 1D);
    GenericData.Record childRecord = childRecordBuilder.build();

    genericRecordBuilder.set("field1", childRecord);
    GenericData.Record record = genericRecordBuilder.build();

    Map<String, TestSuiteEntity> testSuites = TestSuiteEntity.fromFile(new File(
            "example/parser/test_suit_for_different_events.json"));

    TestSuiteEntity testSuiteEntity = testSuites.get("test_generic_record_builder_nested");
    HadoopDruidIndexerConfig config = testSuiteEntity.getDruidSpec();

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }

    assertTrue(row.getDimension("field1").equals(Collections.singletonList("{\"field2\": 1.0}")));
    assertTrue(row.getDimension("field2").equals(Collections.singletonList("1.0")));
  }

  @Test
  public void testUnionType() throws IOException {
    Schema schema = SchemaBuilder.record("test_record")
            .fields()
            .name("field1").type().unionOf().stringType().and().intType().endUnion().noDefault()
            .endRecord();

    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    genericRecordBuilder.set("field1", "1");
    GenericData.Record record = genericRecordBuilder.build();

    Map<String, TestSuiteEntity> testSuites = TestSuiteEntity.fromFile(new File(
            "example/parser/test_suit_for_different_events.json"));

    TestSuiteEntity testSuiteEntity = testSuites.get("test_union_type");
    HadoopDruidIndexerConfig config = testSuiteEntity.getDruidSpec();

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }

    assertTrue(row.getDimension("field1").equals(Collections.singletonList("1")));
  }

  @Test
  public void testUnionTypeWithMap() throws IOException {
    Schema schema = SchemaBuilder.record("test_record")
            .fields()
            .name("field1").type().unionOf().stringType().and().map().values().intType().endUnion().noDefault()
            .endRecord();

    GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(schema);
    Map<Utf8, Integer> map = new HashMap<>();
    map.put(new Utf8("map_key"), 1);
    genericRecordBuilder.set("field1", map);
    GenericData.Record record = genericRecordBuilder.build();

    Map<String, TestSuiteEntity> testSuites = TestSuiteEntity.fromFile(new File(
            "example/parser/test_suit_for_different_events.json"));

    TestSuiteEntity testSuiteEntity = testSuites.get("test_union_type_with_map");
    HadoopDruidIndexerConfig config = testSuiteEntity.getDruidSpec();

    Job job = Job.getInstance(new Configuration());
    config.intoConfiguration(job);

    InputRow row = ((List<InputRow>) config.getParser().parseBatch(record)).get(0);

    for (String dimension : row.getDimensions()) {
      System.out.println(dimension + " -> " + row.getDimension(dimension));
    }

    assertTrue(row.getDimension("field1").equals(Collections.singletonList("1")));
  }
}