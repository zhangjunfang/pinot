/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.linkedin.pinot.integration.tests;

import com.google.common.util.concurrent.Uninterruptibles;
import com.linkedin.pinot.common.utils.KafkaStarterUtils;
import java.io.File;
import java.util.ArrayList;
import java.util.Collections;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.commons.io.FileUtils;
import org.codehaus.jackson.node.NullNode;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;


/**
 * Test for the deduplication.
 */
public class RealtimeDedupeIntegrationTest extends RealtimeClusterIntegrationTest {
  private static final int EXPECTED_RECORD_COUNT = ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH * 5 +
      ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH / 2;

  private static final File avroFile = new File("RealtimeDedupe.avro");

  protected void startKafka() {
    kafkaStarters =
        KafkaStarterUtils.startServers(getKafkaBrokerCount(), KafkaStarterUtils.DEFAULT_KAFKA_PORT,
            KafkaStarterUtils.DEFAULT_ZK_STR, KafkaStarterUtils.getDefaultKafkaConfiguration());

    KafkaStarterUtils.createTopic(KAFKA_TOPIC, KafkaStarterUtils.DEFAULT_ZK_STR, 1);
  }

  @BeforeClass
  @Override
  public void setUp() throws Exception {
    // Start ZK and Kafka
    startZk();
    startKafka();

    // Start the Pinot cluster
    startController();
    startBroker();
    startServer();

    if (avroFile.exists()) {
      FileUtils.deleteQuietly(avroFile);
    }

    // Generate an Avro file with our schema
    Schema avroSchema = Schema.createRecord("DedupeSchema", "no documentation", "no namespace", false);
    ArrayList<Schema.Field> fields = new ArrayList<>();
    fields.add(new Schema.Field("key", Schema.create(Schema.Type.INT), "the key", NullNode.getInstance()));
    fields.add(new Schema.Field("time", Schema.create(Schema.Type.INT), "the time column", NullNode.getInstance()));
    fields.add(new Schema.Field("myMetric", Schema.create(Schema.Type.INT), "a metric", NullNode.getInstance()));
    fields.add(new Schema.Field("myDimension", Schema.create(Schema.Type.INT), "a dimension", NullNode.getInstance()));
    avroSchema.setFields(fields);

    System.out.println("avroSchema = " + avroSchema);
    GenericDatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<>(avroSchema);
    DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<>(datumWriter);

    dataFileWriter.create(avroSchema, avroFile);
    for (int i = 0; i < EXPECTED_RECORD_COUNT - ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH; i++) {
      generateAndAppendRecord(avroSchema, dataFileWriter, i);
    }

    // Put in a few duplicate records from this segment
    for (int i = EXPECTED_RECORD_COUNT - 10; i < EXPECTED_RECORD_COUNT; i++) {
      generateAndAppendRecord(avroSchema, dataFileWriter, i);
    }

    // Put in a few duplicate records from previous segments
    for (int i = 0; i < 10; i++) {
      generateAndAppendRecord(avroSchema, dataFileWriter, i);
    }

    // Add some records to force flushing the segment
    for (int i = EXPECTED_RECORD_COUNT - ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH; i < EXPECTED_RECORD_COUNT; i++) {
      generateAndAppendRecord(avroSchema, dataFileWriter, i);
    }

    dataFileWriter.close();

    // Write the Avro data into the stream
    ExecutorService executor = Executors.newCachedThreadPool();
    pushAvroIntoKafka(Collections.singletonList(avroFile), executor, KAFKA_TOPIC);
    executor.shutdown();
    executor.awaitTermination(10, TimeUnit.MINUTES);

    // Create the table
    File pinotSchemaFile =
        new File(RealtimeDedupeIntegrationTest.class.getClassLoader().getResource("realtimededupe.schema").getFile());

    // TODO jfim: For now, the key column isn't special in the schema (it's just a regular dimension), maybe we want to treat it in a different way?
    com.linkedin.pinot.common.data.Schema pinotSchema = com.linkedin.pinot.common.data.Schema.fromFile(pinotSchemaFile);

    addSchema(pinotSchemaFile, pinotSchema.getSchemaName());
    addLLCRealtimeTable("mytable", "time", "MILLISECONDS", -1, "", KafkaStarterUtils.DEFAULT_KAFKA_BROKER, KAFKA_TOPIC,
        pinotSchema.getSchemaName(), null, null, avroFile , ROW_COUNT_FOR_REALTIME_SEGMENT_FLUSH, null,
        Collections.<String>emptyList(), "MMAP", "key");

//    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MINUTES);
  }

  private void generateAndAppendRecord(Schema avroSchema, DataFileWriter<GenericRecord> dataFileWriter, int id)
      throws java.io.IOException {
    GenericRecord record = new GenericData.Record(avroSchema);
    record.put("key", id);
    record.put("time", id);
    record.put("myMetric", id);
    record.put("myDimension", id);
    dataFileWriter.append(record);
  }

  @Test
  public void testNoDuplicateRecords() throws Exception {
    // Wait until the record count matches or exceeds the expected record count
    // If the record count exceeds the expected record count, we did not duplicate records correctly, so fail the test
    // If it does not match the expected record count, we timed out, so fail the test
    long timeInFiveMinutes = System.currentTimeMillis() + TimeUnit.MINUTES.toMillis(5);
    waitForRecordCountToStabilizeToExpectedCount(EXPECTED_RECORD_COUNT, timeInFiveMinutes);

    System.out.println("Looks good :)");
//    Uninterruptibles.sleepUninterruptibly(10, TimeUnit.MINUTES);
  }

  @Override
  public void testHardcodedQueries() throws Exception {
    // Do nothing
  }

  @Override
  public void testHardcodedQuerySet() throws Exception {
    // Do nothing
  }

  @Override
  public void testGeneratedQueriesWithoutMultiValues() throws Exception {
    // Do nothing
  }

  @Override
  public void testGeneratedQueriesWithMultiValues() throws Exception {
    // Do nothing
  }

  @Override
  protected void testGeneratedQueries(boolean withMultiValues) throws Exception {
    // Do nothing
  }
}
