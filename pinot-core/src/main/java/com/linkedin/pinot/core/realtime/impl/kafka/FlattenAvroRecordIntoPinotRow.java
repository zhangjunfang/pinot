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

package com.linkedin.pinot.core.realtime.impl.kafka;

import com.google.common.hash.HashCode;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hasher;
import com.google.common.hash.Hashing;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.indexsegment.utils.AvroUtils;
import java.io.File;
import java.util.concurrent.TimeUnit;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.DatumReader;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Flatten an Avro generic record into a Pinot GenericRow
 */
public class FlattenAvroRecordIntoPinotRow {
  private static final Logger LOGGER = LoggerFactory.getLogger(FlattenAvroRecordIntoPinotRow.class);

  private HashFunction _hashFunction = Hashing.sipHash24();
  private static final String SEPARATOR = "__";

  public GenericRow transform(GenericData.Record record, org.apache.avro.Schema schema, GenericRow destination) {
    flattenRecordIntoRow(record, schema, destination, "");

    return destination;
  }

  private void flattenRecordIntoRow(GenericData.Record record, org.apache.avro.Schema schema, GenericRow destination, String columnPrefix) {
    for (Schema.Field field : schema.getFields()) {
      String fieldName = field.name();
      Schema.Type fieldType = field.schema().getType();

      Object value = null;

      if (record != null) {
        value = record.get(fieldName);
      }

      flattenField(destination, columnPrefix, field, fieldName, fieldType, value);
    }
  }

  private void flattenField(GenericRow destination, String columnPrefix, Schema.Field field, String fieldName,
      Schema.Type fieldType, Object value) {
    if (value == null && fieldType != Schema.Type.RECORD && fieldType != Schema.Type.UNION) {
      destination.putField(columnPrefix + fieldName, null);
      return;
    }

    switch (fieldType) {
      case RECORD:
        flattenRecordIntoRow((GenericData.Record) value, field.schema(), destination, columnPrefix + fieldName + SEPARATOR);
        break;
      case ENUM:
      case STRING:
        String stringValue = value.toString();

        if (1024 < stringValue.length()) {
          LOGGER.warn("Truncating string of length {} to 1024 characters", stringValue.length());
          stringValue = stringValue.substring(0, 1024);
        }
        
        destination.putField(columnPrefix + fieldName, stringValue);
        break;
      case ARRAY:
        break;
      case MAP:
        break;
      case UNION:
        // Find first non-null type and flatten it
        // HACK jfim This is really ugly
        for (Schema schema : field.schema().getTypes()) {
          if (schema.getType() == Schema.Type.RECORD) {
            flattenRecordIntoRow((GenericData.Record) value, schema, destination, columnPrefix + fieldName + SEPARATOR);
            break;
          } else if (schema.getType() != Schema.Type.NULL) {
            flattenField(destination, columnPrefix, null, fieldName, schema.getType(), value);
            break;
          }
        }
        break;
      case FIXED:
        // HACK jfim For now just turn all fixed types into a 64-bit hash (eg. for GUIDs)
        GenericData.Fixed fixedValue = (GenericData.Fixed) value;
        HashCode hashCode = _hashFunction.newHasher().putBytes(fixedValue.bytes()).hash();
        long fixedHash = hashCode.asLong();
        destination.putField(columnPrefix + fieldName, fixedHash);
        break;
      case BYTES:
        break;
      case INT:
      case LONG:
      case FLOAT:
      case DOUBLE:
      case BOOLEAN:
        destination.putField(columnPrefix + fieldName, value);
        break;
      case NULL:
        break;
    }
  }

  public static void main(String[] args) throws Exception {
    DatumReader<GenericRecord> datumReader = new GenericDatumReader<>();
    File file = new File("/Users/jfim/work/PageViewEvent-part-r-5359753.1486116408692.544450217.avro");
    DataFileReader<GenericRecord> dataFileReader = new DataFileReader<GenericRecord>(
        file, datumReader);
    FlattenAvroRecordIntoPinotRow flattener = new FlattenAvroRecordIntoPinotRow();

    AvroFlattener avroSchemaFlattener = new AvroFlattener();
    Schema flatAvroSchema = avroSchemaFlattener.flatten(dataFileReader.getSchema(), true);
    System.out.println("avroSchemaFlattener.flatten(dataFileReader.getSchema(), true) = " + flatAvroSchema);
    com.linkedin.pinot.common.data.Schema pinotSchema = AvroUtils.getPinotSchemaFromAvroSchema(flatAvroSchema, AvroUtils.getDefaultFieldTypes(flatAvroSchema), TimeUnit.MILLISECONDS);
    System.out.println("pinotSchema = " + pinotSchema);

    int rowCount = 0;
    while(dataFileReader.hasNext()) {
      GenericRecord record = dataFileReader.next();
      //GenericRow genericRow = flattener.transform((GenericData.Record) record, dataFileReader.getSchema(), new GenericRow());
      // System.out.println("genericRow = " + genericRow);
      rowCount++;
    }
    System.out.println("rowCount = " + rowCount);
    System.out.println("file.length() = " + file.length());
  }
}
