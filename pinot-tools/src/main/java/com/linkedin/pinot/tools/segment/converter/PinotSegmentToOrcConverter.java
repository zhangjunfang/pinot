package com.linkedin.pinot.tools.segment.converter;

import com.google.common.base.Preconditions;
import com.linkedin.pinot.common.data.FieldSpec;
import com.linkedin.pinot.common.data.Schema;
import com.linkedin.pinot.core.data.GenericRow;
import com.linkedin.pinot.core.data.readers.PinotSegmentRecordReader;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.orc.CompressionKind;
import org.apache.hadoop.hive.ql.io.orc.OrcFile;
import org.apache.hadoop.hive.ql.io.orc.OrcStruct;
import org.apache.hadoop.hive.ql.io.orc.Reader;
import org.apache.hadoop.hive.ql.io.orc.RecordReader;
import org.apache.hadoop.hive.ql.io.orc.Writer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.Text;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class PinotSegmentToOrcConverter implements PinotSegmentConverter {
  private static final Logger LOGGER = LoggerFactory.getLogger(PinotSegmentToOrcConverter.class);
  private final File outputFile;
  private final File segmentDir;

  PinotSegmentToOrcConverter(String segmentDir, String outputFile) {
    Preconditions.checkNotNull(segmentDir);
    Preconditions.checkNotNull(outputFile);
    this.segmentDir = new File(segmentDir);
    this.outputFile = new File(outputFile);
    Preconditions.checkArgument(this.segmentDir.exists(), "Input segmentDir %s does not exist", segmentDir);
    //Preconditions.checkArgument(!this.outputFile.exists(), "Output file: %s already exists", outputFile);
  }

  @Override
  public void convert() throws Exception {
    FileSystem fs = FileSystem.getLocal(new Configuration());
    if (!outputFile.getParentFile().exists()) {
      outputFile.getParentFile().mkdirs();
    }
    PinotSegmentRecordReader reader = new PinotSegmentRecordReader(segmentDir);
    Schema schema = reader.getSchema();
    createTableStatement(schema);
    Writer writer = initOrcWriter(schema);
    reader.init();
    while (reader.hasNext()) {
      GenericRow row = reader.next();
      String[] fields = row.getFieldNames();
      OrcRow orcRow = new OrcRow(fields.length);
      int offset = 0;
      for (String field : fields) {
        Object val = row.getValue(field);
        if (val instanceof Object[]) {
          orcRow.setFieldValue(offset, toStringList(val));
        } else {
          orcRow.setFieldValue(offset, val);
        }
        offset++;
      }
      try {
        writer.addRow(orcRow);
      } catch (Exception e) {
        Object[] fspecs = schema.getAllFieldSpecs().toArray();
        int pos = offset - 1;
        System.out.println("Failed to add row: " + row + ", offset: " + pos + ", val: " + fields[pos]
            + ", type: " + fspecs[pos]);
        throw e;
      }
    }
    writer.close();

  }

  private List<Text> toStringList(Object val) {
    List<Object> values = Arrays.asList((Object[]) val);
    List<Text> slist = new ArrayList<>(values.size());
    for (Object value : values) {
      slist.add(new Text((String)value));
    }
    return slist;
  }

  public void toCsv(String input) throws IOException {
    Preconditions.checkArgument(new File(input).exists(), "Input does not exist");
    FileSystem fs = FileSystem.getLocal(new Configuration());
    Reader reader = OrcFile.createReader(fs, new Path(input));
    StructObjectInspector soi = (StructObjectInspector) reader.getObjectInspector();
    List<? extends StructField> sref = soi.getAllStructFieldRefs();
    for (StructField sfield : sref) {
      System.out.println(sfield.getFieldName() + ":" + sfield.getFieldObjectInspector().getTypeName());
    }

    RecordReader rows = reader.rows();
    while (rows.hasNext()) {
      Object obj = null;
      obj = rows.next(obj);
      List<Object> fields = soi.getStructFieldsDataAsList(obj);
      for (Object field : fields) {
        System.out.print(field == null ? field : field.toString() + " , ");
      }
      System.out.println("");
    }
  }
  private void createTableStatement(Schema schema) {
    Collection<FieldSpec> fields = schema.getAllFieldSpecs();
    boolean isFirst = true;
    StringBuilder sb = new StringBuilder("CREATE TABLE ").append(schema.getSchemaName())
        .append(" (");
    for (FieldSpec field : fields) {
      if (!isFirst) {
        sb.append(",");
      } else {
        isFirst = false;
      }
      sb.append("\n\t").append(field.getName()).append(" ").append(hiveDataType(field));
    }
    sb.append(")");
    System.out.println(sb.toString());
  }

  private String hiveDataType(FieldSpec fieldSpec) {
    switch (fieldSpec.getDataType()) {
      case BOOLEAN:
        return fieldSpec.isSingleValueField() ? "boolean" : "ARRAY boolean";
      case BYTE:
        return fieldSpec.isSingleValueField() ? "tinyint" : "ARRAY tinyint";
      case CHAR:
        return fieldSpec.isSingleValueField() ?  "char" : "ARRAY CHAR";
      case SHORT:
        return fieldSpec.isSingleValueField() ? "smallint" : "ARRAY smallint";
      case INT:
        return fieldSpec.isSingleValueField() ? "int" : "ARRAY int";
      case LONG:
        return fieldSpec.isSingleValueField() ? "bigint" : "ARRAY bigint";
      case FLOAT:
        return fieldSpec.isSingleValueField() ? "float" : "ARRAY float";
      case DOUBLE:
        return fieldSpec.isSingleValueField() ? "double" : "ARRAY double";
      case STRING:
        return fieldSpec.isSingleValueField() ? "string" : "ARRAY string";
      case OBJECT:
        throw new RuntimeException("Object not supported");
      default:
        throw new RuntimeException("Invalid data type");
    }
  }

  private Writer initOrcWriter(Schema schema) throws IOException {
    OrcField[] fields = new OrcField[schema.getAllFieldSpecs().size()];
    Collection<FieldSpec> fieldSpecs = schema.getAllFieldSpecs();
    int offset = 0;
    for (FieldSpec fieldSpec : fieldSpecs) {
      ObjectInspector oi = getObjectInspector(fieldSpec);
      System.out.println(fieldSpec.getName() + ":" + fieldSpec.getDataType() + ":" + oi.getTypeName());
      fields[offset] = new OrcField(fieldSpec.getName(), oi, offset);
      offset++;
    }

    ObjectInspector oi = new OrcRowInspector(fields);
    OrcFile.WriterOptions writerOptions = OrcFile.writerOptions(new Configuration())
        .fileSystem(FileSystem.getLocal(new Configuration()))
        .inspector(oi)
        .stripeSize(100000)
        .bufferSize(10000)
        .compress(CompressionKind.SNAPPY)
        .version(OrcFile.Version.V_0_12);
    Writer w;
    try {
      w = OrcFile.createWriter(new Path(outputFile.toString()), writerOptions);
    } catch (IOException e) {
      throw new RuntimeException("Failed to create orc writer");
    }
    return w;
  }

  private ObjectInspector getObjectInspector(FieldSpec fspec) {
    if (fspec.isSingleValueField()) {
      switch (fspec.getDataType()) {
        case INT:
          return PrimitiveObjectInspectorFactory.javaIntObjectInspector;
        case CHAR:
          return PrimitiveObjectInspectorFactory.writableHiveCharObjectInspector;
        case BOOLEAN:
          return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
        case BYTE:
          return PrimitiveObjectInspectorFactory.writableByteObjectInspector;
        case LONG:
          return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
        case FLOAT:
          return PrimitiveObjectInspectorFactory.writableFloatObjectInspector;
        case DOUBLE:
          return PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
        case STRING:
          //return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
          return ObjectInspectorFactory.getReflectionObjectInspector(String.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        case SHORT:
          return PrimitiveObjectInspectorFactory.writableShortObjectInspector;
        default:
          throw new RuntimeException("Unknown type " + fspec);
      }
    } else {
      switch (fspec.getDataType()) {
        case BYTE:
          System.out.println("byte array");
          return PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
        case CHAR:
          //char[] c = new char[1];
          List<Character> c = new ArrayList<>();
          System.out.println(c.getClass());
          return ObjectInspectorFactory.getReflectionObjectInspector(c.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        case INT:
          List<Integer> i = new ArrayList<>();
          //int[] i = new int[1];
          System.out.println(i.getClass());
          return ObjectInspectorFactory.getReflectionObjectInspector(i.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        case LONG:
          //long[] l = new long[1];
          List<Long> l = new ArrayList<>();
          System.out.println(l.getClass());
          return ObjectInspectorFactory.getReflectionObjectInspector(l.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        case FLOAT:
          //float[] f = new float[1];
          List<Float> f = new ArrayList<>();
          System.out.println(f.getClass());
          return ObjectInspectorFactory.getReflectionObjectInspector(f.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        case DOUBLE:
          //double[] d = new double[1];
          List<Double> d = new ArrayList<>();
          System.out.println(d.getClass());
          return ObjectInspectorFactory.getReflectionObjectInspector(d.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        case STRING:
          //String[] s = new String[1];
          List<String> s = new ArrayList<>();
          System.out.println(s.getClass());
          return OrcStruct.createObjectInspector(TypeInfoUtils.getTypeInfoFromTypeString("array<string>"));
          //return ObjectInspectorFactory.getReflectionObjectInspector(s.getClass(), ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
        default:
          throw new RuntimeException("Unknown type: " + fspec);
      }
    }
  }

  public static class OrcRow {
    public Object[] columns;

    OrcRow(int colCount) {
      columns = new Object[colCount];
    }

    void setFieldValue(int FieldIndex, Object value) {
      columns[FieldIndex] = value;
    }

    void setNumFields(int newSize) {
      if (newSize != columns.length) {
        Object[] oldColumns = columns;
        columns = new Object[newSize];
        System.arraycopy(oldColumns, 0, columns, 0, oldColumns.length);
      }
    }
  }

  static class OrcField implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int offset;

    OrcField(String name, ObjectInspector inspector, int offset) {
      this.name = name;
      this.inspector = inspector;
      this.offset = offset;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public int getFieldID() {
      return offset;
    }

    @Override
    public String getFieldComment() {
      return null;
    }
  }

  static class OrcRowInspector extends SettableStructObjectInspector {
    private List<StructField> fields;

    public OrcRowInspector(StructField... fields) {
      super();
      this.fields = Arrays.asList(fields);
    }

    @Override
    public List<StructField> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String s) {
      for (StructField field : fields) {
        if (field.getFieldName().equalsIgnoreCase(s)) {
          return field;
        }
      }
      return null;
    }

    @Override
    public Object getStructFieldData(Object object, StructField field) {
      if (object == null) {
        return null;
      }
      int offset = ((OrcField) field).offset;
      OrcRow struct = (OrcRow) object;
      if (offset >= struct.columns.length) {
        return null;
      }

      return struct.columns[offset];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object object) {
      if (object == null) {
        return null;
      }
      OrcRow struct = (OrcRow) object;
      List<Object> result = new ArrayList<Object>(struct.columns.length);
      for (Object child : struct.columns) {
        result.add(child);
      }
      return result;
    }

    @Override
    public String getTypeName() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("struct<");
      for (int i = 0; i < fields.size(); ++i) {
        StructField field = fields.get(i);
        if (i != 0) {
          buffer.append(",");
        }
        buffer.append(field.getFieldName());
        buffer.append(":");
        buffer.append(field.getFieldObjectInspector().getTypeName());
      }
      buffer.append(">");
      return buffer.toString();
    }

    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }

    @Override
    public Object create() {
      return new OrcRow(0);
    }

    @Override
    public Object setStructFieldData(Object struct, StructField field, Object fieldValue) {
      OrcRow orcStruct = (OrcRow) struct;
      int offset = ((OrcField) field).offset;
      // if the offset is bigger than our current number of fields, grow it
      if (orcStruct.columns.length <= offset) {
        orcStruct.setNumFields(offset + 1);
      }
      orcStruct.setFieldValue(offset, fieldValue);
      return struct;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        List<StructField> other = ((OrcRowInspector) o).fields;
        if (other.size() != fields.size()) {
          return false;
        }
        for (int i = 0; i < fields.size(); ++i) {
          StructField left = other.get(i);
          StructField right = fields.get(i);
          if (!(left.getFieldName().equalsIgnoreCase(right.getFieldName()) && left.getFieldObjectInspector().equals(right.getFieldObjectInspector()))) {
            return false;
          }
        }
        return true;
      }
    }
  }

  public static void main(String[] args) throws Exception {
    String dest = "/home/atumbde/debug/b11.orc";
    File destFile = new File(dest);
    if (destFile.exists()) {
      destFile.delete();
    }
    PinotSegmentToOrcConverter converter = new PinotSegmentToOrcConverter(
        "/home/atumbde/debug/bid-suggest/bidSuggest_OFFLINE/bidSuggest_bidSuggest_correlation_3_11", dest);
    String tinfo = "array<string>";
    ObjectInspector oi = OrcStruct.createObjectInspector(TypeInfoUtils.getTypeInfoFromTypeString(tinfo));
    System.out.println(oi.getTypeName());
    System.out.println(oi.getCategory());
    converter.convert();
    System.out.println("Finished conversion to orc");
    System.in.read();
    converter.toCsv(dest);
  }
}
