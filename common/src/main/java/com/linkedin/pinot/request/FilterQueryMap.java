/**
 * Autogenerated by Thrift Compiler (0.9.1)
 *
 * DO NOT EDIT UNLESS YOU ARE SURE THAT YOU KNOW WHAT YOU ARE DOING
 *  @generated
 */
package com.linkedin.pinot.request;

import org.apache.thrift.scheme.IScheme;
import org.apache.thrift.scheme.SchemeFactory;
import org.apache.thrift.scheme.StandardScheme;

import org.apache.thrift.scheme.TupleScheme;
import org.apache.thrift.protocol.TTupleProtocol;
import org.apache.thrift.protocol.TProtocolException;
import org.apache.thrift.EncodingUtils;
import org.apache.thrift.TException;
import org.apache.thrift.async.AsyncMethodCallback;
import org.apache.thrift.server.AbstractNonblockingServer.*;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.HashMap;
import java.util.EnumMap;
import java.util.Set;
import java.util.HashSet;
import java.util.EnumSet;
import java.util.Collections;
import java.util.BitSet;
import java.nio.ByteBuffer;
import java.util.Arrays;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filter Query is nested but thrift stable version does not support yet (The support is there in top of the trunk but no released jars. Two concerns : stability and onus of maintaining a stable point. Also, its pretty difficult to compile thrift in Linkedin software development environment which is not geared towards c++ dev. Hence, the )
 * 
 */
public class FilterQueryMap implements org.apache.thrift.TBase<FilterQueryMap, FilterQueryMap._Fields>, java.io.Serializable, Cloneable, Comparable<FilterQueryMap> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("FilterQueryMap");

  private static final org.apache.thrift.protocol.TField FILTER_QUERY_MAP_FIELD_DESC = new org.apache.thrift.protocol.TField("filterQueryMap", org.apache.thrift.protocol.TType.MAP, (short)1);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new FilterQueryMapStandardSchemeFactory());
    schemes.put(TupleScheme.class, new FilterQueryMapTupleSchemeFactory());
  }

  private Map<Integer,FilterQuery> filterQueryMap; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    FILTER_QUERY_MAP((short)1, "filterQueryMap");

    private static final Map<String, _Fields> byName = new HashMap<String, _Fields>();

    static {
      for (_Fields field : EnumSet.allOf(_Fields.class)) {
        byName.put(field.getFieldName(), field);
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, or null if its not found.
     */
    public static _Fields findByThriftId(int fieldId) {
      switch(fieldId) {
        case 1: // FILTER_QUERY_MAP
          return FILTER_QUERY_MAP;
        default:
          return null;
      }
    }

    /**
     * Find the _Fields constant that matches fieldId, throwing an exception
     * if it is not found.
     */
    public static _Fields findByThriftIdOrThrow(int fieldId) {
      _Fields fields = findByThriftId(fieldId);
      if (fields == null) throw new IllegalArgumentException("Field " + fieldId + " doesn't exist!");
      return fields;
    }

    /**
     * Find the _Fields constant that matches name, or null if its not found.
     */
    public static _Fields findByName(String name) {
      return byName.get(name);
    }

    private final short _thriftId;
    private final String _fieldName;

    _Fields(short thriftId, String fieldName) {
      _thriftId = thriftId;
      _fieldName = fieldName;
    }

    public short getThriftFieldId() {
      return _thriftId;
    }

    public String getFieldName() {
      return _fieldName;
    }
  }

  // isset id assignments
  private _Fields optionals[] = {_Fields.FILTER_QUERY_MAP};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.FILTER_QUERY_MAP, new org.apache.thrift.meta_data.FieldMetaData("filterQueryMap", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.MapMetaData(org.apache.thrift.protocol.TType.MAP, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I32), 
            new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, FilterQuery.class))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(FilterQueryMap.class, metaDataMap);
  }

  public FilterQueryMap() {
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public FilterQueryMap(FilterQueryMap other) {
    if (other.isSetFilterQueryMap()) {
      Map<Integer,FilterQuery> __this__filterQueryMap = new HashMap<Integer,FilterQuery>(other.filterQueryMap.size());
      for (Map.Entry<Integer, FilterQuery> other_element : other.filterQueryMap.entrySet()) {

        Integer other_element_key = other_element.getKey();
        FilterQuery other_element_value = other_element.getValue();

        Integer __this__filterQueryMap_copy_key = other_element_key;

        FilterQuery __this__filterQueryMap_copy_value = new FilterQuery(other_element_value);

        __this__filterQueryMap.put(__this__filterQueryMap_copy_key, __this__filterQueryMap_copy_value);
      }
      this.filterQueryMap = __this__filterQueryMap;
    }
  }

  public FilterQueryMap deepCopy() {
    return new FilterQueryMap(this);
  }

  @Override
  public void clear() {
    this.filterQueryMap = null;
  }

  public int getFilterQueryMapSize() {
    return (this.filterQueryMap == null) ? 0 : this.filterQueryMap.size();
  }

  public void putToFilterQueryMap(int key, FilterQuery val) {
    if (this.filterQueryMap == null) {
      this.filterQueryMap = new HashMap<Integer,FilterQuery>();
    }
    this.filterQueryMap.put(key, val);
  }

  public Map<Integer,FilterQuery> getFilterQueryMap() {
    return this.filterQueryMap;
  }

  public void setFilterQueryMap(Map<Integer,FilterQuery> filterQueryMap) {
    this.filterQueryMap = filterQueryMap;
  }

  public void unsetFilterQueryMap() {
    this.filterQueryMap = null;
  }

  /** Returns true if field filterQueryMap is set (has been assigned a value) and false otherwise */
  public boolean isSetFilterQueryMap() {
    return this.filterQueryMap != null;
  }

  public void setFilterQueryMapIsSet(boolean value) {
    if (!value) {
      this.filterQueryMap = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case FILTER_QUERY_MAP:
      if (value == null) {
        unsetFilterQueryMap();
      } else {
        setFilterQueryMap((Map<Integer,FilterQuery>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case FILTER_QUERY_MAP:
      return getFilterQueryMap();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case FILTER_QUERY_MAP:
      return isSetFilterQueryMap();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof FilterQueryMap)
      return this.equals((FilterQueryMap)that);
    return false;
  }

  public boolean equals(FilterQueryMap that) {
    if (that == null)
      return false;

    boolean this_present_filterQueryMap = true && this.isSetFilterQueryMap();
    boolean that_present_filterQueryMap = true && that.isSetFilterQueryMap();
    if (this_present_filterQueryMap || that_present_filterQueryMap) {
      if (!(this_present_filterQueryMap && that_present_filterQueryMap))
        return false;
      if (!this.filterQueryMap.equals(that.filterQueryMap))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(FilterQueryMap other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetFilterQueryMap()).compareTo(other.isSetFilterQueryMap());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetFilterQueryMap()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.filterQueryMap, other.filterQueryMap);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    return 0;
  }

  public _Fields fieldForId(int fieldId) {
    return _Fields.findByThriftId(fieldId);
  }

  public void read(org.apache.thrift.protocol.TProtocol iprot) throws org.apache.thrift.TException {
    schemes.get(iprot.getScheme()).getScheme().read(iprot, this);
  }

  public void write(org.apache.thrift.protocol.TProtocol oprot) throws org.apache.thrift.TException {
    schemes.get(oprot.getScheme()).getScheme().write(oprot, this);
  }

  @Override
  public String toString() {
    StringBuilder sb = new StringBuilder("FilterQueryMap(");
    boolean first = true;

    if (isSetFilterQueryMap()) {
      sb.append("filterQueryMap:");
      if (this.filterQueryMap == null) {
        sb.append("null");
      } else {
        sb.append(this.filterQueryMap);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    // check for sub-struct validity
  }

  private void writeObject(java.io.ObjectOutputStream out) throws java.io.IOException {
    try {
      write(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(out)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private void readObject(java.io.ObjectInputStream in) throws java.io.IOException, ClassNotFoundException {
    try {
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class FilterQueryMapStandardSchemeFactory implements SchemeFactory {
    public FilterQueryMapStandardScheme getScheme() {
      return new FilterQueryMapStandardScheme();
    }
  }

  private static class FilterQueryMapStandardScheme extends StandardScheme<FilterQueryMap> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, FilterQueryMap struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // FILTER_QUERY_MAP
            if (schemeField.type == org.apache.thrift.protocol.TType.MAP) {
              {
                org.apache.thrift.protocol.TMap _map16 = iprot.readMapBegin();
                struct.filterQueryMap = new HashMap<Integer,FilterQuery>(2*_map16.size);
                for (int _i17 = 0; _i17 < _map16.size; ++_i17)
                {
                  int _key18;
                  FilterQuery _val19;
                  _key18 = iprot.readI32();
                  _val19 = new FilterQuery();
                  _val19.read(iprot);
                  struct.filterQueryMap.put(_key18, _val19);
                }
                iprot.readMapEnd();
              }
              struct.setFilterQueryMapIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          default:
            org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
        }
        iprot.readFieldEnd();
      }
      iprot.readStructEnd();
      struct.validate();
    }

    public void write(org.apache.thrift.protocol.TProtocol oprot, FilterQueryMap struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      if (struct.filterQueryMap != null) {
        if (struct.isSetFilterQueryMap()) {
          oprot.writeFieldBegin(FILTER_QUERY_MAP_FIELD_DESC);
          {
            oprot.writeMapBegin(new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.STRUCT, struct.filterQueryMap.size()));
            for (Map.Entry<Integer, FilterQuery> _iter20 : struct.filterQueryMap.entrySet())
            {
              oprot.writeI32(_iter20.getKey());
              _iter20.getValue().write(oprot);
            }
            oprot.writeMapEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class FilterQueryMapTupleSchemeFactory implements SchemeFactory {
    public FilterQueryMapTupleScheme getScheme() {
      return new FilterQueryMapTupleScheme();
    }
  }

  private static class FilterQueryMapTupleScheme extends TupleScheme<FilterQueryMap> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, FilterQueryMap struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      BitSet optionals = new BitSet();
      if (struct.isSetFilterQueryMap()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetFilterQueryMap()) {
        {
          oprot.writeI32(struct.filterQueryMap.size());
          for (Map.Entry<Integer, FilterQuery> _iter21 : struct.filterQueryMap.entrySet())
          {
            oprot.writeI32(_iter21.getKey());
            _iter21.getValue().write(oprot);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, FilterQueryMap struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TMap _map22 = new org.apache.thrift.protocol.TMap(org.apache.thrift.protocol.TType.I32, org.apache.thrift.protocol.TType.STRUCT, iprot.readI32());
          struct.filterQueryMap = new HashMap<Integer,FilterQuery>(2*_map22.size);
          for (int _i23 = 0; _i23 < _map22.size; ++_i23)
          {
            int _key24;
            FilterQuery _val25;
            _key24 = iprot.readI32();
            _val25 = new FilterQuery();
            _val25.read(iprot);
            struct.filterQueryMap.put(_key24, _val25);
          }
        }
        struct.setFilterQueryMapIsSet(true);
      }
    }
  }

}

