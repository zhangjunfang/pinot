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
 * Request
 * 
 */
public class InstanceRequest implements org.apache.thrift.TBase<InstanceRequest, InstanceRequest._Fields>, java.io.Serializable, Cloneable, Comparable<InstanceRequest> {
  private static final org.apache.thrift.protocol.TStruct STRUCT_DESC = new org.apache.thrift.protocol.TStruct("InstanceRequest");

  private static final org.apache.thrift.protocol.TField REQUEST_ID_FIELD_DESC = new org.apache.thrift.protocol.TField("requestId", org.apache.thrift.protocol.TType.I64, (short)1);
  private static final org.apache.thrift.protocol.TField QUERY_FIELD_DESC = new org.apache.thrift.protocol.TField("query", org.apache.thrift.protocol.TType.STRUCT, (short)2);
  private static final org.apache.thrift.protocol.TField SEARCH_PARTITIONS_FIELD_DESC = new org.apache.thrift.protocol.TField("searchPartitions", org.apache.thrift.protocol.TType.LIST, (short)3);

  private static final Map<Class<? extends IScheme>, SchemeFactory> schemes = new HashMap<Class<? extends IScheme>, SchemeFactory>();
  static {
    schemes.put(StandardScheme.class, new InstanceRequestStandardSchemeFactory());
    schemes.put(TupleScheme.class, new InstanceRequestTupleSchemeFactory());
  }

  private long requestId; // required
  private BrokerRequest query; // required
  private List<Long> searchPartitions; // optional

  /** The set of fields this struct contains, along with convenience methods for finding and manipulating them. */
  public enum _Fields implements org.apache.thrift.TFieldIdEnum {
    REQUEST_ID((short)1, "requestId"),
    QUERY((short)2, "query"),
    SEARCH_PARTITIONS((short)3, "searchPartitions");

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
        case 1: // REQUEST_ID
          return REQUEST_ID;
        case 2: // QUERY
          return QUERY;
        case 3: // SEARCH_PARTITIONS
          return SEARCH_PARTITIONS;
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
  private static final int __REQUESTID_ISSET_ID = 0;
  private byte __isset_bitfield = 0;
  private _Fields optionals[] = {_Fields.SEARCH_PARTITIONS};
  public static final Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> metaDataMap;
  static {
    Map<_Fields, org.apache.thrift.meta_data.FieldMetaData> tmpMap = new EnumMap<_Fields, org.apache.thrift.meta_data.FieldMetaData>(_Fields.class);
    tmpMap.put(_Fields.REQUEST_ID, new org.apache.thrift.meta_data.FieldMetaData("requestId", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64)));
    tmpMap.put(_Fields.QUERY, new org.apache.thrift.meta_data.FieldMetaData("query", org.apache.thrift.TFieldRequirementType.REQUIRED, 
        new org.apache.thrift.meta_data.StructMetaData(org.apache.thrift.protocol.TType.STRUCT, BrokerRequest.class)));
    tmpMap.put(_Fields.SEARCH_PARTITIONS, new org.apache.thrift.meta_data.FieldMetaData("searchPartitions", org.apache.thrift.TFieldRequirementType.OPTIONAL, 
        new org.apache.thrift.meta_data.ListMetaData(org.apache.thrift.protocol.TType.LIST, 
            new org.apache.thrift.meta_data.FieldValueMetaData(org.apache.thrift.protocol.TType.I64))));
    metaDataMap = Collections.unmodifiableMap(tmpMap);
    org.apache.thrift.meta_data.FieldMetaData.addStructMetaDataMap(InstanceRequest.class, metaDataMap);
  }

  public InstanceRequest() {
  }

  public InstanceRequest(
    long requestId,
    BrokerRequest query)
  {
    this();
    this.requestId = requestId;
    setRequestIdIsSet(true);
    this.query = query;
  }

  /**
   * Performs a deep copy on <i>other</i>.
   */
  public InstanceRequest(InstanceRequest other) {
    __isset_bitfield = other.__isset_bitfield;
    this.requestId = other.requestId;
    if (other.isSetQuery()) {
      this.query = new BrokerRequest(other.query);
    }
    if (other.isSetSearchPartitions()) {
      List<Long> __this__searchPartitions = new ArrayList<Long>(other.searchPartitions);
      this.searchPartitions = __this__searchPartitions;
    }
  }

  public InstanceRequest deepCopy() {
    return new InstanceRequest(this);
  }

  @Override
  public void clear() {
    setRequestIdIsSet(false);
    this.requestId = 0;
    this.query = null;
    this.searchPartitions = null;
  }

  public long getRequestId() {
    return this.requestId;
  }

  public void setRequestId(long requestId) {
    this.requestId = requestId;
    setRequestIdIsSet(true);
  }

  public void unsetRequestId() {
    __isset_bitfield = EncodingUtils.clearBit(__isset_bitfield, __REQUESTID_ISSET_ID);
  }

  /** Returns true if field requestId is set (has been assigned a value) and false otherwise */
  public boolean isSetRequestId() {
    return EncodingUtils.testBit(__isset_bitfield, __REQUESTID_ISSET_ID);
  }

  public void setRequestIdIsSet(boolean value) {
    __isset_bitfield = EncodingUtils.setBit(__isset_bitfield, __REQUESTID_ISSET_ID, value);
  }

  public BrokerRequest getQuery() {
    return this.query;
  }

  public void setQuery(BrokerRequest query) {
    this.query = query;
  }

  public void unsetQuery() {
    this.query = null;
  }

  /** Returns true if field query is set (has been assigned a value) and false otherwise */
  public boolean isSetQuery() {
    return this.query != null;
  }

  public void setQueryIsSet(boolean value) {
    if (!value) {
      this.query = null;
    }
  }

  public int getSearchPartitionsSize() {
    return (this.searchPartitions == null) ? 0 : this.searchPartitions.size();
  }

  public java.util.Iterator<Long> getSearchPartitionsIterator() {
    return (this.searchPartitions == null) ? null : this.searchPartitions.iterator();
  }

  public void addToSearchPartitions(long elem) {
    if (this.searchPartitions == null) {
      this.searchPartitions = new ArrayList<Long>();
    }
    this.searchPartitions.add(elem);
  }

  public List<Long> getSearchPartitions() {
    return this.searchPartitions;
  }

  public void setSearchPartitions(List<Long> searchPartitions) {
    this.searchPartitions = searchPartitions;
  }

  public void unsetSearchPartitions() {
    this.searchPartitions = null;
  }

  /** Returns true if field searchPartitions is set (has been assigned a value) and false otherwise */
  public boolean isSetSearchPartitions() {
    return this.searchPartitions != null;
  }

  public void setSearchPartitionsIsSet(boolean value) {
    if (!value) {
      this.searchPartitions = null;
    }
  }

  public void setFieldValue(_Fields field, Object value) {
    switch (field) {
    case REQUEST_ID:
      if (value == null) {
        unsetRequestId();
      } else {
        setRequestId((Long)value);
      }
      break;

    case QUERY:
      if (value == null) {
        unsetQuery();
      } else {
        setQuery((BrokerRequest)value);
      }
      break;

    case SEARCH_PARTITIONS:
      if (value == null) {
        unsetSearchPartitions();
      } else {
        setSearchPartitions((List<Long>)value);
      }
      break;

    }
  }

  public Object getFieldValue(_Fields field) {
    switch (field) {
    case REQUEST_ID:
      return Long.valueOf(getRequestId());

    case QUERY:
      return getQuery();

    case SEARCH_PARTITIONS:
      return getSearchPartitions();

    }
    throw new IllegalStateException();
  }

  /** Returns true if field corresponding to fieldID is set (has been assigned a value) and false otherwise */
  public boolean isSet(_Fields field) {
    if (field == null) {
      throw new IllegalArgumentException();
    }

    switch (field) {
    case REQUEST_ID:
      return isSetRequestId();
    case QUERY:
      return isSetQuery();
    case SEARCH_PARTITIONS:
      return isSetSearchPartitions();
    }
    throw new IllegalStateException();
  }

  @Override
  public boolean equals(Object that) {
    if (that == null)
      return false;
    if (that instanceof InstanceRequest)
      return this.equals((InstanceRequest)that);
    return false;
  }

  public boolean equals(InstanceRequest that) {
    if (that == null)
      return false;

    boolean this_present_requestId = true;
    boolean that_present_requestId = true;
    if (this_present_requestId || that_present_requestId) {
      if (!(this_present_requestId && that_present_requestId))
        return false;
      if (this.requestId != that.requestId)
        return false;
    }

    boolean this_present_query = true && this.isSetQuery();
    boolean that_present_query = true && that.isSetQuery();
    if (this_present_query || that_present_query) {
      if (!(this_present_query && that_present_query))
        return false;
      if (!this.query.equals(that.query))
        return false;
    }

    boolean this_present_searchPartitions = true && this.isSetSearchPartitions();
    boolean that_present_searchPartitions = true && that.isSetSearchPartitions();
    if (this_present_searchPartitions || that_present_searchPartitions) {
      if (!(this_present_searchPartitions && that_present_searchPartitions))
        return false;
      if (!this.searchPartitions.equals(that.searchPartitions))
        return false;
    }

    return true;
  }

  @Override
  public int hashCode() {
    return 0;
  }

  @Override
  public int compareTo(InstanceRequest other) {
    if (!getClass().equals(other.getClass())) {
      return getClass().getName().compareTo(other.getClass().getName());
    }

    int lastComparison = 0;

    lastComparison = Boolean.valueOf(isSetRequestId()).compareTo(other.isSetRequestId());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetRequestId()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.requestId, other.requestId);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetQuery()).compareTo(other.isSetQuery());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetQuery()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.query, other.query);
      if (lastComparison != 0) {
        return lastComparison;
      }
    }
    lastComparison = Boolean.valueOf(isSetSearchPartitions()).compareTo(other.isSetSearchPartitions());
    if (lastComparison != 0) {
      return lastComparison;
    }
    if (isSetSearchPartitions()) {
      lastComparison = org.apache.thrift.TBaseHelper.compareTo(this.searchPartitions, other.searchPartitions);
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
    StringBuilder sb = new StringBuilder("InstanceRequest(");
    boolean first = true;

    sb.append("requestId:");
    sb.append(this.requestId);
    first = false;
    if (!first) sb.append(", ");
    sb.append("query:");
    if (this.query == null) {
      sb.append("null");
    } else {
      sb.append(this.query);
    }
    first = false;
    if (isSetSearchPartitions()) {
      if (!first) sb.append(", ");
      sb.append("searchPartitions:");
      if (this.searchPartitions == null) {
        sb.append("null");
      } else {
        sb.append(this.searchPartitions);
      }
      first = false;
    }
    sb.append(")");
    return sb.toString();
  }

  public void validate() throws org.apache.thrift.TException {
    // check for required fields
    if (!isSetRequestId()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'requestId' is unset! Struct:" + toString());
    }

    if (!isSetQuery()) {
      throw new org.apache.thrift.protocol.TProtocolException("Required field 'query' is unset! Struct:" + toString());
    }

    // check for sub-struct validity
    if (query != null) {
      query.validate();
    }
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
      // it doesn't seem like you should have to do this, but java serialization is wacky, and doesn't call the default constructor.
      __isset_bitfield = 0;
      read(new org.apache.thrift.protocol.TCompactProtocol(new org.apache.thrift.transport.TIOStreamTransport(in)));
    } catch (org.apache.thrift.TException te) {
      throw new java.io.IOException(te);
    }
  }

  private static class InstanceRequestStandardSchemeFactory implements SchemeFactory {
    public InstanceRequestStandardScheme getScheme() {
      return new InstanceRequestStandardScheme();
    }
  }

  private static class InstanceRequestStandardScheme extends StandardScheme<InstanceRequest> {

    public void read(org.apache.thrift.protocol.TProtocol iprot, InstanceRequest struct) throws org.apache.thrift.TException {
      org.apache.thrift.protocol.TField schemeField;
      iprot.readStructBegin();
      while (true)
      {
        schemeField = iprot.readFieldBegin();
        if (schemeField.type == org.apache.thrift.protocol.TType.STOP) { 
          break;
        }
        switch (schemeField.id) {
          case 1: // REQUEST_ID
            if (schemeField.type == org.apache.thrift.protocol.TType.I64) {
              struct.requestId = iprot.readI64();
              struct.setRequestIdIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 2: // QUERY
            if (schemeField.type == org.apache.thrift.protocol.TType.STRUCT) {
              struct.query = new BrokerRequest();
              struct.query.read(iprot);
              struct.setQueryIsSet(true);
            } else { 
              org.apache.thrift.protocol.TProtocolUtil.skip(iprot, schemeField.type);
            }
            break;
          case 3: // SEARCH_PARTITIONS
            if (schemeField.type == org.apache.thrift.protocol.TType.LIST) {
              {
                org.apache.thrift.protocol.TList _list68 = iprot.readListBegin();
                struct.searchPartitions = new ArrayList<Long>(_list68.size);
                for (int _i69 = 0; _i69 < _list68.size; ++_i69)
                {
                  long _elem70;
                  _elem70 = iprot.readI64();
                  struct.searchPartitions.add(_elem70);
                }
                iprot.readListEnd();
              }
              struct.setSearchPartitionsIsSet(true);
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

    public void write(org.apache.thrift.protocol.TProtocol oprot, InstanceRequest struct) throws org.apache.thrift.TException {
      struct.validate();

      oprot.writeStructBegin(STRUCT_DESC);
      oprot.writeFieldBegin(REQUEST_ID_FIELD_DESC);
      oprot.writeI64(struct.requestId);
      oprot.writeFieldEnd();
      if (struct.query != null) {
        oprot.writeFieldBegin(QUERY_FIELD_DESC);
        struct.query.write(oprot);
        oprot.writeFieldEnd();
      }
      if (struct.searchPartitions != null) {
        if (struct.isSetSearchPartitions()) {
          oprot.writeFieldBegin(SEARCH_PARTITIONS_FIELD_DESC);
          {
            oprot.writeListBegin(new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, struct.searchPartitions.size()));
            for (long _iter71 : struct.searchPartitions)
            {
              oprot.writeI64(_iter71);
            }
            oprot.writeListEnd();
          }
          oprot.writeFieldEnd();
        }
      }
      oprot.writeFieldStop();
      oprot.writeStructEnd();
    }

  }

  private static class InstanceRequestTupleSchemeFactory implements SchemeFactory {
    public InstanceRequestTupleScheme getScheme() {
      return new InstanceRequestTupleScheme();
    }
  }

  private static class InstanceRequestTupleScheme extends TupleScheme<InstanceRequest> {

    @Override
    public void write(org.apache.thrift.protocol.TProtocol prot, InstanceRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol oprot = (TTupleProtocol) prot;
      oprot.writeI64(struct.requestId);
      struct.query.write(oprot);
      BitSet optionals = new BitSet();
      if (struct.isSetSearchPartitions()) {
        optionals.set(0);
      }
      oprot.writeBitSet(optionals, 1);
      if (struct.isSetSearchPartitions()) {
        {
          oprot.writeI32(struct.searchPartitions.size());
          for (long _iter72 : struct.searchPartitions)
          {
            oprot.writeI64(_iter72);
          }
        }
      }
    }

    @Override
    public void read(org.apache.thrift.protocol.TProtocol prot, InstanceRequest struct) throws org.apache.thrift.TException {
      TTupleProtocol iprot = (TTupleProtocol) prot;
      struct.requestId = iprot.readI64();
      struct.setRequestIdIsSet(true);
      struct.query = new BrokerRequest();
      struct.query.read(iprot);
      struct.setQueryIsSet(true);
      BitSet incoming = iprot.readBitSet(1);
      if (incoming.get(0)) {
        {
          org.apache.thrift.protocol.TList _list73 = new org.apache.thrift.protocol.TList(org.apache.thrift.protocol.TType.I64, iprot.readI32());
          struct.searchPartitions = new ArrayList<Long>(_list73.size);
          for (int _i74 = 0; _i74 < _list73.size; ++_i74)
          {
            long _elem75;
            _elem75 = iprot.readI64();
            struct.searchPartitions.add(_elem75);
          }
        }
        struct.setSearchPartitionsIsSet(true);
      }
    }
  }

}

