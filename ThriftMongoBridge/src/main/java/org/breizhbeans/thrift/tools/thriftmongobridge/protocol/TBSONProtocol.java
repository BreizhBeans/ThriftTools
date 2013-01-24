/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.breizhbeans.thrift.tools.thriftmongobridge.protocol;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.Stack;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TField;
import org.apache.thrift.protocol.TList;
import org.apache.thrift.protocol.TMap;
import org.apache.thrift.protocol.TMessage;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.protocol.TProtocolFactory;
import org.apache.thrift.protocol.TSet;
import org.apache.thrift.protocol.TStruct;
import org.apache.thrift.transport.TTransport;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public class TBSONProtocol extends TProtocol {
	public static final char QUOTE = '"';

	private static ThreadLocal<Stack<Context>> threadSafeContextStack = new ThreadLocal<Stack<Context>>();
	private static ThreadLocal<DBObject>       threadSafeDBObject = new ThreadLocal<DBObject>();

	/**
	 * Factory
	 */
	public static class Factory implements TProtocolFactory {
		public TProtocol getProtocol(TTransport trans) {
			return new TBSONProtocol();
		}
	}

	/**
	 * Constructor
	 */
	public TBSONProtocol() {
		super(null);
	}

	public DBObject getDBObject() {
		return threadSafeDBObject.get();
	}

	private static final TStruct ANONYMOUS_STRUCT = new TStruct();
	private static final TField ANONYMOUS_FIELD = new TField();
	private static final TMessage EMPTY_MESSAGE = new TMessage();
	private static final TSet EMPTY_SET = new TSet();
	private static final TList EMPTY_LIST = new TList();
	private static final TMap EMPTY_MAP = new TMap();

	protected class Context {
		public DBObject dbObject = null; 
		void add(String value) {}
		void add(int value) {}
		void add(long value) {}
		void add(double value) {}
		public void addList(DBObject dbList) {
			this.dbObject = dbList;
			
		}
	}

	protected class FieldContext extends Context {
		public FieldContext(String name) {
			this.name = name;
		}

		public String name;
		public Object value;

		void add(String value) {
			this.value = value;
		}
		
		void add(int value) {
			this.value = Integer.valueOf(value);
		}
		
		void add(long value) {
			this.value = Long.valueOf(value);
		}		
		
		void add(double value) {
			this.value = Double.valueOf(value);
		}
	}

	protected class ListContext extends Context {
		DBObject dbList = new BasicDBList();
		Integer index = 0;
		void add(String value) {
			dbList.put(index.toString(), value);
			index++;
		}
	}

	protected class StructContext extends Context {
		public StructContext() {
			dbObject = new BasicDBObject();
		}
		
		void add(String value) {
			// TODO Auto-generated method stub
			
		}
	}

	/**
	 * Push a new write context onto the stack.
	 */
	protected void pushWriteContext(Context c) {
		Stack<Context> stack = threadSafeContextStack.get();
		if( stack == null ) {
			stack = new Stack<Context>();
			stack.push(c);
			threadSafeContextStack.set(stack);
		} else {
			threadSafeContextStack.get().push(c);
		}
	}

	/**
	 * Pop the last write context off the stack
	 */
	protected Context popWriteContext() {
		Context c = threadSafeContextStack.get().pop(); 
		return c;
	}

	protected Context peekWriteContext() {
		return threadSafeContextStack.get().peek();
	}
	public void writeMessageBegin(TMessage message) throws TException {
		// trans_.write(LBRACKET);
		pushWriteContext(new ListContext());
		writeString(message.name);
		writeByte(message.type);
		writeI32(message.seqid);
	}

	public void writeMessageEnd() throws TException {
		popWriteContext();
		// trans_.write(RBRACKET);
	}

	public void writeStructBegin(TStruct struct) throws TException {
		// writeContext_.write();
		// trans_.write(LBRACE);
		StructContext c = new StructContext();
		pushWriteContext(c);
		threadSafeDBObject.set(c.dbObject);
	}

	public void writeStructEnd() throws TException {
		DBObject dbObject = popWriteContext().dbObject;
		threadSafeDBObject.set(dbObject);
	}

	public void writeFieldBegin(TField field) throws TException {
		// Note that extra type information is omitted in BSON!
		pushWriteContext(new FieldContext(field.name));
		writeString(field.name);
	}

	public void writeFieldEnd() throws TException {
		Context c = popWriteContext();
		if( c.dbObject == null ) {
			threadSafeDBObject.get().put( ((FieldContext)c).name, ((FieldContext)c).value);
		} else {
			threadSafeDBObject.get().put( ((FieldContext)c).name, c.dbObject);
		}
	}

	public void writeFieldStop() {
	}

	public void writeMapBegin(TMap map) throws TException {
		// writeContext_.write();
		// trans_.write(LBRACE);
		pushWriteContext(new StructContext());
		// No metadata!
	}

	public void writeMapEnd() throws TException {
		popWriteContext();
		// trans_.write(RBRACE);
	}

	public void writeListBegin(TList list) throws TException {
		pushWriteContext(new ListContext());
	}

	public void writeListEnd() throws TException {
		// Gets the list
		ListContext list = (ListContext) popWriteContext();
		// Add the list to the current field
		Context fieldContext = peekWriteContext();
		fieldContext.addList(list.dbList);
		
	}

	public void writeSetBegin(TSet set) throws TException {
		// writeContext_.write();
		// trans_.write(LBRACKET);
		pushWriteContext(new ListContext());
		// No metadata!
	}

	public void writeSetEnd() throws TException {
		popWriteContext();
		// trans_.write(RBRACKET);
	}

	public void writeBool(boolean b) throws TException {
		writeByte(b ? (byte) 1 : (byte) 0);
	}

	public void writeByte(byte b) throws TException {
		peekWriteContext().add((int)b);
	}

	public void writeI16(short i16) throws TException {
		peekWriteContext().add((int)i16);
	}

	public void writeI32(int i32) throws TException {
		peekWriteContext().add(i32);
	}

	public void _writeStringData(String s) throws TException {
		try {
			byte[] b = s.getBytes("UTF-8");
			peekWriteContext().add(new String(s));
		} catch (UnsupportedEncodingException uex) {
			throw new TException("JVM DOES NOT SUPPORT UTF-8");
		}
	}

	public void writeI64(long i64) throws TException {
		peekWriteContext().add(i64);
	}

	public void writeDouble(double dub) throws TException {
		peekWriteContext().add(dub);
	}

	public void writeString(String str) throws TException {
		_writeStringData(str);
	}

	public void writeBinary(ByteBuffer bin) throws TException {
		try {
			// TODO: Fix this
			writeString(new String(bin.array(), bin.position() + bin.arrayOffset(), bin.limit() - bin.position() - bin.arrayOffset(), "UTF-8"));
		} catch (UnsupportedEncodingException uex) {
			throw new TException("JVM DOES NOT SUPPORT UTF-8");
		}
	}

	/**
	 * Reading methods.
	 */

	public TMessage readMessageBegin() throws TException {
		return EMPTY_MESSAGE;
	}

	public void readMessageEnd() {
	}

	public TStruct readStructBegin() {
		return ANONYMOUS_STRUCT;
	}

	public void readStructEnd() {
	}

	public TField readFieldBegin() throws TException {
		return ANONYMOUS_FIELD;
	}

	public void readFieldEnd() {
	}

	public TMap readMapBegin() throws TException {
		return EMPTY_MAP;
	}

	public void readMapEnd() {
	}

	public TList readListBegin() throws TException {
		return EMPTY_LIST;
	}

	public void readListEnd() {
	}

	public TSet readSetBegin() throws TException {
		return EMPTY_SET;
	}

	public void readSetEnd() {
	}

	public boolean readBool() throws TException {
		return (readByte() == 1);
	}

	public byte readByte() throws TException {
		return 0;
	}

	public short readI16() throws TException {
		return 0;
	}

	public int readI32() throws TException {
		return 0;
	}

	public long readI64() throws TException {
		return 0;
	}

	public double readDouble() throws TException {
		return 0;
	}

	public String readString() throws TException {
		return "";
	}

	public String readStringBody(int size) throws TException {
		return "";
	}

	public ByteBuffer readBinary() throws TException {
		return ByteBuffer.wrap(new byte[0]);
	}
}
