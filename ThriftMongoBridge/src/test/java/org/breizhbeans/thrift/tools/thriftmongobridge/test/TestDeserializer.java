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
package org.breizhbeans.thrift.tools.thriftmongobridge.test;

import com.mongodb.DBObject;
import junit.framework.Assert;
import org.apache.thrift.TBase;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONDeserializer;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONSerializer;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

public class TestDeserializer {


    @Test
    public void testTBSONObjectListEnum() throws Exception {
        TBSONDeserializer deserializer = new TBSONDeserializer();

        ThriftEnumList expectedThriftObject = new ThriftEnumList();

        expectedThriftObject.addToThriftEnums(ThriftEnum.VALUE_ONE);
        expectedThriftObject.addToThriftEnums(ThriftEnum.VALUE_TWO);

        DBObject expectedDbObject = getDBObject(expectedThriftObject);
        ThriftEnumList actualThriftObject = new ThriftEnumList();

        deserializer.deserialize(actualThriftObject, expectedDbObject);
        Assert.assertEquals(expectedThriftObject, actualThriftObject);
    }
	
	@Test
	public void testSimpleThrift() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();
		
		AnotherThrift anotherThrift1 = new AnotherThrift();
		anotherThrift1.setAnotherString("str1");
		anotherThrift1.setAnotherInteger(31);
		
		AnotherThrift anotherThrift2 = new AnotherThrift();
		anotherThrift2.setAnotherString("str2");
		anotherThrift2.setAnotherInteger(32);
		
		BSonObjectList expectedThriftObject = new BSonObjectList();
		expectedThriftObject.setSimpleString("simple string");
		expectedThriftObject.addToAnotherThrift(anotherThrift1);
		expectedThriftObject.addToAnotherThrift(anotherThrift2);
		
		BSonObjectList actualThriftObject = new BSonObjectList();
		
		DBObject expectedDbObject = getDBObject(expectedThriftObject);
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		Assert.assertEquals(expectedThriftObject, actualThriftObject);
	}
	
	@Test
	public void testTBSONComposite() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();
		
		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		
		BSonComposite bsonComposite = new BSonComposite();
		bsonComposite.setSimpleString("simple string");
		bsonComposite.setBsonThrift(inputBsonThrift);
		
		DBObject expectedDbObject = getDBObject(bsonComposite);
		
		System.out.println("expected DBObject=" + expectedDbObject.toString());
		BSonComposite actualThriftObject = new BSonComposite();
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		
		System.out.println("actual   DBObject=" + actualThriftObject.toString());
		
		Assert.assertEquals(bsonComposite, actualThriftObject);
	}	
	
	@Test
	public void testTBSONCompositeNLevel() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();
		
		AnotherThrift anotherThrift = new AnotherThrift();
		anotherThrift.setAnotherString("str1");
		anotherThrift.setAnotherInteger(32);
		
		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setAnotherThrift(anotherThrift);
		
		BSonComposite bsonComposite = new BSonComposite();
		bsonComposite.setSimpleString("simple string");
		bsonComposite.setBsonThrift(inputBsonThrift);
		
		DBObject expectedDbObject = getDBObject(bsonComposite);
		
		System.out.println("expected DBObject=" + expectedDbObject.toString());
		BSonComposite actualThriftObject = new BSonComposite();
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		
		System.out.println("actual   DBObject=" + actualThriftObject.toString());
		
		Assert.assertEquals(bsonComposite, actualThriftObject);	
	}		
	
	@Test
	public void testTBSONList() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);

		// A list (like Java list)
		inputBsonThrift.addToOneStringList("toto1");
		inputBsonThrift.addToOneStringList("toto1");
		inputBsonThrift.addToOneStringList("toto3");

        //A List of double
        inputBsonThrift.addToOneDoubleList((double)8.123);
        inputBsonThrift.addToOneDoubleList((double)8.129);

		// A set (like Java Set)
		inputBsonThrift.addToOneStringSet("set3");		
		inputBsonThrift.addToOneStringSet("set1");
		inputBsonThrift.addToOneStringSet("set2");
		inputBsonThrift.addToOneStringSet("set1");

		// serialize into DBObject
		DBObject expectedDbObject = getDBObject(inputBsonThrift);
		
		System.out.println("expected DBObject=" + expectedDbObject.toString());
		BSonThrift actualThriftObject = new BSonThrift();
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		
		System.out.println("actual   DBObject=" + actualThriftObject.toString());
		
		Assert.assertEquals(inputBsonThrift, actualThriftObject);	
	}	
	
	@Test
	public void testTBSONMapStringString() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);

		// A Map like Java Map
		Map<String,String> oneStringMap = new HashMap<String,String>();
		oneStringMap.put("key1", "value1");
		oneStringMap.put("key2", "value2");
		inputBsonThrift.setOneStringMap(oneStringMap);

		// serialize into DBObject
		DBObject expectedDbObject = getDBObject(inputBsonThrift);
		
		System.out.println("expected DBObject=" + expectedDbObject.toString());
		BSonThrift actualThriftObject = new BSonThrift();
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		
		System.out.println("actual   DBObject=" + actualThriftObject.toString());
		
		Assert.assertEquals(inputBsonThrift, actualThriftObject);	
	}		
	
	@Test
	public void testTBSONSerializerMapStringObject() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);
		
		// A Map like Java Map
		Map<String,AnotherThrift> oneMap = new HashMap<String,AnotherThrift>();
		oneMap.put("key1", new AnotherThrift("value1", 1));
		oneMap.put("key2", new AnotherThrift("value2", 2));
		inputBsonThrift.setOneObjectMapAsValue(oneMap);
		
		// serialize into DBObject
		DBObject expectedDbObject = getDBObject(inputBsonThrift);
		
		System.out.println("expected DBObject=" + expectedDbObject.toString());
		BSonThrift actualThriftObject = new BSonThrift();
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		
		System.out.println("actual   DBObject=" + actualThriftObject.toString());
		
		Assert.assertEquals(inputBsonThrift, actualThriftObject);	
	}	
	
	@Test
	public void testTBSONBinaryObject() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();

		BSonThrift inputBsonThrift = new BSonThrift();

		byte[] binaryData = { (byte) 0x00, (byte) 0x01, (byte) 0x02, (byte) 0x03, (byte) 0x04, (byte) 0x05,
		      (byte) 0x06, (byte) 0x07, (byte) 0x08, (byte) 0x09, (byte) 0x0a, (byte) 0x0b, (byte) 0x0c, (byte) 0x0d, (byte) 0x0e,
		      (byte) 0x0f, (byte) 0xf0, (byte) 0xf1, (byte) 0xf2, (byte) 0xf3, (byte) 0xf4, (byte) 0xf5, (byte) 0xf6, (byte) 0xf7,
		      (byte) 0xf8, (byte) 0xf9, (byte) 0xfa, (byte) 0xfb, (byte) 0xfc, (byte) 0xfd, (byte) 0xfe, (byte) 0xff };

		
		inputBsonThrift.setBinaryData(binaryData);
		
		// serialize into DBObject
		DBObject expectedDbObject = getDBObject(inputBsonThrift);
		
		System.out.println("expected DBObject=" + expectedDbObject.toString());
		BSonThrift actualThriftObject = new BSonThrift();
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		
		System.out.println("actual   DBObject=" + actualThriftObject.toString());
		
		Assert.assertEquals(inputBsonThrift, actualThriftObject);			
	}
	
	@Test
	public void testTBSONEnum() throws Exception {
		TBSONDeserializer deserializer = new TBSONDeserializer();

		BSonThrift inputBsonThrift = new BSonThrift();

		inputBsonThrift.setThriftEnum(ThriftEnum.VALUE_THREE);
		
		// serialize into DBObject
		DBObject expectedDbObject = getDBObject(inputBsonThrift);
		
		System.out.println("expected DBObject=" + expectedDbObject.toString());
		BSonThrift actualThriftObject = new BSonThrift();
		deserializer.deserialize(actualThriftObject, expectedDbObject);
		
		System.out.println("actual   DBObject=" + actualThriftObject.toString());
		
		Assert.assertEquals(inputBsonThrift, actualThriftObject);			
	}	
	
	private DBObject getDBObject( TBase<?,?> tbase) throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();
		// serialize into DBObject
		DBObject dbObject = tbsonSerializer.serialize(tbase);
		return dbObject;
	}
}
