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

import java.util.ArrayList;
import java.util.List;

import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TSimpleJSONProtocol;
import org.breizhbeans.thrift.tools.thriftmongobridge.TBSONSerializer;
import org.breizhbeans.thrift.tools.thriftmongobridge.ThriftMongoHelper;
import org.junit.Assert;
import org.junit.Test;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.Mongo;
import com.mongodb.util.JSON;

public class TestThriftMongoHelper {

	@Test
	public void testTBSONSerializer() throws Exception {
		TBSONSerializer tbsonSerializer = new TBSONSerializer();

		BSonThrift inputBsonThrift = new BSonThrift();
		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);

		inputBsonThrift.addToOneStringList("toto1");
		inputBsonThrift.addToOneStringList("toto2");
		inputBsonThrift.addToOneStringList("toto3");

		TSerializer tjsonSerializer = new TSerializer(new TSimpleJSONProtocol.Factory());
		byte[] jsonObject = tjsonSerializer.serialize(inputBsonThrift);

		DBObject expectedDBObject = (DBObject) JSON.parse(new String(jsonObject));
		DBObject dbObject = tbsonSerializer.serialize(inputBsonThrift);

		Assert.assertEquals(expectedDBObject.toString(), dbObject.toString());
	}

	@Test
	public void testSerializeIntegrity() throws Exception {
		BSonThrift inputBsonThrift = new BSonThrift();
		BSonThrift outputBsonThrift = new BSonThrift();

		inputBsonThrift.setOneString("string value");
		inputBsonThrift.setOneBigInteger(123456);
		List<String> oneStringList = new ArrayList<String>();
		oneStringList.add("toto1");
		oneStringList.add("toto2");
		oneStringList.add("toto3");
		inputBsonThrift.setOneStringList(oneStringList);

		DBObject dbObject = ThriftMongoHelper.thrift2DBObject(inputBsonThrift);
		outputBsonThrift = (BSonThrift) ThriftMongoHelper.DBObject2Thrift(dbObject);

		Assert.assertEquals(outputBsonThrift, inputBsonThrift);
	}

	@Test
	public void testPerfThriftMongoHelper() throws Exception {
		Mongo mongo = new Mongo("localhost", 27017);
		DB db = mongo.getDB("mydb");

		// get a single collection
		DBCollection collection = db.getCollection("dummyColl");

		// build an thrift object
		BSonThrift bsonThrift = null;

		for (int i = 0; i < 5; i++) {

			bsonThrift = new BSonThrift();
			bsonThrift.setOneString("string value" + i);
			bsonThrift.setOneBigInteger(123456 + i);

			long startTime = System.nanoTime();
			DBObject dbObject = ThriftMongoHelper.thrift2DBObject(bsonThrift);
			long endTime = System.nanoTime();
			System.out.println("serialisation  nano time=" + (endTime - startTime));

			// Put the document with the thrift introspection and the binary
			collection.insert(dbObject);

		}
		DBCursor cursorDoc = collection.find();
		while (cursorDoc.hasNext()) {
			DBObject dbObject = cursorDoc.next();

			long startTime = System.nanoTime();
			ThriftMongoHelper.DBObject2Thrift(dbObject);
			long endTime = System.nanoTime();
			System.out.println("deserialisation nano time=" + (endTime - startTime));
		}

		collection.remove(new BasicDBObject());
	}
}
