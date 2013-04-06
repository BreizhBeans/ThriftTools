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
package org.breizhbeans.thrift.tools.thriftmongobridge;

import org.apache.thrift.TBase;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.apache.thrift.protocol.TCompactProtocol;

import com.mongodb.DBObject;

/**
 * 
 * Simple Thrift Serializer for MongoDB Objects
 * 
 * @author Sebastien Lambour
 * 
 */
public class ThriftMongoHelper {

	private static TBSONSerializer tbsonSerializer = new TBSONSerializer();
	private static TBSONDeserializer tbsonDeserializer = new TBSONDeserializer();	

	public static DBObject thrift2DBObject(final TBase<?, ?> thriftObject) throws Exception {
		// Thrift object serialize
		// Construction of the dbobject
		DBObject dbObject = tbsonSerializer.serialize(thriftObject);
		
		// Add the thrift serialization to the dbobject
		dbObject.put("classname", thriftObject.getClass().getCanonicalName());

		return dbObject;
	}

	public static TBase<?, ?> DBObject2Thrift(final DBObject dbObject) throws Exception {
		String classname = (String) dbObject.get("classname");
		TBase<?, ?> thriftObject = (TBase<?, ?>) Class.forName(classname).newInstance();

		tbsonDeserializer.deserialize(thriftObject, dbObject);

		return thriftObject;
	}
}
