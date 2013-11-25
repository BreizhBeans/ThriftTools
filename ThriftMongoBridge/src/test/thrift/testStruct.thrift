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
 *
 * Contains some contributions under the Thrift Software License.
 * Please see doc/old-thrift-license.txt in the Thrift distribution for
 * details.
 */

namespace java org.breizhbeans.thrift.tools.thriftmongobridge.test

enum ThriftEnum {
  VALUE_ONE  = 1,
  VALUE_TWO = 2,
  VALUE_THREE  = 3,
}


struct ThriftEnumList {
    1:list<ThriftEnum> thriftEnums,
}

struct AnotherThrift {
	1:string anotherString,
	2:i32    anotherInteger,
}

struct KeyObject {
	1:string strKey,
	2:i32	 intKey,
}

struct BSonThrift {
	1:string oneString,
	2:bool oneBool,
	3:i64 oneBigInteger,
	4:i32 oneInter,
	5:list<string> oneStringList,
	6:AnotherThrift anotherThrift,
	7:set<string> oneStringSet,
	8:map<string,string> oneStringMap,
	9:map<string,AnotherThrift> oneObjectMapAsValue,
	10:map<KeyObject,AnotherThrift> oneMapObjectKeyObjectValue,
	11:binary binaryData,
	12:ThriftEnum thriftEnum,
	13:list<double> oneDoubleList,
}

struct BSonComposite {
	1:string simpleString,
	2:BSonThrift bsonThrift,
}

struct BSonObjectList {
	1:string simpleString
	2:list<AnotherThrift> anotherThrift
}