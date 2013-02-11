namespace java org.breizhbeans.thrift.tools.thriftmongobridge.test


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
}

struct BSonComposite {
	1:string simpleString,
	2:BSonThrift bsonThrift,
}

struct BSonObjectList {
	1:string simpleString
	2:list<AnotherThrift> anotherThrift
}