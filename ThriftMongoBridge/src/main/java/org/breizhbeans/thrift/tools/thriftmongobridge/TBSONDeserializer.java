package org.breizhbeans.thrift.tools.thriftmongobridge;

import org.apache.thrift.TBase;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TProtocolFactory;
import org.breizhbeans.thrift.tools.thriftmongobridge.protocol.TBSONProtocol;

import com.mongodb.DBObject;

public class TBSONDeserializer {
	/**
	 * Internal protocol used for serializing objects.
	 */
	private TBSONProtocol protocol_;

	public TBSONDeserializer() {
		this(new TBSONProtocol.Factory());
	}

	private TBSONDeserializer(TProtocolFactory protocolFactory) {
		protocol_ = (TBSONProtocol) protocolFactory.getProtocol(null);
	}

	public void deserialize(TBase<?,?> base, DBObject dbObject) throws TException {
		try {
			protocol_.setDBOject(dbObject);
			protocol_.setBaseObject( base );
			base.read(protocol_);
		} finally {
			protocol_.reset();
		}
	}

}
