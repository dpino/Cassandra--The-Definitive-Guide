package com.cassandraguide.clients.old;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class GetFromSCExample {

	private static final Logger LOG = Logger.getLogger(GetFromSCExample.class);
	
	private static final String UTF8 = "UTF8";
	private static final String HOST = "localhost";
	private static final int PORT = 9160;

	public static void main(String[] args) throws UnsupportedEncodingException,
			InvalidRequestException, UnavailableException, TimedOutException,
			TException, NotFoundException {

		TTransport tr = new TSocket(HOST, PORT);
		TProtocol proto = new TBinaryProtocol(tr);
		Cassandra.Client client = new Cassandra.Client(proto);
		tr.open();

		String keyspace = "Keyspace1";
		String sc = "Hotel";
		//create a slice predicate representing the columns to read
		//start and finish are the range of columns--here, all
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[0]);
		sliceRange.setFinish(new byte[0]);
		predicate.setSlice_range(sliceRange);

		// read all columns in the row
		ColumnParent parent = new ColumnParent(sc);
		parent.super_column = "Clarion".getBytes();

		KeyRange keyRange = new KeyRange();
		keyRange.setStart_key("");
		keyRange.setEnd_key("");
		keyRange.count = 5;
		
		List<KeySlice> keySlices = 
			client.get_range_slices(keyspace, parent, predicate, keyRange, 
					ConsistencyLevel.ONE);
		
		for (KeySlice ks : keySlices) {
			List<ColumnOrSuperColumn> coscs = ks.columns;
			LOG.debug(new String("Key: " + ks.key + " -> "));
			for (ColumnOrSuperColumn cs : coscs) {				
				LOG.debug(new String(cs.column.name, UTF8) + " : "
						+ new String(cs.column.value, UTF8));
			}
			
		}
		
		tr.close();
		
		LOG.debug("All done.");
	}
}