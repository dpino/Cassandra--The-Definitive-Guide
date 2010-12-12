package com.cassandraguide.rw;

import java.io.UnsupportedEncodingException;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

public class SimpleWriteRead {

	private static final Logger LOG = Logger.getLogger(SimpleWriteRead.class);
	
	//set up some constants 
	private static final String UTF8 = "UTF8";
	private static final String HOST = "localhost";
	private static final int PORT = 9160;
	private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

	//not paying attention to exceptions here
	public static void main(String[] args) throws UnsupportedEncodingException,
			InvalidRequestException, UnavailableException, TimedOutException,
			TException, NotFoundException {

		TTransport tr = new TSocket(HOST, PORT);
		//new default in 0.7 is framed transport
		TFramedTransport tf = new TFramedTransport(tr);
		TProtocol proto = new TBinaryProtocol(tf);
		Cassandra.Client client = new Cassandra.Client(proto);
		tf.open();
		client.set_keyspace("Keyspace1");

		String cfName = "Standard1";
		byte[] userIDKey = "1".getBytes(); //this is a row key

		Clock clock = new Clock(System.currentTimeMillis());
		
		ColumnParent cp = new ColumnParent(cfName);

		//insert the name column
		LOG.debug("Inserting row for key " + new String(userIDKey));
		client.insert(userIDKey, cp, 
				new Column("name".getBytes(UTF8), 
						"George Clinton".getBytes(), clock), CL);

		//insert the Age column
		client.insert(userIDKey, cp, 
				new Column("age".getBytes(UTF8), 
						"69".getBytes(), clock), CL);
				
		LOG.debug("Row insert done.");

		// read just the Name column
		LOG.debug("Reading Name Column:");
		
		//create a representation of the Name column
		ColumnPath colPathName = new ColumnPath(cfName);
		colPathName.setColumn("name".getBytes(UTF8));
		Column col = client.get(userIDKey, colPathName,
				CL).getColumn();

		LOG.debug("Column name: " + new String(col.name, UTF8));
		LOG.debug("Column value: " + new String(col.value, UTF8));
		LOG.debug("Column timestamp: " + col.clock.timestamp);

		//create a slice predicate representing the columns to read
		//start and finish are the range of columns--here, all
		SlicePredicate predicate = new SlicePredicate();
		SliceRange sliceRange = new SliceRange();
		sliceRange.setStart(new byte[0]);
		sliceRange.setFinish(new byte[0]);
		predicate.setSlice_range(sliceRange);

		LOG.debug("Complete Row:");
		// read all columns in the row
		ColumnParent parent = new ColumnParent(cfName);
		List<ColumnOrSuperColumn> results = 
			client.get_slice(userIDKey, 
					parent, predicate, CL);
		
		//loop over columns, outputting values
		for (ColumnOrSuperColumn result : results) {
			Column column = result.column;
			LOG.debug(new String(column.name, UTF8) + " : "
					+ new String(column.value, UTF8));
		}
		tf.close();
		
		LOG.debug("All done.");
	}
}