package com.cassandraguide.rw;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;

public class RemoveExample {
	
	public static void main(String[] args) throws Exception {

		System.out.println("Running remove.");
		
		Connector conn = new Connector();
		Cassandra.Client client = conn.connect();

		String columnFamily = "Standard1";
		byte[] key = "k2".getBytes(); //this is the row key

		Clock clock = new Clock(System.currentTimeMillis());
		
		ColumnPath colPath = new ColumnPath();
		colPath.column_family = columnFamily;
		colPath.column = "b".getBytes();
		
		client.remove(key, colPath, clock, ConsistencyLevel.ALL);
				
		System.out.println("Remove done.");	
		
		conn.close();
		
		System.out.println("All done.");
	}
}