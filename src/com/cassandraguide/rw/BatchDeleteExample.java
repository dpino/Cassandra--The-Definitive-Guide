package com.cassandraguide.rw;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Clock;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;

/**
 * Shows how to use Mutation and Delete. Requires 0.7.
 */
public class BatchDeleteExample {
	
	public static void main(String[] args) throws Exception {

		System.out.println("Running delete.");
		
		Connector conn = new Connector();
		Cassandra.Client client = conn.connect();

		String columnFamily = "Standard1";
		byte[] key = "k2".getBytes(); //this is the row key

		Clock clock = new Clock(System.currentTimeMillis());
		
		SlicePredicate delPred = new SlicePredicate();
		List<byte[]> delCols = new ArrayList<byte[]>();
		
		//let's delete the column named 'b', though we could add more
		delCols.add("b".getBytes());
		delPred.column_names = delCols;
		
		Deletion deletion = new Deletion();
		deletion.predicate = delPred;
		deletion.clock = clock;
		Mutation mutation = new Mutation();
		mutation.deletion = deletion;
		
		Map<byte[], Map<String, List<Mutation>>> mutationMap = 
			new HashMap<byte[], Map<String, List<Mutation>>>();

		List<Mutation> mutationList = new ArrayList<Mutation>();
		mutationList.add(mutation);
		
		Map<String, List<Mutation>> m = new HashMap<String, List<Mutation>>();
		m.put(columnFamily, mutationList);
				
		//just for this row key, though we could add more
		mutationMap.put(key, m);
		client.batch_mutate(mutationMap, ConsistencyLevel.ALL);
				
		System.out.println("Delete mutation done.");	
		
		conn.close();
		
		System.out.println("All done.");
	}
}