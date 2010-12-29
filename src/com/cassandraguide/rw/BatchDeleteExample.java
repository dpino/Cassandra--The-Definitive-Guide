package com.cassandraguide.rw;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.Deletion;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * Should create a column family named 'Standard1' at keyspace 'Keyspace1'
 *
 * [default@unknown] use keyspace Keyspace1
 * [default@unknown] create column family Standard1 ;
 *
 */

/**
 * OUTPUT:
 *
 * Running delete.
 * Delete mutation done.
 * All done.
 */

/**
 * Shows how to use Mutation and Delete. Requires 0.7.
 */
public class BatchDeleteExample {

    public static void main(String[] args) throws Exception {

        System.out.println("Running delete.");

        Connector conn = new Connector();
        Cassandra.Client client = conn.connect();

        String columnFamily = "Standard1";
        ByteBuffer key = ByteBufferUtil.bytes("k2"); // this is the row key

        long ts = System.currentTimeMillis();

        SlicePredicate delPred = new SlicePredicate();
        List<ByteBuffer> delCols = new ArrayList<ByteBuffer>();

        // let's delete the column named 'b', though we could add more
        delCols.add(ByteBufferUtil.bytes("b"));
        delPred.column_names = delCols;

        Deletion deletion = new Deletion();
        deletion.predicate = delPred;
        deletion.timestamp = ts;
        Mutation mutation = new Mutation();
        mutation.deletion = deletion;

        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        List<Mutation> mutationList = new ArrayList<Mutation>();
        mutationList.add(mutation);

        Map<String, List<Mutation>> m = new HashMap<String, List<Mutation>>();
        m.put(columnFamily, mutationList);

        // just for this row key, though we could add more
        mutationMap.put(key, m);
        client.batch_mutate(mutationMap, ConsistencyLevel.ALL);

        System.out.println("Delete mutation done.");

        conn.close();

        System.out.println("All done.");
    }
}
