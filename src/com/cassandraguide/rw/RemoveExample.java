package com.cassandraguide.rw;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;

/**
 * OUTPUT:
 *
 * Running remove.
 * Remove done.
 * All done
 */

/**
 *
 */
public class RemoveExample {

    public static void main(String[] args) throws Exception {

        System.out.println("Running remove.");

        Connector conn = new Connector();
        Cassandra.Client client = conn.connect();

        String columnFamily = "Standard1";
        ByteBuffer key = bytes("k2"); // this is the row key

        long ts = System.currentTimeMillis();

        ColumnPath colPath = new ColumnPath();
        colPath.column_family = columnFamily;
        colPath.column = bytes("b");

        client.remove(key, colPath, ts, ConsistencyLevel.ALL);

        System.out.println("Remove done.");

        conn.close();

        System.out.println("All done.");
    }
}
