package com.cassandraguide.rw;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * OUTPUT:
 *
 * All done.
 *
 * FIXME: Apparently, is not working OK.
 */

/**
 *
 */
public class SlicePredicateExample {

    public static void main(String[] args) throws Exception {
        Connector conn = new Connector();
        Cassandra.Client client = conn.connect();

        SlicePredicate predicate = new SlicePredicate();
        List<ByteBuffer> colNames = new ArrayList<ByteBuffer>();
        colNames.add(bytes("a"));
        colNames.add(bytes("b"));
        predicate.column_names = colNames;

        ColumnParent parent = new ColumnParent("Standard1");

        ByteBuffer key = bytes("k1");
        List<ColumnOrSuperColumn> results = client.get_slice(key, parent,
                predicate, ConsistencyLevel.ONE);

        for (ColumnOrSuperColumn cosc : results) {
            Column c = cosc.column;
            System.out.println(ByteBufferUtil.string(c.name) + " : "
                    + ByteBufferUtil.string(c.value));
        }

        conn.close();

        System.out.println("All done.");
    }
}
