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
import org.apache.cassandra.thrift.KeyRange;
import org.apache.cassandra.thrift.KeySlice;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.utils.ByteBufferUtil;

/**
 * OUTPUT:
 *
 * Getting Range Slices.
 * Current row: k2
 * All done.
 */

/**
 *
 */
public class GetRangeSliceExample {

    public static void main(String[] args) throws Exception {
        Connector conn = new Connector();
        Cassandra.Client client = conn.connect();

        System.out.println("Getting Range Slices.");

        SlicePredicate predicate = new SlicePredicate();
        List<ByteBuffer> colNames = new ArrayList<ByteBuffer>();
        colNames.add(bytes("a"));
        colNames.add(bytes("b"));
        predicate.column_names = colNames;

        ColumnParent parent = new ColumnParent("Standard1");

        KeyRange keyRange = new KeyRange();
        keyRange.start_key = bytes("k1");
        keyRange.end_key = bytes("k2");

        // a key slice is returned
        List<KeySlice> results = client.get_range_slices(parent, predicate,
                keyRange, ConsistencyLevel.ONE);

        for (KeySlice keySlice : results) {
            List<ColumnOrSuperColumn> cosc = keySlice.getColumns();

            System.out.println("Current row: " + new String(keySlice.getKey()));

            for (int i = 0; i < cosc.size(); i++) {
                Column c = cosc.get(i).getColumn();
                System.out.println(ByteBufferUtil.string(c.name) + " : "
                        + ByteBufferUtil.string(c.value));
            }
        }

        conn.close();

        System.out.println("All done.");
    }
}
