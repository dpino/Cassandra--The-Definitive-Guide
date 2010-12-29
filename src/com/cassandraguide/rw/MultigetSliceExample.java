package com.cassandraguide.rw;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.thrift.TException;

/**
 * OUPUT:
 *
 * Running Multiget Slice.
 * Row k2 -->
 * Row k1 -->
 * All done.
 */

/**
 * FIXME: Apparently is not working OK, the expected output should be:
 *
 * Running Multiget Slice.
 * Row k2 --> a
 * Row k1 --> c
 * All done.
 */

/**
 *
 */
public class MultigetSliceExample {

    private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

    private static final String columnFamily = "Standard1";

    public static void main(String[] args) throws UnsupportedEncodingException,
            InvalidRequestException, UnavailableException, TimedOutException,
            TException, NotFoundException {

        Connector conn = new Connector();
        Cassandra.Client client = conn.connect();

        System.out.println("Running Multiget Slice.");

        SlicePredicate predicate = new SlicePredicate();
        List<ByteBuffer> colNames = new ArrayList<ByteBuffer>();
        colNames.add(bytes("a"));
        colNames.add(bytes("c"));
        predicate.column_names = colNames;

        ColumnParent parent = new ColumnParent(columnFamily);

        // instead of one row key, we specify many
        List<ByteBuffer> rowKeys = new ArrayList<ByteBuffer>();
        rowKeys.add(bytes("k1"));
        rowKeys.add(bytes("k2"));

        // instead of a simple list, we get a map, where the keys are row keys
        // and the values the list of columns returned for each
        Map<ByteBuffer, List<ColumnOrSuperColumn>> results = client
                .multiget_slice(rowKeys, parent, predicate, CL);

        for (ByteBuffer key : results.keySet()) {
            List<ColumnOrSuperColumn> row = results.get(key);

            System.out.println("Row " + ByteBufferUtil.string(key) + " --> ");
            for (ColumnOrSuperColumn cosc : row) {
                Column c = cosc.column;
                System.out.println(ByteBufferUtil.string(c.name) + " : "
                        + ByteBufferUtil.string(c.value));
            }
        }

        conn.close();

        System.out.println("All done.");
    }
}
