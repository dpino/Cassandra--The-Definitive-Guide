package com.cassandraguide.rw;

import java.io.UnsupportedEncodingException;
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
import org.apache.thrift.TException;

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
        List<byte[]> colNames = new ArrayList<byte[]>();
        colNames.add("a".getBytes());
        colNames.add("c".getBytes());
        predicate.column_names = colNames;

        ColumnParent parent = new ColumnParent(columnFamily);

        // instead of one row key, we specify many
        List<byte[]> rowKeys = new ArrayList<byte[]>();
        rowKeys.add("k1".getBytes());
        rowKeys.add("k2".getBytes());

        // instead of a simple list, we get a map, where the keys are row keys
        // and the values the list of columns returned for each
        Map<byte[], List<ColumnOrSuperColumn>> results = client.multiget_slice(
                rowKeys, parent, predicate, CL);

        for (byte[] key : results.keySet()) {
            List<ColumnOrSuperColumn> row = results.get(key);

            System.out.println("Row " + new String(key) + " --> ");
            for (ColumnOrSuperColumn cosc : row) {
                Column c = cosc.column;
                System.out.println(new String(c.name, "UTF-8") + " : "
                        + new String(c.value, "UTF-8"));
            }
        }

        conn.close();

        System.out.println("All done.");
    }
}