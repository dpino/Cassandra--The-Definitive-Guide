package com.cassandraguide.rw;

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

public class GetRangeSliceExample {

    public static void main(String[] args) throws Exception {
        Connector conn = new Connector();
        Cassandra.Client client = conn.connect();

        System.out.println("Getting Range Slices.");

        SlicePredicate predicate = new SlicePredicate();
        List<byte[]> colNames = new ArrayList<byte[]>();
        colNames.add("a".getBytes());
        colNames.add("b".getBytes());
        predicate.column_names = colNames;

        ColumnParent parent = new ColumnParent("Standard1");

        KeyRange keyRange = new KeyRange();
        keyRange.start_key = "k1".getBytes();
        keyRange.end_key = "k2".getBytes();

        // a key slice is returned
        List<KeySlice> results = client.get_range_slices(parent, predicate,
                keyRange, ConsistencyLevel.ONE);

        for (KeySlice keySlice : results) {
            List<ColumnOrSuperColumn> cosc = keySlice.getColumns();

            System.out.println("Current row: " + new String(keySlice.getKey()));

            for (int i = 0; i < cosc.size(); i++) {
                Column c = cosc.get(i).getColumn();
                System.out.println(new String(c.name, "UTF-8") + " : "
                        + new String(c.value, "UTF-8"));
            }
        }

        conn.close();

        System.out.println("All done.");
    }
}
