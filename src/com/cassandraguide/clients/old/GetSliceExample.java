package com.cassandraguide.clients.old;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.List;

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
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Shows how to use get_slice. Assumes we've done this: cassandra> set
 * Keyspace1.Standard2['bootsy']['name']='Bootsy Collins' Value inserted.
 * cassandra> set Keyspace1.Standard2['bootsy']['instrument']='bass' Value
 * inserted. cassandra> set
 * Keyspace1.Standard2['bootsy']['email']='bootsy@pfunk.com' Value inserted.
 * cassandra> set Keyspace1.Standard2['george']['name']='George Clinton' Value
 * inserted.
 */
public class GetSliceExample {

    private static final Logger LOG = Logger.getLogger(GetSliceExample.class);

    private static final String UTF8 = "UTF8";
    private static final String HOST = "localhost";
    private static final int PORT = 9160;
    private static final ConsistencyLevel CL = ConsistencyLevel.QUORUM;

    private static final String keyspace = "Keyspace1";
    private static final String columnFamily = "Standard2";

    public static void main(String[] args) throws UnsupportedEncodingException,
            InvalidRequestException, UnavailableException, TimedOutException,
            TException, NotFoundException {

        TTransport tr = new TSocket(HOST, PORT);
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        SlicePredicate predicate = new SlicePredicate();
        List<byte[]> colNames = new ArrayList<byte[]>();
        colNames.add("name".getBytes());
        colNames.add("instrument".getBytes());
        predicate.column_names = colNames;

        ColumnParent parent = new ColumnParent(columnFamily);

        String key = "bootsy";
        List<ColumnOrSuperColumn> results = client.get_slice(key, parent,
                predicate, CL);

        for (ColumnOrSuperColumn cosc : results) {
            Column c = cosc.column;
            LOG.debug(new String(c.name, UTF8) + " : "
                    + new String(c.value, UTF8));
        }

        tr.close();

        LOG.debug("All done.");
    }
}