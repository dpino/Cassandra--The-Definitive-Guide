package com.cassandraguide.rw;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * OUTPUT:
 *
 * DEBUG 20:45:00,523 Inserting row for key 1
 * DEBUG 20:45:00,533 Row insert done.
 * DEBUG 20:45:00,533 Get result:
 * DEBUG 20:45:00,547 name : George Clinton
 * DEBUG 20:45:00,547 All done.
 *
 */

/**
 *
 */
public class GetExample {

    private static final Logger LOG = Logger.getLogger(GetExample.class);

    private static final String UTF8 = "UTF8";
    private static final String HOST = "localhost";
    private static final int PORT = 9160;
    private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

    public static void main(String[] args) throws UnsupportedEncodingException,
            InvalidRequestException, UnavailableException, TimedOutException,
            TException, NotFoundException {

        TTransport tr = new TSocket(HOST, PORT);
        // new default in 0.7 is framed transport
        TFramedTransport tf = new TFramedTransport(tr);
        TProtocol proto = new TBinaryProtocol(tf);
        Cassandra.Client client = new Cassandra.Client(proto);
        tf.open();
        client.set_keyspace("Keyspace1");

        String cfName = "Standard1";
        ByteBuffer userIDKey = bytes("1"); // this is the row key

        long ts = System.currentTimeMillis();

        // create a representation of the Name column
        ColumnParent cp = new ColumnParent(cfName);

        // insert the name column
        LOG.debug("Inserting row for key " + ByteBufferUtil.string(userIDKey));
        client.insert(userIDKey, cp, new Column(bytes("name"),
                bytes("George Clinton"), ts), CL);

        LOG.debug("Row insert done.");

        /** Do the GET */

        LOG.debug("Get result:");
        // read all columns in the row
        ColumnPath path = new ColumnPath();
        path.column_family = cfName;
        path.column = bytes("name");

        ColumnOrSuperColumn cosc = client.get(userIDKey, path, CL);
        Column column = cosc.column;
        LOG.debug(ByteBufferUtil.string(column.name) + " : "
                + ByteBufferUtil.string(column.value));
        tr.close();

        LOG.debug("All done.");
    }
}
