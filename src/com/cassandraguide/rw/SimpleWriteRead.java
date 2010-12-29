package com.cassandraguide.rw;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.List;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
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
 * DEBUG 20:51:45,969 Inserting row for key 1
 * DEBUG 20:51:45,980 Row insert done.
 * DEBUG 20:51:45,980 Reading Name Column:
 * DEBUG 20:51:45,988 Column name: name
 * DEBUG 20:51:45,988 Column value: George Clinton
 * DEBUG 20:51:45,989 Column timestamp: 1293565905967
 * DEBUG 20:51:45,993 Complete Row:
 * DEBUG 20:51:46,003 age : 69
 * DEBUG 20:51:46,006 name : George Clinton
 * DEBUG 20:51:46,006 All done.
 */

/**
 *
 */
public class SimpleWriteRead {

    private static final Logger LOG = Logger.getLogger(SimpleWriteRead.class);

    // set up some constants
    private static final String UTF8 = "UTF8";
    private static final String HOST = "localhost";
    private static final int PORT = 9160;
    private static final ConsistencyLevel CL = ConsistencyLevel.ONE;

    // not paying attention to exceptions here
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
        ByteBuffer userIDKey = bytes("1"); // this is a row key

        long ts = System.currentTimeMillis();

        ColumnParent cp = new ColumnParent(cfName);

        // insert the name column
        LOG.debug("Inserting row for key " + ByteBufferUtil.string(userIDKey));
        client.insert(userIDKey, cp, new Column(bytes("name"),
                bytes("George Clinton"), ts), CL);

        // insert the Age column
        client.insert(userIDKey, cp, new Column(bytes("age"), bytes("69"), ts),
                CL);

        LOG.debug("Row insert done.");

        // read just the Name column
        LOG.debug("Reading Name Column:");

        // create a representation of the Name column
        ColumnPath colPathName = new ColumnPath(cfName);
        colPathName.setColumn("name".getBytes(UTF8));
        Column col = client.get(userIDKey, colPathName, CL).getColumn();

        LOG.debug("Column name: " + ByteBufferUtil.string(col.name));
        LOG.debug("Column value: " + ByteBufferUtil.string(col.value));
        LOG.debug("Column timestamp: " + col.timestamp);

        // create a slice predicate representing the columns to read
        // start and finish are the range of columns--here, all
        SlicePredicate predicate = new SlicePredicate();
        SliceRange sliceRange = new SliceRange();
        sliceRange.setStart(new byte[0]);
        sliceRange.setFinish(new byte[0]);
        predicate.setSlice_range(sliceRange);

        LOG.debug("Complete Row:");
        // read all columns in the row
        ColumnParent parent = new ColumnParent(cfName);
        List<ColumnOrSuperColumn> results = client.get_slice(userIDKey, parent,
                predicate, CL);

        // loop over columns, outputting values
        for (ColumnOrSuperColumn result : results) {
            Column column = result.column;
            LOG.debug(ByteBufferUtil.string(column.name) + " : "
                    + ByteBufferUtil.string(column.value));
        }
        tf.close();

        LOG.debug("All done.");
    }
}
