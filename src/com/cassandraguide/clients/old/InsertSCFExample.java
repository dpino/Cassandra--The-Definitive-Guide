package com.cassandraguide.clients.old;

import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.NotFoundException;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;
import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * Uses a SuperColumn Family. Uses a SCF called Hotel, with row keys for each
 * hotel that hold subcolumns of hotel information.
 */
public class InsertSCFExample {

    private static final Logger LOG = Logger.getLogger(InsertSCFExample.class);

    private static final String UTF8 = "UTF8";
    private static final String HOST = "localhost";
    private static final int PORT = 9160;

    private static final ConsistencyLevel CL = ConsistencyLevel.QUORUM;

    public static void main(String[] args) throws UnsupportedEncodingException,
            InvalidRequestException, UnavailableException, TimedOutException,
            TException, NotFoundException {

        TTransport tr = new TSocket(HOST, PORT);
        TProtocol proto = new TBinaryProtocol(tr);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        String keyspace = "Keyspace1";
        String SUPERCF_NAME = "Hotel";

        LOG.debug("Setting up...");

        // first create the structure to hold the insert batch
        Map<String, List<ColumnOrSuperColumn>> dataMap = new HashMap<String, List<ColumnOrSuperColumn>>();

        // Clarion
        List<ColumnOrSuperColumn> row = new ArrayList<ColumnOrSuperColumn>();

        List<Column> cols = new ArrayList<Column>();

        long ts = System.currentTimeMillis();

        // set up columns for Clarion
        Column clarionNameCol = new Column("name".getBytes(UTF8),
                "Clarion SF".getBytes("UTF-8"), ts);
        Column clarionAddressCol = new Column("address".getBytes(UTF8),
                "123 Market St., SF".getBytes(UTF8), ts);
        Column clarionPhoneCol = new Column("phone".getBytes(UTF8),
                "415-555-1000".getBytes(UTF8), ts);

        cols.add(clarionNameCol);
        cols.add(clarionAddressCol);
        cols.add(clarionPhoneCol);

        // create the supercolumn
        SuperColumn clarionSC = new SuperColumn("Clarion".getBytes(UTF8), cols);

        ColumnOrSuperColumn superCol = new ColumnOrSuperColumn();
        superCol.setSuper_column(clarionSC);

        row.clear();
        row.add(superCol);

        dataMap.put(SUPERCF_NAME, row);

        // insert the first hotel
        client.batch_insert(keyspace, "Clarion_123", dataMap, CL);

        LOG.debug("Inserted first row.");

        // Do it again, for a new super column with a different set of
        // subcolumns

        // set up columns for Comfort
        Column comfortNameCol = new Column("name".getBytes(UTF8),
                "Comfort Midtwown".getBytes(UTF8), ts);
        Column comfortAddressCol = new Column("address".getBytes(UTF8),
                "57th Street, NYC".getBytes(UTF8), ts);
        Column comfortPhoneCol = new Column("phone".getBytes(UTF8),
                "212-555-1000".getBytes(UTF8), ts);

        // set up to reuse
        cols.clear();
        cols.add(comfortNameCol);
        cols.add(comfortAddressCol);
        cols.add(comfortPhoneCol);

        // create the supercolumn
        SuperColumn comfortSC = new SuperColumn("Comfort".getBytes(UTF8), cols);

        ColumnOrSuperColumn superCol2 = new ColumnOrSuperColumn();
        superCol2.setSuper_column(comfortSC);

        row.add(superCol2);

        dataMap.put(SUPERCF_NAME, row);

        String comfortRowKey = "Comfort_789";

        // insert the second hotel
        client.batch_insert(keyspace, comfortRowKey, dataMap, CL);

        LOG.debug("Inserted second row.");

        LOG.debug("Batch Insert done.");

        tr.close();

        LOG.debug("All done.");
    }
}