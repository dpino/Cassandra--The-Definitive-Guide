package com.cassandraguide.hotel;

import static com.cassandraguide.hotel.Constants.CAMBRIA_NAME;
import static com.cassandraguide.hotel.Constants.CL;
import static com.cassandraguide.hotel.Constants.CLARION_NAME;
import static com.cassandraguide.hotel.Constants.UTF8;
import static com.cassandraguide.hotel.Constants.WALDORF_NAME;
import static com.cassandraguide.hotel.Constants.W_NAME;
import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.UnsupportedEncodingException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.Mutation;
import org.apache.cassandra.thrift.SuperColumn;
import org.apache.log4j.Logger;

/**
 * Performs the initial population of the database. Fills the CFs and SCFs with
 * Hotel, Point of Interest, and index data. Shows batch_mutate and insert for
 * Column Families and Super Column Families.
 * 
 * I am totally ignoring exceptions to save space.
 */
public class Prepopulate {
    private static final Logger LOG = Logger.getLogger(Prepopulate.class);

    private Cassandra.Client client;
    private Connector connector;

    // constructor opens a connection so we don't have to
    // constantly recreate it
    public Prepopulate() throws Exception {
        connector = new Connector();
        client = connector.connect();
    }

    void prepopulate() throws Exception {
        // pre-populate the DB with Hotels
        insertAllHotels();

        // also add all hotels to index to help searches
        insertByCityIndexes();

        // pre-populate the DB with POIs
        insertAllPointsOfInterest();

        connector.close();
    }

    // also add hotels to lookup by city index
    public void insertByCityIndexes() throws Exception {

        String scottsdaleKey = "Scottsdale:AZ";
        String sfKey = "San Francisco:CA";
        String newYorkKey = "New York:NY";

        insertByCityIndex(scottsdaleKey, CAMBRIA_NAME);
        insertByCityIndex(scottsdaleKey, CLARION_NAME);
        insertByCityIndex(sfKey, W_NAME);
        insertByCityIndex(newYorkKey, WALDORF_NAME);
    }

    // use Valueless Column pattern
    private void insertByCityIndex(String rowKey, String hotelName)
            throws Exception {

        long timestamp = System.nanoTime();

        Column nameCol = new Column(bytes(hotelName), bytes(""), timestamp);

        ColumnOrSuperColumn nameCosc = new ColumnOrSuperColumn();
        nameCosc.column = nameCol;

        Mutation nameMut = new Mutation();
        nameMut.column_or_supercolumn = nameCosc;

        // set up the batch
        Map<String, Map<String, List<Mutation>>> mutationMap = new HashMap<String, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> muts = new HashMap<String, List<Mutation>>();
        List<Mutation> cols = new ArrayList<Mutation>();
        cols.add(nameMut);

        String columnFamily = "HotelByCity";
        muts.put(columnFamily, cols);

        // outer map key is a row key
        // inner map key is the column family name
        mutationMap.put(rowKey, muts);

        // create representation of the column
        ColumnPath cp = new ColumnPath(columnFamily);
        cp.setColumn(hotelName.getBytes(UTF8));

        ColumnParent parent = new ColumnParent(columnFamily);
        // here, the column name IS the value (there's no value)
        Column col = new Column(bytes(hotelName), bytes(""), timestamp);

        client.insert(bytes(rowKey), parent, col, CL);

        LOG.debug("Inserted HotelByCity index for " + hotelName);

    } // end inserting ByCity index

    // POI
    public void insertAllPointsOfInterest() throws Exception {

        LOG.debug("Inserting POIs.");

        insertPOIEmpireState();
        insertPOICentralPark();
        insertPOIPhoenixZoo();
        insertPOISpringTraining();

        LOG.debug("Done inserting POIs.");
    }

    private void insertPOISpringTraining() throws Exception {
        Map<ByteBuffer, Map<String, List<Mutation>>> outerMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        List<Mutation> columnsToAdd = new ArrayList<Mutation>();

        long timestamp = System.nanoTime();
        String keyName = "Spring Training";
        Column descCol = new Column(bytes("desc"),
                bytes("Fun for baseball fans."), timestamp);
        Column phoneCol = new Column(bytes("phone"), bytes("623-333-3333"),
                timestamp);

        List<Column> cols = new ArrayList<Column>();
        cols.add(descCol);
        cols.add(phoneCol);

        Map<String, List<Mutation>> innerMap = new HashMap<String, List<Mutation>>();

        Mutation columns = new Mutation();
        ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
        SuperColumn sc = new SuperColumn();
        sc.name = bytes(CAMBRIA_NAME);
        sc.columns = cols;

        descCosc.super_column = sc;
        columns.setColumn_or_supercolumn(descCosc);

        columnsToAdd.add(columns);

        String superCFName = "PointOfInterest";
        ColumnPath cp = new ColumnPath();
        cp.column_family = superCFName;
        cp.setSuper_column(CAMBRIA_NAME.getBytes());
        cp.setSuper_columnIsSet(true);

        innerMap.put(superCFName, columnsToAdd);
        outerMap.put(bytes(keyName), innerMap);

        client.batch_mutate(outerMap, CL);

        LOG.debug("Done inserting Spring Training.");
    }

    private void insertPOIPhoenixZoo() throws Exception {

        Map<ByteBuffer, Map<String, List<Mutation>>> outerMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        List<Mutation> columnsToAdd = new ArrayList<Mutation>();

        long ts = System.currentTimeMillis();
        String keyName = "Phoenix Zoo";
        Column descCol = new Column(bytes("desc"),
                bytes("They have animals here."), ts);

        Column phoneCol = new Column(bytes("phone"), bytes("480-555-9999"), ts);

        List<Column> cols = new ArrayList<Column>();
        cols.add(descCol);
        cols.add(phoneCol);

        Map<String, List<Mutation>> innerMap = new HashMap<String, List<Mutation>>();

        String cambriaName = "Cambria Suites Hayden";

        Mutation columns = new Mutation();
        ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
        SuperColumn sc = new SuperColumn();
        sc.name = bytes(cambriaName);
        sc.columns = cols;

        descCosc.super_column = sc;
        columns.setColumn_or_supercolumn(descCosc);

        columnsToAdd.add(columns);

        String superCFName = "PointOfInterest";
        ColumnPath cp = new ColumnPath();
        cp.column_family = superCFName;
        cp.setSuper_column(cambriaName.getBytes());
        cp.setSuper_columnIsSet(true);

        innerMap.put(superCFName, columnsToAdd);
        outerMap.put(bytes(keyName), innerMap);

        client.batch_mutate(outerMap, CL);

        LOG.debug("Done inserting Phoenix Zoo.");
    }

    private void insertPOICentralPark() throws Exception {

        Map<ByteBuffer, Map<String, List<Mutation>>> outerMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();
        List<Mutation> columnsToAdd = new ArrayList<Mutation>();

        long ts = System.nanoTime();
        String keyName = "Central Park";
        Column descCol = new Column(bytes("desc"),
                bytes("Walk around in the park. It's pretty."), ts);

        // no phone column for park

        List<Column> cols = new ArrayList<Column>();
        cols.add(descCol);

        Map<String, List<Mutation>> innerMap = new HashMap<String, List<Mutation>>();

        Mutation columns = new Mutation();
        ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
        SuperColumn waldorfSC = new SuperColumn();
        waldorfSC.name = bytes(WALDORF_NAME);
        waldorfSC.columns = cols;

        descCosc.super_column = waldorfSC;
        columns.setColumn_or_supercolumn(descCosc);

        columnsToAdd.add(columns);

        String superCFName = "PointOfInterest";
        ColumnPath cp = new ColumnPath();
        cp.column_family = superCFName;
        cp.setSuper_column(WALDORF_NAME.getBytes());
        cp.setSuper_columnIsSet(true);

        innerMap.put(superCFName, columnsToAdd);
        outerMap.put(bytes(keyName), innerMap);

        client.batch_mutate(outerMap, CL);

        LOG.debug("Done inserting Central Park.");
    }

    private void insertPOIEmpireState() throws Exception {

        Map<ByteBuffer, Map<String, List<Mutation>>> outerMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        List<Mutation> columnsToAdd = new ArrayList<Mutation>();

        long ts = System.nanoTime();
        String esbName = "Empire State Building";
        Column descCol = new Column(bytes("desc"),
                bytes("Great view from 102nd floor."), ts);
        Column phoneCol = new Column(bytes("phone"), bytes("212-777-7777"), ts);

        List<Column> esbCols = new ArrayList<Column>();
        esbCols.add(descCol);
        esbCols.add(phoneCol);

        Map<String, List<Mutation>> innerMap = new HashMap<String, List<Mutation>>();

        Mutation columns = new Mutation();
        ColumnOrSuperColumn descCosc = new ColumnOrSuperColumn();
        SuperColumn waldorfSC = new SuperColumn();
        waldorfSC.name = bytes(WALDORF_NAME);
        waldorfSC.columns = esbCols;

        descCosc.super_column = waldorfSC;
        columns.setColumn_or_supercolumn(descCosc);

        columnsToAdd.add(columns);

        String superCFName = "PointOfInterest";
        ColumnPath cp = new ColumnPath();
        cp.column_family = superCFName;
        cp.setSuper_column(WALDORF_NAME.getBytes());
        cp.setSuper_columnIsSet(true);

        innerMap.put(superCFName, columnsToAdd);
        outerMap.put(bytes(esbName), innerMap);

        client.batch_mutate(outerMap, CL);

        LOG.debug("Done inserting Empire State.");
    }

    // convenience method runs all of the individual inserts
    public void insertAllHotels() throws Exception {

        String columnFamily = "Hotel";

        // row keys
        String cambriaKey = "AZC_043";
        String clarionKey = "AZS_011";
        String wKey = "CAS_021";
        String waldorfKey = "NYN_042";

        // conveniences
        Map<ByteBuffer, Map<String, List<Mutation>>> cambriaMutationMap = createCambriaMutation(
                columnFamily, cambriaKey);

        Map<ByteBuffer, Map<String, List<Mutation>>> clarionMutationMap = createClarionMutation(
                columnFamily, clarionKey);

        Map<ByteBuffer, Map<String, List<Mutation>>> waldorfMutationMap = createWaldorfMutation(
                columnFamily, waldorfKey);

        Map<ByteBuffer, Map<String, List<Mutation>>> wMutationMap = createWMutation(
                columnFamily, wKey);

        client.batch_mutate(cambriaMutationMap, CL);
        LOG.debug("Inserted " + cambriaKey);
        client.batch_mutate(clarionMutationMap, CL);
        LOG.debug("Inserted " + clarionKey);
        client.batch_mutate(wMutationMap, CL);
        LOG.debug("Inserted " + wKey);
        client.batch_mutate(waldorfMutationMap, CL);
        LOG.debug("Inserted " + waldorfKey);

        LOG.debug("Done inserting at " + System.nanoTime());
    }

    // set up columns to insert for W
    private Map<ByteBuffer, Map<String, List<Mutation>>> createWMutation(
            String columnFamily, String rowKey)
            throws UnsupportedEncodingException {

        long ts = System.nanoTime();

        Column nameCol = new Column(bytes("name"), bytes(W_NAME), ts);
        Column phoneCol = new Column(bytes("phone"), bytes("415-222-2222"), ts);
        Column addressCol = new Column(bytes("address"),
                bytes("181 3rd Street"), ts);
        Column cityCol = new Column(bytes("city"), bytes("San Francisco"), ts);
        Column stateCol = new Column(bytes("state"), bytes("CA"), ts);
        Column zipCol = new Column(bytes("zip"), bytes("94103"), ts);

        ColumnOrSuperColumn nameCosc = new ColumnOrSuperColumn();
        nameCosc.column = nameCol;

        ColumnOrSuperColumn phoneCosc = new ColumnOrSuperColumn();
        phoneCosc.column = phoneCol;

        ColumnOrSuperColumn addressCosc = new ColumnOrSuperColumn();
        addressCosc.column = addressCol;

        ColumnOrSuperColumn cityCosc = new ColumnOrSuperColumn();
        cityCosc.column = cityCol;

        ColumnOrSuperColumn stateCosc = new ColumnOrSuperColumn();
        stateCosc.column = stateCol;

        ColumnOrSuperColumn zipCosc = new ColumnOrSuperColumn();
        zipCosc.column = zipCol;

        Mutation nameMut = new Mutation();
        nameMut.column_or_supercolumn = nameCosc;
        Mutation phoneMut = new Mutation();
        phoneMut.column_or_supercolumn = phoneCosc;
        Mutation addressMut = new Mutation();
        addressMut.column_or_supercolumn = addressCosc;
        Mutation cityMut = new Mutation();
        cityMut.column_or_supercolumn = cityCosc;
        Mutation stateMut = new Mutation();
        stateMut.column_or_supercolumn = stateCosc;
        Mutation zipMut = new Mutation();
        zipMut.column_or_supercolumn = zipCosc;

        // set up the batch
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> muts = new HashMap<String, List<Mutation>>();
        List<Mutation> cols = new ArrayList<Mutation>();
        cols.add(nameMut);
        cols.add(phoneMut);
        cols.add(addressMut);
        cols.add(cityMut);
        cols.add(stateMut);
        cols.add(zipMut);

        muts.put(columnFamily, cols);

        // outer map key is a row key
        // inner map key is the column family name
        mutationMap.put(bytes(rowKey), muts);
        return mutationMap;
    }

    // add Waldorf hotel to Hotel CF
    private Map<ByteBuffer, Map<String, List<Mutation>>> createWaldorfMutation(
            String columnFamily, String rowKey)
            throws UnsupportedEncodingException {

        long ts = System.nanoTime();

        Column nameCol = new Column(bytes("name"), bytes(WALDORF_NAME), ts);
        Column phoneCol = new Column(bytes("phone"), bytes("212-555-5555"), ts);
        Column addressCol = new Column(bytes("address"), bytes("301 Park Ave"),
                ts);
        Column cityCol = new Column(bytes("city"), bytes("New York"), ts);
        Column stateCol = new Column(bytes("state"), bytes("NY"), ts);
        Column zipCol = new Column(bytes("zip"), bytes("10019"), ts);

        ColumnOrSuperColumn nameCosc = new ColumnOrSuperColumn();
        nameCosc.column = nameCol;

        ColumnOrSuperColumn phoneCosc = new ColumnOrSuperColumn();
        phoneCosc.column = phoneCol;

        ColumnOrSuperColumn addressCosc = new ColumnOrSuperColumn();
        addressCosc.column = addressCol;

        ColumnOrSuperColumn cityCosc = new ColumnOrSuperColumn();
        cityCosc.column = cityCol;

        ColumnOrSuperColumn stateCosc = new ColumnOrSuperColumn();
        stateCosc.column = stateCol;

        ColumnOrSuperColumn zipCosc = new ColumnOrSuperColumn();
        zipCosc.column = zipCol;

        Mutation nameMut = new Mutation();
        nameMut.column_or_supercolumn = nameCosc;
        Mutation phoneMut = new Mutation();
        phoneMut.column_or_supercolumn = phoneCosc;
        Mutation addressMut = new Mutation();
        addressMut.column_or_supercolumn = addressCosc;
        Mutation cityMut = new Mutation();
        cityMut.column_or_supercolumn = cityCosc;
        Mutation stateMut = new Mutation();
        stateMut.column_or_supercolumn = stateCosc;
        Mutation zipMut = new Mutation();
        zipMut.column_or_supercolumn = zipCosc;

        // set up the batch
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> muts = new HashMap<String, List<Mutation>>();
        List<Mutation> cols = new ArrayList<Mutation>();
        cols.add(nameMut);
        cols.add(phoneMut);
        cols.add(addressMut);
        cols.add(cityMut);
        cols.add(stateMut);
        cols.add(zipMut);

        muts.put(columnFamily, cols);

        // outer map key is a row key
        // inner map key is the column family name
        mutationMap.put(bytes(rowKey), muts);
        return mutationMap;
    }

    // set up columns to insert for Clarion
    private Map<ByteBuffer, Map<String, List<Mutation>>> createClarionMutation(
            String columnFamily, String rowKey)
            throws UnsupportedEncodingException {

        long ts = System.nanoTime();

        Column nameCol = new Column(bytes("name"), bytes(CLARION_NAME), ts);
        Column phoneCol = new Column(bytes("phone"), bytes("480-333-3333"), ts);
        Column addressCol = new Column(bytes("address"),
                bytes("3000 N. Scottsdale Rd"), ts);
        Column cityCol = new Column(bytes("city"), bytes("Scottsdale"), ts);
        Column stateCol = new Column(bytes("state"), bytes("AZ"), ts);
        Column zipCol = new Column(bytes("zip"), bytes("85255"), ts);

        ColumnOrSuperColumn nameCosc = new ColumnOrSuperColumn();
        nameCosc.column = nameCol;

        ColumnOrSuperColumn phoneCosc = new ColumnOrSuperColumn();
        phoneCosc.column = phoneCol;

        ColumnOrSuperColumn addressCosc = new ColumnOrSuperColumn();
        addressCosc.column = addressCol;

        ColumnOrSuperColumn cityCosc = new ColumnOrSuperColumn();
        cityCosc.column = cityCol;

        ColumnOrSuperColumn stateCosc = new ColumnOrSuperColumn();
        stateCosc.column = stateCol;

        ColumnOrSuperColumn zipCosc = new ColumnOrSuperColumn();
        zipCosc.column = zipCol;

        Mutation nameMut = new Mutation();
        nameMut.column_or_supercolumn = nameCosc;
        Mutation phoneMut = new Mutation();
        phoneMut.column_or_supercolumn = phoneCosc;
        Mutation addressMut = new Mutation();
        addressMut.column_or_supercolumn = addressCosc;
        Mutation cityMut = new Mutation();
        cityMut.column_or_supercolumn = cityCosc;
        Mutation stateMut = new Mutation();
        stateMut.column_or_supercolumn = stateCosc;
        Mutation zipMut = new Mutation();
        zipMut.column_or_supercolumn = zipCosc;

        // set up the batch
        Map<ByteBuffer, Map<String, List<Mutation>>> mutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> muts = new HashMap<String, List<Mutation>>();
        List<Mutation> cols = new ArrayList<Mutation>();
        cols.add(nameMut);
        cols.add(phoneMut);
        cols.add(addressMut);
        cols.add(cityMut);
        cols.add(stateMut);
        cols.add(zipMut);

        muts.put(columnFamily, cols);

        // outer map key is a row key
        // inner map key is the column family name
        mutationMap.put(bytes(rowKey), muts);
        return mutationMap;
    }

    // set up columns to insert for Cambria
    private Map<ByteBuffer, Map<String, List<Mutation>>> createCambriaMutation(
            String columnFamily, String cambriaKey)
            throws UnsupportedEncodingException {

        // set up columns for Cambria
        long ts = System.nanoTime();

        Column cambriaNameCol = new Column(bytes("name"),
                bytes("Cambria Suites Hayden"), ts);
        Column cambriaPhoneCol = new Column(bytes("phone"),
                bytes("480-444-4444"), ts);
        Column cambriaAddressCol = new Column(bytes("address"),
                bytes("400 N. Hayden"), ts);
        Column cambriaCityCol = new Column(bytes("city"), bytes("Scottsdale"),
                ts);
        Column cambriaStateCol = new Column(bytes("state"), bytes("AZ"), ts);
        Column cambriaZipCol = new Column(bytes("zip"), bytes("85255"), ts);

        ColumnOrSuperColumn nameCosc = new ColumnOrSuperColumn();
        nameCosc.column = cambriaNameCol;

        ColumnOrSuperColumn phoneCosc = new ColumnOrSuperColumn();
        phoneCosc.column = cambriaPhoneCol;

        ColumnOrSuperColumn addressCosc = new ColumnOrSuperColumn();
        addressCosc.column = cambriaAddressCol;

        ColumnOrSuperColumn cityCosc = new ColumnOrSuperColumn();
        cityCosc.column = cambriaCityCol;

        ColumnOrSuperColumn stateCosc = new ColumnOrSuperColumn();
        stateCosc.column = cambriaStateCol;

        ColumnOrSuperColumn zipCosc = new ColumnOrSuperColumn();
        zipCosc.column = cambriaZipCol;

        Mutation nameMut = new Mutation();
        nameMut.column_or_supercolumn = nameCosc;
        Mutation phoneMut = new Mutation();
        phoneMut.column_or_supercolumn = phoneCosc;
        Mutation addressMut = new Mutation();
        addressMut.column_or_supercolumn = addressCosc;
        Mutation cityMut = new Mutation();
        cityMut.column_or_supercolumn = cityCosc;
        Mutation stateMut = new Mutation();
        stateMut.column_or_supercolumn = stateCosc;
        Mutation zipMut = new Mutation();
        zipMut.column_or_supercolumn = zipCosc;

        // set up the batch
        Map<ByteBuffer, Map<String, List<Mutation>>> cambriaMutationMap = new HashMap<ByteBuffer, Map<String, List<Mutation>>>();

        Map<String, List<Mutation>> cambriaMuts = new HashMap<String, List<Mutation>>();
        List<Mutation> cambriaCols = new ArrayList<Mutation>();
        cambriaCols.add(nameMut);
        cambriaCols.add(phoneMut);
        cambriaCols.add(addressMut);
        cambriaCols.add(cityMut);
        cambriaCols.add(stateMut);
        cambriaCols.add(zipMut);

        cambriaMuts.put(columnFamily, cambriaCols);

        // outer map key is a row key
        // inner map key is the column family name
        cambriaMutationMap.put(bytes(cambriaKey), cambriaMuts);
        return cambriaMutationMap;
    }
}
