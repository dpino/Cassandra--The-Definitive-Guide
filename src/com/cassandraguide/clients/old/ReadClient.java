package com.cassandraguide.clients.old;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.log4j.Logger;

/**
 * FIXME: Not working
 */

/**
 *
 */
public class ReadClient {
    private static final Logger LOG = Logger.getLogger(WriteClient.class);

    public static void main(String... args) {
        LOG.debug("Starting Java reader...");

        try {
            StorageService.instance.initClient();

            // sleep for gossip
            Thread.sleep(2000L);
        } catch (Exception ex) {
            throw new AssertionError(ex);
        }

        try {
            readData();

            LOG.debug("Reader all done.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void readData() throws IOException {
        LOG.debug("Reading data...");

        // this is the single column we want to read from
        Collection<ByteBuffer> column = new ArrayList<ByteBuffer>();
        column.add(bytes("mycol"));

        // start reading
        for (int i = 0; i < 10; i++) {
            List<ReadCommand> commands = new ArrayList<ReadCommand>();
            // ORIGINAL: SliceByNamesReadCommand readCommand = new
            // SliceByNamesReadCommand("Keyspace1", "key" + i, new
            // QueryPath("Standard1", null, null), cols);

            QueryPath path = new QueryPath("Standard2");
            ReadCommand readCommand = new SliceByNamesReadCommand("Keyspace1",
                    bytes("key" + i), path, column);

            readCommand.setDigestQuery(false);
            commands.add(readCommand);
            LOG.debug("Created read command.");
            try {
                List<Row> rows;
                try {
                    rows = StorageProxy.readProtocol(commands,
                            ConsistencyLevel.ANY);

                    LOG.debug("Found it!");

                    assert rows.size() == 1;
                    Row row = rows.get(0);
                    ColumnFamily cf = row.cf;
                    if (cf != null) {
                        for (IColumn col : cf.getSortedColumns()) {
                            LOG.debug("Read value: "
                                    + new String(col.name().array()) + " : "
                                    + new String(col.value().array()));
                        }
                    } else {
                        LOG.debug("Couldn't read anything!");
                    }

                } catch (InvalidRequestException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
            } catch (UnavailableException e) {
                throw new RuntimeException(e);
            } catch (TimeoutException e) {
                throw new RuntimeException(e);
            }
        }

        StorageService.instance.stopClient();

        LOG.debug("Done reading data.");
    }
}
