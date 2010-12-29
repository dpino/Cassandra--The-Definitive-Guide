package com.cassandraguide.clients.old;

import static org.apache.cassandra.utils.ByteBufferUtil.bytes;

import java.io.IOException;
import java.util.Arrays;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.log4j.Logger;

/**
 * FIXME: Not working
 */

/**
 *
 */
public class WriteClient {
    private static final Logger LOG = Logger.getLogger(WriteClient.class);

    public static void main(String... args) {
        LOG.debug("Starting Java writer...");

        try {
            StorageService.instance.initClient();

            // sleeping allows gossip to spread
            Thread.sleep(5000L);
        } catch (Exception ex) {
            throw new AssertionError(ex);
        }

        try {
            writeData();

            LOG.debug("Writer all done.");

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void writeData() throws IOException {
        LOG.debug("Writing data.");

        // write
        for (int i = 0; i < 10; i++) {
            RowMutation row = new RowMutation("Keyspace1", bytes("key" + i));
            ColumnPath cp = new ColumnPath("Standard2").setColumn(("mycol")
                    .getBytes());

            row.add(new QueryPath(cp), bytes("value" + i),
                    System.currentTimeMillis());

            try {
                StorageProxy.mutate(Arrays.asList(row), ConsistencyLevel.ONE);
                Thread.sleep(50L);
            } catch (Exception ex) {
                throw new AssertionError(ex);
            }
            LOG.debug("Wrote key" + i);
        }
        StorageService.instance.stopClient();

        LOG.debug("Done writing data.");
    }
}
