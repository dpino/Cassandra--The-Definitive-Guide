package com.cassandraguide.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

/**
 * NOTE: Create keyspace "Keyspace1" before running AuthExample
 *
 * [default@unknown] create keyspace Keyspace1;
 *
 * FIXME: Example shows no results, as credentials subsystem has changed since
 * version 0.7
 */

/**
 * How to connect if you've set up SimpleAuthenticator
 */
public class AuthExample {

    public static void main(String[] args) throws Exception {

        TTransport tr = new TSocket("localhost", 9160);
        TFramedTransport tf = new TFramedTransport(tr);
        TProtocol proto = new TBinaryProtocol(tf);
        Cassandra.Client client = new Cassandra.Client(proto);
        tr.open();

        AuthenticationRequest authRequest = new AuthenticationRequest();
        Map<String, String> credentials = new HashMap<String, String>();
        credentials.put("username", "jsmith");
        credentials.put("password", "havebadpass");
        authRequest.setCredentials(credentials);

        client.set_keyspace("Keyspace1");

        client.login(authRequest);
        tr.close();
    }
}
