package com.cassandraguide.config;

import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.thrift.AccessLevel;
import org.apache.cassandra.thrift.AuthenticationRequest;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.TFramedTransport;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;

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
		
		AccessLevel access = client.login(authRequest);
		System.out.println("ACCESS LEVEL: " + access);
		tr.close();
	}
}
