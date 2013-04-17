package com.yahoo.ycsb.db;

import java.util.Properties;

import org.lightcouch.CouchDbProperties;

/**
 * 
 * Helper class It scans the YSCB properties objects for properties related to
 * couchDB connection string , if properties are not set it uses defaults as
 * follows : default DB = yscb , Default protocol = http , Default port = 5984 ,
 * Default host = localhost , Default username = "" , Default password=""
 * 
 * 
 */
public class Utils {

	private static final String COUCHDB_PASSWORD = "couchdb.password";
	private static final String COUCHDB_USERNAME = "couchdb.username";
	private static final String COUCHDB_PORT = "couchdb.port";
	private static final String COUCHDB_HOST = "couchdb.host";
	private static final String COUCHDB_PROTOCOL = "couchdb.protocol";
	private static final String COUCHDB_CREATEDB_IF_NOT_EXIST = "couchdb.createdb.if-not-exist";
	private static final String COUCHDB_NAME = "couchdb.name";

	public static CouchDbProperties getConnectionProperties(Properties ycsbProps) {

		CouchDbProperties couchDbProperties = new CouchDbProperties();
		String dbName = ycsbProps.getProperty(COUCHDB_NAME, "ycsb");
		boolean createDbIfNotExist = Boolean.parseBoolean(ycsbProps
				.getProperty(COUCHDB_CREATEDB_IF_NOT_EXIST, "true"));
		String protocol = ycsbProps.getProperty(COUCHDB_PROTOCOL, "http");
		String host = ycsbProps.getProperty(COUCHDB_HOST, "127.0.0.1");
		int port = Integer
				.parseInt(ycsbProps.getProperty(COUCHDB_PORT, "5984"));
		String username = ycsbProps.getProperty(COUCHDB_USERNAME, "");
		String password = ycsbProps.getProperty(COUCHDB_PASSWORD, "");

		couchDbProperties.setDbName(dbName);
		couchDbProperties.setCreateDbIfNotExist(createDbIfNotExist);
		couchDbProperties.setHost(host);
		couchDbProperties.setProtocol(protocol);
		couchDbProperties.setPort(port);
		couchDbProperties.setUsername(username);
		couchDbProperties.setPassword(password);

		return couchDbProperties;
	}
}
