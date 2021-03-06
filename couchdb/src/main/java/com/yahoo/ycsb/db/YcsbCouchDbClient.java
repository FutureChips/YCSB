package com.yahoo.ycsb.db;

import java.util.HashMap;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;
import java.util.Vector;

import org.lightcouch.CouchDbClient;
import org.lightcouch.CouchDbProperties;
import org.lightcouch.DesignDocument;
import org.lightcouch.DocumentConflictException;
import org.lightcouch.NoDocumentException;
import org.lightcouch.CouchDbException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DB;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

/**
 * 
 * Database Interface Layer for CouchDB
 * 
 */
public class YcsbCouchDbClient extends DB {
        public static final String OPERATION_RETRY_PROPERTY = "couchdb.operationretries";
        public static final String OPERATION_RETRY_PROPERTY_DEFAULT = "300";
        public static final String BATCH_INSERT_PROPERTY = "couchdb.batchinsert";
        public static final String BATCH_INSERT_PROPERTY_DEFAULT = "false";
        public static final String BULK_INSERT_PROPERTY = "couchdb.bulkinsert";
        public static final String BULK_INSERT_PROPERTY_DEFAULT = "0";

	private CouchDbClient dbClient;
	private CouchDbProperties couchDbProperties;
	private static final Logger LOGGER = LoggerFactory
			.getLogger(YcsbCouchDbClient.class);
        private static Object lock = new Object();
        private static int min_sleep_wait = 250;
        private static int max_sleep_wait=500;
        CouchDbException errorexception = null;
        int OperationRetries;
        boolean batch_insert;
        int bulk_insert_count;

        HashMap <String, List<HashMap<String,String> > > pending_bulk_inserts;
	/* YCSB requires a no-arg constructor */
	public YcsbCouchDbClient() {
            pending_bulk_inserts = new HashMap <String, List<HashMap<String,String> > >();
	}

	public void init() throws DBException {
		/*
		 * we can get the properties passed to YCSB on commandline or in
		 * properties files and extract couchdb connection properties from them
		 */
                synchronized( lock ){
                        Properties ycsbProps = getProperties();
                        couchDbProperties = Utils.getConnectionProperties(ycsbProps);
                        OperationRetries = Integer.parseInt(ycsbProps.getProperty(OPERATION_RETRY_PROPERTY,
                                                                                  OPERATION_RETRY_PROPERTY_DEFAULT));
                        batch_insert = Boolean.parseBoolean(ycsbProps.getProperty(BATCH_INSERT_PROPERTY,
                                                                                  BATCH_INSERT_PROPERTY_DEFAULT));
                        bulk_insert_count = Integer.parseInt(ycsbProps.getProperty(BULK_INSERT_PROPERTY,
                                                                                   BULK_INSERT_PROPERTY_DEFAULT));
                        int sleep_wait = min_sleep_wait;
                        for (int i = 0; i < OperationRetries; ++i){
                                try{
                                        dbClient = new CouchDbClient(couchDbProperties);
                                        break;
                                }catch( CouchDbException e ){
                                        LOGGER.info("Detected exception:"+e.toString()+" when instantiating the client");
                                        errorexception = e;                
                                }
                                try{
                                        Thread.sleep(sleep_wait);
                                        // In line with YCSB implementation, double-sleep time but keep less than 5 minutes
                                        if( sleep_wait < max_sleep_wait ){
                                                sleep_wait *= 2;
                                        }
                                }catch(InterruptedException e){}
                                throw new DBException(errorexception);
                        }
                        for (int i = 0; i < OperationRetries; ++i){
                                try{
                                        DesignDocument exampleDoc = dbClient.design().getFromDesk("scan");
                                        dbClient.design().synchronizeWithDb(exampleDoc);
                                        break;
                                }catch( CouchDbException e ){
                                        LOGGER.info("Detected exception:"+e.toString()+" when setting up design doc for scan");
                                        errorexception = e;
                                }
                                try{
                                        Thread.sleep(sleep_wait);
                                        // In line with YCSB implementation, double-sleep time but keep less than 5 minutes
                                        if( sleep_wait < max_sleep_wait ){
                                                sleep_wait *= 2;
                                        }
                                }catch(InterruptedException e){}
                                throw new DBException(errorexception);
                        }
                }
	}

        private int send_bulk_insert(String table, List<HashMap<String,String>> reqs){
		/*
		 * we will set tablename = database name since couchdb is one giant data
		 * store with no notion of tables/collections
		 */

		table = couchDbProperties.getDbName();

                int sleep_wait = min_sleep_wait;
                for (int i = 0; i < OperationRetries; ++i){
                        try{
                                boolean all_or_nothing = false;
                                dbClient.bulk(reqs, all_or_nothing);
                                return 0; // Even if we look at the errors, there isn't much we can do.
                        }catch( CouchDbException e ){
                                LOGGER.info("Detected exception:"+e.toString()+" when attempting bulk insert");
                                errorexception = e;                
                        }
                        try{
                                Thread.sleep(sleep_wait);
                                // In line with YCSB implementation, double-sleep time but keep less than 5 minutes
                                if( sleep_wait < max_sleep_wait ){
                                        sleep_wait *= 2;
                                }
                        }catch(InterruptedException e){}            
                }
                LOGGER.error("Failed to bulk insert operation retry count exceeded");
                throw errorexception;                
        }
        
	private int aggregate_insert(String table, String key,
			HashMap<String, ByteIterator> fields) throws CouchDbException{
                // First we must aggregate the requests
                List<HashMap<String,String>> list = pending_bulk_inserts.get(table);
                // Table does not exist, create it
                if( list == null ){
                        list = new ArrayList<HashMap<String,String>>();
                        pending_bulk_inserts.put(table, list);
                }
		/*
		 * convert the String,ByteIterator map passed in by YCSB to
		 * String,String Map since lightCouch will accept String,String map
		 */
		HashMap<String, String> stringFields = StringByteIterator
				.getStringMap(fields);
		stringFields.put("_id", key);
                list.add(stringFields);
                if( list.size() >= bulk_insert_count ){
                        int retval =  send_bulk_insert(table, list);
                        list.clear();
                        return retval;
                }
                return 0;
        }

	private int single_insert(String table, String key,
			HashMap<String, ByteIterator> fields) throws CouchDbException{
                
		/*
		 * we will set tablename = database name since couchdb is one giant data
		 * store with no notion of tables/collections
		 */

		table = couchDbProperties.getDbName();
		/*
		 * convert the String,ByteIterator map passed in by YCSB to
		 * String,String Map since lightCouch will accept String,String map
		 */
		HashMap<String, String> stringFields = StringByteIterator
				.getStringMap(fields);

		stringFields.put("_id", key);

                int sleep_wait = min_sleep_wait;
                for (int i = 0; i < OperationRetries; ++i){
                        try{
                                try {
                                        if( batch_insert ){
                                                dbClient.batch(stringFields);
                                        }else{
                                                dbClient.save(stringFields);
                                        }
                                } catch (DocumentConflictException ex) {
                                        /* Document with the same id already existed , return non-zero value */
                                        LOGGER.error("record with {} key already exists", key);
                                        return 1;
                                }
                                return 0;
                        }catch( CouchDbException e ){
                                LOGGER.info("Detected exception:"+e.toString()+" when attempting to insert key {}", key);
                                errorexception = e;                
                        }
                        try{
                                Thread.sleep(sleep_wait);
                                // In line with YCSB implementation, double-sleep time but keep less than 5 minutes
                                if( sleep_wait < max_sleep_wait ){
                                        sleep_wait *= 2;
                                }
                        }catch(InterruptedException e){}            
                }
                LOGGER.error("Failed to read key {}, operation retry count exceeded", key);
                throw errorexception;
        }
	/**
     * Insert a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key.
     *
     * @param table The name of the table
     * @param key The record key of the record to insert.
     * @param values A HashMap of field/value pairs to insert in the record
     * @return Zero on success, a non-zero error code on error. 
     */
	@Override
	public int insert(String table, String key,
			HashMap<String, ByteIterator> fields){
                if( bulk_insert_count == 0 ){
                        return single_insert( table, key, fields);
                }else{
                        return aggregate_insert( table, key, fields);                        
                }
	}

	/**
     * Read a record from the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param key The record key of the record to read.
     * @param fields The list of fields to read, or null for all of them
     * @param result A HashMap of field/value pairs for the result
     * @return Zero on success, a non-zero error code on error or "not found".
     */
	
	@Override
	public int read(String table, String key, Set<String> fields,
			HashMap<String, ByteIterator> result) throws CouchDbException{
		/*
		 * we will set tablename = database name since couchdb is one giant data
		 * store with no notion of tables/collections
		 */

		table = couchDbProperties.getDbName();
                int sleep_wait = min_sleep_wait;
                for (int i = 0; i < OperationRetries; ++i){
                        try{
                                try {
                                        /* Read the record as JsonObject */
                                        JsonObject record = dbClient.find(JsonObject.class, key);

                                        /*
                                         * Iterate the JSON object and add the key,value pair to result map
                                         * . fields set is null then add all fields to result map ,
                                         * otherwise add only those fields which exist in fields Set
                                         */
                                        for (Entry<String, JsonElement> element : record.entrySet()) {
                                                String name = element.getKey();
                                                if (fields == null || fields.contains(name)) {
                                                        result.put(name, new StringByteIterator(element.getValue()
                                                                                                .getAsString()));
                                                }
                                        }

                                } catch (NoDocumentException ex) {
                                        LOGGER.error(
                                                     "tried to read document with key {} , but document does not exist",
                                                     key);
                                        return 1;
                                }
                                return 0;
                        }catch( CouchDbException e ){
                                LOGGER.info("Detected exception:"+e.toString()+" when attempting to read key {}", key);
                                errorexception = e;                
                        }
                        try{
                                Thread.sleep(sleep_wait);
                                // In line with YCSB implementation, double-sleep time but keep less than 5 minutes
                                if( sleep_wait < max_sleep_wait ){
                                        sleep_wait *= 2;
                                }
                        }catch(InterruptedException e){}            
                }
                LOGGER.error("Failed to read key {}, operation retry count exceeded", key);
                throw errorexception;
	}
	
	 /**
     * Delete a record from the database.
     *
     * @param table The name of the table
     * @param key The record key of the record to delete.
     * @return Zero on success, a non-zero error code on error
     */
	
	@Override
	public int delete(String table, String key) throws CouchDbException{
		/*
		 * we will set tablename = database name since couchdb is one giant data
		 * store with no notion of tables/collections
		 */

		table = couchDbProperties.getDbName();
                int sleep_wait = min_sleep_wait;
                for (int i = 0; i < OperationRetries; ++i){
                        try{
                                try {
                                        /*
                                         * In CouchDB , to delete a record , you must know its revision
                                         * number , since YCSB will only pass the key , we will first have
                                         * to find the document with that key and then delete that whole
                                         * document. Deletion just by knowing key of record is not possible
                                         */
                                        JsonObject record = dbClient.find(JsonObject.class, key);
                                        dbClient.remove(record);
                                } catch (NoDocumentException ex) {
                                        LOGGER.error(
                                                     "tried to remove document with key {} , but document does not exist",
                                                     key);
                                        return 1;
                                }
                                return 0;
                        }catch( CouchDbException e ){
                                LOGGER.info("Detected exception:"+e.toString()+" when attempting to delete key {}", key);
                                errorexception = e;                
                        }
                        try{
                                Thread.sleep(sleep_wait);
                                // In line with YCSB implementation, double-sleep time but keep less than 5 minutes
                                if( sleep_wait < max_sleep_wait ){
                                        sleep_wait *= 2;
                                }
                        }catch(InterruptedException e){}            
                }
                LOGGER.error("Failed to delete key {}, retries exceeded limit", key);
                throw errorexception;
	}
	
	 /**
     * Update a record in the database. Any field/value pairs in the specified values HashMap will be written into the record with the specified
     * record key, overwriting any existing values with the same field name.
     *
     * @param table The name of the table
     * @param key The record key of the record to write.
     * @param values A HashMap of field/value pairs to update in the record
     * @return Zero on success, a non-zero error code on error. See this class's description for a discussion of error codes.
     */
	
	@Override
	public int update(String table, String key,
			HashMap<String, ByteIterator> fields) throws CouchDbException{

		/*
		 * we will set tablename = database name since couchdb is one giant data
		 * store with no notion of tables/collections
		 */
		table = couchDbProperties.getDbName();
                int sleep_wait = min_sleep_wait;
                for (int i = 0; i < OperationRetries; ++i){
                        try{
                                /*
                                 * Same as with deletion function , we need to first find the record to
                                 * read its revision
                                 */
                                JsonObject record;
                                try {
                                        record = dbClient.find(JsonObject.class, key);
                                } catch (NoDocumentException ex) {
                                        LOGGER.error(
                                                     "tried to update document with key {} , but document does not exist",
                                                     key);
                                        return 1;
                                }
                                HashMap<String, String> stringFields = StringByteIterator
                                        .getStringMap(fields);

                                stringFields.put("_id", key);
                                stringFields.put("_rev", record.get("_rev").getAsString());
                                dbClient.update(stringFields);
                                return 0;
                        }catch( CouchDbException e ){
                                LOGGER.info("Detected exception:"+e.toString()+" when attempting to update key {}", key);
                                errorexception = e;                
                        }
                        try{
                                Thread.sleep(sleep_wait);
                                // In line with YCSB implementation, double-sleep time but keep less than 5 minutes
                                if( sleep_wait < max_sleep_wait ){
                                        sleep_wait *= 2;
                                }
                        }catch(InterruptedException e){}            
                }
                LOGGER.error("Failed to update key {}, retries exceeded limit", key);
                throw errorexception;
        }
	
	  /**
     * Perform a range scan for a set of records in the database. Each field/value pair from the result will be stored in a HashMap.
     *
     * @param table The name of the table
     * @param startkey The record key of the first record to read.
     * @param recordcount The number of records to read
     * @param fields The list of fields to read, or null for all of them
     * @param result A Vector of HashMaps, where each HashMap is a set field/value pairs for one record
     * @return Zero on success, a non-zero error code on error. 
     */
	
	@Override
	public int scan(String table, String startKey, int recordCount,
			Set<String> fields, Vector<HashMap<String, ByteIterator>> result) {
		LOGGER.info("scan: starting key {} size {}", startKey , recordCount);
		/*
		 * Get all the records contained in our id view, id view (defined in
		 * src/main/resources/scan/views/id/map.js will keep a sorted list of
		 * all entries by id
		 */
		List<JsonObject> records = dbClient.view("scan/id").includeDocs(true)
				.startKey(startKey).limit(recordCount).query(JsonObject.class);

		/* Iterate over records , add records to result Vector */
		for (JsonObject record : records) {
			HashMap<String, ByteIterator> value = new HashMap<String, ByteIterator>();
			for (Entry<String, JsonElement> element : record.entrySet()) {
				String name = element.getKey();
				if (fields == null || fields.contains(name)) {
					value.put(name, new StringByteIterator(element.getValue()
							.getAsString()));
				}
			}
			result.add(value);
		}
		LOGGER.info("returning records of size {}", result.size());
		return 0;
	}

	@Override
	public void cleanup() {
                // First cler out any pending insert requests
                Iterator<String> it = pending_bulk_inserts.keySet().iterator();
                while( it.hasNext() ){
                        String table = it.next();
                        List<HashMap<String, String> > reqs = pending_bulk_inserts.get(table);
                        if( reqs.size() != 0 ){
                                send_bulk_insert(table, reqs);
                        }
                        reqs.clear();
                }
                // Shutdown the client
		dbClient.shutdown();
	}

}
