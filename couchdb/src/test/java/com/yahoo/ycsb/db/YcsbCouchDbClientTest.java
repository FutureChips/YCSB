package com.yahoo.ycsb.db;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Vector;

import org.junit.Test;

import com.yahoo.ycsb.ByteIterator;
import com.yahoo.ycsb.DBException;
import com.yahoo.ycsb.StringByteIterator;

public class YcsbCouchDbClientTest {

	private static final String INSERT_ID = "12346";

	@Test
	public void insertRecordTest() throws DBException {
		//given
		YcsbCouchDbClient client = new YcsbCouchDbClient();
		client.init();
		HashMap<String, String> stringFields = new HashMap<String, String>();
		stringFields.put("fName", "YCSB");
		stringFields.put("lName", "COUCH DB");

		HashMap<String, ByteIterator> fields = StringByteIterator
				.getByteIteratorMap(stringFields);
		//when
		int retVal = client.insert("ycsb", INSERT_ID, fields);
		//then
		assertThat(retVal, is(0));
		
		//inserting same key again should fail with return value of 1
		retVal = client.insert("ycsb", INSERT_ID, fields);
		assertThat(retVal, is(1));
		
		client.cleanup();
	}
	@Test
	public void scanTest() throws DBException{
		
		YcsbCouchDbClient client = new YcsbCouchDbClient();
		client.init();
		
		Set<String> fields = new HashSet<String>(Arrays.asList("fName","lName"));
		Vector<HashMap<String, ByteIterator>> result = new Vector<HashMap<String,ByteIterator>>();
		
		int retVal = client.scan("ycsb", INSERT_ID,1, fields, result);
		
		assertThat(retVal, is(0));
		
		assertThat(result.size(), is(1));
		
		HashMap<String,ByteIterator> element = result.elementAt(0);
		
		HashMap<String, String> stringValues = StringByteIterator
				.getStringMap(element);
		
		assertThat(stringValues.get("fName"),is("YCSB"));
		assertThat(stringValues.get("lName"),is("COUCH DB"));
		client.cleanup();
	}
	@Test
	public void readRecordSuccessTest() throws DBException {
		//given
		YcsbCouchDbClient client = new YcsbCouchDbClient();
		
		client.init();
		
		Set<String> fields = new HashSet<String>(Arrays.asList("fName","lName"));
		HashMap<String, ByteIterator> returnValues = new HashMap<String, ByteIterator>();
		
	
		
		//when
		int retVal = client.read("ycsb", INSERT_ID, fields , returnValues);
		//then
		assertThat(retVal, is(0));
		
		HashMap<String, String> stringValues = StringByteIterator
				.getStringMap(returnValues);
		
		assertThat(stringValues.get("fName"),is("YCSB"));
		assertThat(stringValues.get("lName"),is("COUCH DB"));
		client.cleanup();
	}
	@Test
	public void readRecordFailureTest() throws DBException {
		//given
		YcsbCouchDbClient client = new YcsbCouchDbClient();
		
		client.init();
		
		Set<String> fields = new HashSet<String>(Arrays.asList("fName","lName"));
		HashMap<String, ByteIterator> returnValues = new HashMap<String, ByteIterator>();
		
		String notExistingId = "12345";
	
		
		//when
		int retVal = client.read("ycsb", notExistingId, fields , returnValues);
		//then
		assertThat(retVal, is(1));
		
		client.cleanup();
	}
	
	@Test
	public void updateRecordTest() throws DBException {
		//given
		YcsbCouchDbClient client = new YcsbCouchDbClient();
		client.init();
		HashMap<String, String> stringFields = new HashMap<String, String>();
		stringFields.put("fName", "YCSB-UPDATED");
		stringFields.put("lName", "COUCH DB-UPDATED");

		HashMap<String, ByteIterator> fields = StringByteIterator
				.getByteIteratorMap(stringFields);
		//when
		int retVal = client.update("ycsb", INSERT_ID, fields);
		//then
		assertThat(retVal, is(0));
		
		client.cleanup();
	}
	
	@Test
	public void deleteRecordTest() throws DBException {
		//given
		YcsbCouchDbClient client = new YcsbCouchDbClient();
		
		client.init();
		
		//when
		int retVal = client.delete("ycsb", INSERT_ID);
		//then
		assertThat(retVal, is(0));
		
		client.cleanup();
	}

}
