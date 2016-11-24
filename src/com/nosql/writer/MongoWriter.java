package com.nosql.writer;

import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.bson.Document;


import com.config.ConfigLoader;
import com.mongodb.MongoClient;
import com.mongodb.MongoCredential;
import com.mongodb.MongoException;
import com.mongodb.ServerAddress;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

public class MongoWriter extends AbstractNosqlWriter{
	
	MongoCollection<Document> collection = null;
	private MongoDatabase db;
	
	public MongoDatabase getDb() {
		return db;
	}
	
	@Override
	public void initConnection(ConfigLoader config) throws UnknownHostException, MongoException{
		String needAuth = config.getValue("mongo.useAuth");
		if (needAuth !=null && needAuth.equalsIgnoreCase("true")){
			initConnection(config.getValue("mongo.host"), Integer.parseInt(config.getValue("mongo.port")), config.getValue("mongo.db") ,
					config.getValue("mongo.user"), config.getValue("mongo.password"), config.getValue("mongo.authDB"));
		} else {
			initConnection( config.getValue("mongo.host"), Integer.parseInt(config.getValue("mongo.port")), config.getValue("mongo.db") );
		}
		initCollection(config.getValue("mongo.collection"));
	}
	

	private void initCollection(String collectionName) {
		collection = getDb().getCollection(collectionName);
	}
	
	private void initConnection(String host, int port, String dbName) throws UnknownHostException, MongoException {
		MongoClient m = new MongoClient(host, port);
		db = m.getDatabase(dbName);
	}
	
	private void initConnection(String host, int port, String dbName, String userName,
			String password, String authDB) {
		ServerAddress sa = new ServerAddress(host, port);
		List<MongoCredential> mongoCredentialList = new ArrayList<MongoCredential>();
		mongoCredentialList.add(MongoCredential.createMongoCRCredential(userName, authDB, password.toCharArray()));
		MongoClient m = new MongoClient(sa, mongoCredentialList);
		db = m.getDatabase(dbName);
		
	}	
	
	@Override
	public void writeToNoSql(List<Object> objectList) {
		//batch Insert
		if (null == objectList || objectList.size() == 0)
			return;
		
		List<Document> docs = new ArrayList<Document>();
		for (int i = 0; i < objectList.size(); i++){
			docs.add((Document) objectList.get(i));
		}
		
		collection.insertMany(docs);
		
	}

}
