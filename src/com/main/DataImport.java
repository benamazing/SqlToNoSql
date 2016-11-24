package com.main;


import com.config.ConfigLoader;
import com.nosql.writer.AbstractNosqlWriter;
import com.nosql.writer.MongoWriter;
import com.nosql.writer.MongoImportHandler;
import com.sql.util.DBOperation;

public class DataImport {
	
	public static void main(String[] args) throws Exception{
		ConfigLoader c = new ConfigLoader();
		DBOperation db = new DBOperation(c);
		int batchSize = Integer.parseInt(c.getValue("batch_size"));
		try {
		db.connect();
		String sql = c.getValue("sql");
		AbstractNosqlWriter writer = new MongoWriter();
		writer.initConnection(c);
		MongoImportHandler handler = new MongoImportHandler(writer, batchSize);
		db.query(handler, sql);
		} catch (Exception e) {
			e.printStackTrace();
		} finally {
			db.close();
		}
	}

}
