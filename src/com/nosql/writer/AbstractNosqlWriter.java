package com.nosql.writer;

import java.util.List;
import java.util.Map;
import java.util.ResourceBundle;

import org.bson.Document;

import com.config.ConfigLoader;

public class AbstractNosqlWriter {
	private String primaryKey = null;

	public String getPrimaryKey() {
		return primaryKey;
	}

	public void setPrimaryKey(String primaryKey) {
		this.primaryKey = primaryKey;
	}
	
	public void writeToNoSql(List<Object> objectList) throws Exception {
		
	}
	
	public void initConnection(ConfigLoader config) throws Exception {
		
	}
	

}
