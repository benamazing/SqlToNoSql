package com.nosql.writer;

import java.io.IOException;
import java.io.Reader;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Types;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.bson.Document;

import com.sql.util.ResultSetHandler;

public class MongoImportHandler implements ResultSetHandler{

	//default batch import size
	public final static int DEFAULT_BATCH_SIZE = 1000;
	private int batchSize;
	
	public MongoImportHandler(){
		batchSize = DEFAULT_BATCH_SIZE;
	}
	
	
	public MongoImportHandler(AbstractNosqlWriter writer, int batchSize){
		this.writer = writer;
		this.batchSize = batchSize;
		
	}
	
	public int getBatchSize() {
		return batchSize;
	}

	public void setBatchSize(int batchSize) {
		this.batchSize = batchSize;
	}

	private AbstractNosqlWriter writer;
	
	
	
	public AbstractNosqlWriter getWriter() {
		return writer;
	}



	public void setWriter(AbstractNosqlWriter writer) {
		this.writer = writer;
	}


	private Map<String, Object> toMap(ResultSet rs) throws SQLException {
		Map<String, Object> result = new HashMap<String, Object>();
		int columnCount = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= columnCount; i++){
			result.put(rs.getMetaData().getColumnName(i), rs.getObject(i));
		}
		return result;
		
	}
	
	private Object convertType(Object o) {
		if (o instanceof java.math.BigDecimal) {
			try {
				DecimalFormat df = new DecimalFormat();
				Number n = df.parse(o.toString());
				o = n;
			} catch (ParseException e) {
				e.printStackTrace();
			}
		}
		if (o instanceof java.sql.Date) {
			try {
				java.sql.Date date = (java.sql.Date) o;
				java.util.Date d = new java.util.Date(date.getTime());
				o = d;
			} catch (Exception e){
				e.printStackTrace();
			}
		}
		
		if (o instanceof java.sql.Timestamp) {
			try {
				java.sql.Timestamp date = (java.sql.Timestamp) o;
				java.util.Date d = new java.util.Date(date.getTime());
				o = d;
			} catch (Exception e){
				e.printStackTrace();
			}
		}		
		
		if (o instanceof java.sql.Clob) {
			Reader r = null;
			try {
				java.sql.Clob clob = (java.sql.Clob) o;
				r = clob.getCharacterStream();
				char[] c = new char[(int) clob.length()];
				r.read(c);
				String data = new String(c);
				o = data;
				r.close();
			} catch (Exception e){
				e.printStackTrace();
			} finally {
				if (null != r){
					try {
					r.close();
					} catch (Exception e){
						e.printStackTrace();
					}
					r = null;
				}
			}
		}
		return o;
	}
	
	private Document toDocument(ResultSet rs) throws SQLException{
		Document doc = new Document();
		int columnCount = rs.getMetaData().getColumnCount();
		for (int i = 1; i <= columnCount; i++){
			ResultSetMetaData rsmd = rs.getMetaData();
			int type = rsmd.getColumnType(i);
			switch (type){
			case Types.TIMESTAMP:
				doc.append(rs.getMetaData().getColumnName(i).toLowerCase(), convertType(rs.getTimestamp(i)));
				break;
			case Types.DATE:
				doc.append(rs.getMetaData().getColumnName(i).toLowerCase(), convertType(rs.getTimestamp(i)));
				break;
			case Types.BLOB:
				break;
			case Types.CLOB:
				doc.append(rs.getMetaData().getColumnName(i).toLowerCase(), convertType(rs.getClob(i)));
				break;
			default:
				doc.append(rs.getMetaData().getColumnName(i).toLowerCase(), convertType(rs.getObject(i)));
				break;
			}

		}
		return doc;
	}

	public void handle(ResultSet rs) {
		// TODO Auto-generated method stub
		if (null == rs) {
			System.out.println("ResultSet is null!");
			return;
		}

		List<Object> docs = new ArrayList<Object>();
		int i = 0;
		long start = System.currentTimeMillis();

		try {
			while (rs.next()) {
				docs.add(this.toDocument(rs));
				i++;
				if (i % batchSize == 0) {
					this.writer.writeToNoSql(docs);
					docs = new ArrayList<Object>();
				}
			}
			this.writer.writeToNoSql(docs);
			long end = System.currentTimeMillis();
			System.out.println("Rows inserted: " + i);
			System.out.println("Time cost: " + (end - start)/1000);
		} catch (Exception e) {
			e.printStackTrace();
		}

	}
	

}
