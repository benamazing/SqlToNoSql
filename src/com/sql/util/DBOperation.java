package com.sql.util;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

//import oracle.jdbc.driver.OracleDriver;


import com.config.ConfigLoader;
import com.sql.util.ResultSetHandler;

public class DBOperation {
	
	private Connection conn;
	private String user;
	private String password;
	private String serverURL;
	
	private String driverName = "oracle.jdbc.driver.OracleDriver";
	
	public DBOperation(ConfigLoader config){
		this.user = config.getValue(ConfigLoader.DB_USER);
		this.password = config.getValue(ConfigLoader.DB_PASSWORD);
		this.serverURL = config.getValue(ConfigLoader.DB_URL);
		if (config.getValue(ConfigLoader.DRIVER_NAME)!= null){
			this.driverName = config.getValue(ConfigLoader.DRIVER_NAME);
		}
	}

	
	public void connect() {
		try {
			//DriverManager.registerDriver(new OracleDriver());
			Class.forName(this.driverName);
			this.conn = DriverManager.getConnection(serverURL, user, password);
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (ClassNotFoundException e){
			e.printStackTrace();
		}
		
	}

	public void close() throws SQLException
	{
		if (null != conn){
			conn.close();
			conn = null;
		}
	}
	
	public void query(ResultSetHandler handler, String sql) throws SQLException {
		if (null != conn){
			this.connect();
		}
		Statement s = conn.createStatement(ResultSet.TYPE_FORWARD_ONLY, ResultSet.CONCUR_READ_ONLY);
		ResultSet rset = s.executeQuery(sql);
		handler.handle(rset);
		

	}		
}
