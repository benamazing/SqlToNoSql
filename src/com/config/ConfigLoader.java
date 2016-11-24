package com.config;


import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

public class ConfigLoader {
	public static String DB_USER = "user";
	public static String DB_PASSWORD = "password";
	public static String DB_URL = "url";
	public static String DRIVER_NAME = "driver_name";
	
	
	private Properties props = null;
	private static String DEFAULT_PATH = "res/import.properties";
	public ConfigLoader(String configFilePath) {
		File configFile = new File(configFilePath);
		if (configFile.exists() && configFile.isFile()) {
			try {
				props = new Properties();
				props.load(new FileInputStream(configFile));
			} catch (Exception e) {
			}
		}

	}
	
	public ConfigLoader(){
		File configFile = new File(DEFAULT_PATH);
		if (configFile.exists() && configFile.isFile()) {
			try {
				props = new Properties();
				props.load(new FileInputStream(configFile));
			} catch (Exception e) {
			}
		}
		
	}
	
	public String getValue(String property){
		if (null != props){
			return props.getProperty(property);
		}
		return null;
	}
	

}
