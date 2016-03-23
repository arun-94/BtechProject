package com.db.configuration;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

public class TargetDBPropertyReader {

		
	private Properties property=new Properties();
	private InputStream input=null;
	
	public TargetDBPropertyReader()
	{
		try {
		
			input=new FileInputStream("./config/targetJDBC_DB.properties");
			property.load(input);
		
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception while creating input file object");
			e.printStackTrace();
			
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception while loading input file");
			e.printStackTrace();
		}
				
	}//end constructor TargetDatabase
	
	public DBPropertyPOJO getDBPOJO()
	{
		DBPropertyPOJO dbPOJO=new DBPropertyPOJO();
		
		dbPOJO.setDriverName(property.getProperty("driver_name"));
		dbPOJO.setConnectionType(property.getProperty("connection_type"));
		dbPOJO.setServerIP(property.getProperty("server_ip"));
		dbPOJO.setPortNumber(property.getProperty("port_no"));
		dbPOJO.setDatabaseName(property.getProperty("database"));
		dbPOJO.setUsername(property.getProperty("dbuser"));
		dbPOJO.setPassword(property.getProperty("dbpassword"));
		
		return dbPOJO;
	}

	
}
