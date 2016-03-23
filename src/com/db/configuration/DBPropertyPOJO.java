package com.db.configuration;

public class DBPropertyPOJO {

	private String drivername=null;
	private String connectionType=null;
	private String serverIP=null;
	private String portNumber=null;
	private String database=null;
	private String userName=null;
	private String password=null;
	private String dbType=null;
	
	public void setDriverName(String drivername)
	{
		this.drivername=drivername;
	}
	
	public String getDriverName()
	{
		return drivername;
	}
	
	public void setConnectionType(String connectionType)
	{
		this.connectionType=connectionType;
	}
	
	public String getConnectionType()
	{
		return connectionType;
	}
	
	public void setServerIP(String serverIP)
	{
		this.serverIP=serverIP;
	}
	
	public String getServerIP()
	{
		return serverIP;
	}
	
	public void setPortNumber(String portNumber)
	{
		this.portNumber=portNumber;
	}
	
	public String getPortNumber()
	{
		return portNumber;
	}
	
	public void setDatabaseName(String database)
	{
		this.database=database;
	}
	
	public String getDatabaseName()
	{
		return database;
	}
	
	public void setUsername(String username)
	{
		this.userName=username;
	}
	
	public String getUsername()
	{
		return userName;
	}
	
	public void setPassword(String password)
	{
		this.password=password;
	}
	
	public String getPassword()
	{
		return password;
	}
}
