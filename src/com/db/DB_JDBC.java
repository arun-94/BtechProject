package com.db;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.db.configuration.DBPropertyPOJO;
import com.db.jdbc.MYSQL;
import com.db.reports.ReportPOJO;
import com.db.util.DatabaseSchemaPOJO;
import com.db.util.QueryExecutor;

public abstract class DB_JDBC implements ETLValidator{

	private static Logger logger=Logger.getLogger(DB_JDBC.class.getName());
	
	protected Connection connection;
	
	protected String databaseName="";
	
	public Connection connectToDB(DBPropertyPOJO dbProperty)
	{
		String drivername=dbProperty.getDriverName();
		String connectionURL=dbProperty.getConnectionType()+dbProperty.getServerIP()+dbProperty.getPortNumber()+dbProperty.getDatabaseName();
		String username=dbProperty.getUsername();
		String password=dbProperty.getPassword();
		
		databaseName=dbProperty.getDatabaseName();
		
		logger.info("---**********----------");
		logger.info(drivername+" "+connectionURL+" "+username+" "+password);
		
			try {
				Class.forName(drivername);
				connection=DriverManager.getConnection(connectionURL,username,password);
				
			} catch (ClassNotFoundException e) {
				// TODO Auto-generated catch block
				System.out.println("Exception while registering driver"+drivername);
				e.printStackTrace();
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				System.out.println("Exception while creating connection to"+connectionURL);
				e.printStackTrace();
			}
		System.out.println(connection);
		
		return connection;
	}

	public LinkedList<String> getAllColumnName(String tableName,Connection connection)
	{
			DatabaseMetaData metadata=null;
			ResultSet resultSet=null;
			LinkedList<String> allColumnName=new LinkedList<String>();
			System.out.println(tableName);
			try {
				System.out.println(connection);
				metadata=connection.getMetaData();
				resultSet=metadata.getColumns(null, null, tableName, null);
					
				
				while(resultSet.next())
				{
				    allColumnName.add(resultSet.getString("COLUMN_NAME"));
					//System.out.println(resultSet.getString("COLUMN_NAME"));
				}//end while
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		
		return allColumnName;
		
	}//end getAllColumnName
	
	public LinkedList<String> getPrimaryKeyColumn(String tableName,Connection connection)
	{
		ResultSet resultSet=null;
		LinkedList<String> primaryKeyList=new LinkedList<String>();
		DatabaseMetaData metadata=null;
				
		try {
			metadata=connection.getMetaData();
			resultSet=metadata.getPrimaryKeys(null, null, tableName);
			
			while(resultSet.next())
			{
				primaryKeyList.add(resultSet.getString("COLUMN_NAME"));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
				
		return primaryKeyList;
	}//end getPrimaryKeyColumn()
	
	public abstract String getSchemaName(Connection connection,String tableName);
	
	public abstract String getRowComparisonQuery(String primaryKeyCol,String allColName,String schemaTable,int startCount,int offset);
	
	public abstract String getColascedColName(LinkedList<String> allColumnList);
	
	public abstract void createTemporaryTable(String tableName,String query,String orderByColums); 
	
	public abstract LinkedList<String> getAllColumnName_QE(String tableName,Connection connection);
	
	public String getColumnn_ROWQUERY(LinkedList<String> columnName)
	{
		String columns="";
		
		for(int index=1;index<=columnName.size();index++)
		  {
			  if(index==columnName.size())
			  {
				  columns+=columnName.get(index-1)+" ";
			  }
			  else
			  {
				  columns+=columnName.get(index-1)+",";
				  columns+="'#@@@#',";
			  }
		  }//end for
		
		if(columnName.size()>1)
		  {
			  columns="concat("+columns+")";  
		  }
		return columns;
	}
	
	public void setTotalRecordCount(DatabaseSchemaPOJO dbSchemaInfo,ReportPOJO reportInfo)
	{
		QueryExecutor queryExecutor=new QueryExecutor();
		ResultSet resultset=null;
		String query="Select count(*) from "+dbSchemaInfo.getSchemaName()+dbSchemaInfo.getTableName();
		
		int totalRecordCount=0;
		
		resultset=queryExecutor.executeQuery(connection, query);
			try {
				if(resultset.next())
				{
					totalRecordCount=Integer.parseInt(resultset.getString(1));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("Error while Feating ResultSet ", e);
			}
			reportInfo.setTotalRecordCount(totalRecordCount);
		//return totalRecordCount;
	}
	
	public void closeConnection()
	{
		try {
			connection.close();
		} catch (SQLException e) {
			System.out.println("Error while closing connection");
		}
	}
	
}//end DB_JDBC
