package com.db.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.LinkedList;

import org.apache.log4j.Logger;

import com.db.DB_JDBC;
import com.db.reports.ReportPOJO;
import com.db.util.DatabaseSchemaPOJO;
import com.db.util.QueryExecutor;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

public class DB2 extends DB_JDBC{

	private static Logger logger=Logger.getLogger(DB2.class.getName());
	
	@Override
	public String getSchemaName(Connection connection,String tableName) {
		
		logger.info("In Class: DB2 Method: getSchemaName");
		
		String schemaName="";
		try {
			Statement statement=connection.createStatement();
			ResultSet resultSet=statement.executeQuery("Select tabschema from syscat.tables where tabname='"+tableName.toUpperCase()+"'");
			
			if(resultSet.next())
				logger.info(schemaName=resultSet.getString(1).trim());			
			
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return schemaName;
	}

	public BloomFilter<CharSequence> addDataToBloomFilter(DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) {
		logger.info("In Class: DB2 Method: addDataToBloomFilter()");
		
		QueryExecutor queryExecutor=new QueryExecutor();
		
		String primaryKeyCol=this.getColumnn_ROWQUERY(dbSchemaInfo.getPrimaryKeyColList());
		String allColName=this.getColascedColName(dbSchemaInfo.getAllColumnList());
		String schemaTableName=dbSchemaInfo.getSchemaName()+dbSchemaInfo.getTableName();
		String query="";
		ResultSet resultset=null;
		
		int totalRcordCount=reportInfo.getTotalRecordCount();	int startCount=0;	int bucketSize=500;
		BloomFilter<CharSequence> bloomFilter_src=BloomFilter.create(Funnels.stringFunnel(), totalRcordCount, 0.01);
		
		while(startCount<totalRcordCount)
		{
			query=this.getRowComparisonQuery(primaryKeyCol,allColName,schemaTableName, startCount, bucketSize);
			resultset=queryExecutor.executeQuery(super.connection, query);
				
						
			try {
				while(resultset.next())
				{
					bloomFilter_src.put(resultset.getString(2));
				}
			} catch (SQLException e) {
				// TODO Auto-generated catch block
				logger.error("Error Featcting Records", e);
			}
			
			startCount+=bucketSize;
		}
		
		return bloomFilter_src;
	}

	@Override
	public String getRowComparisonQuery(String primaryKeyCol,
			String allColName, String schemaTable, int startCount, int offset) {
		// TODO Auto-generated method stub
		logger.info("In Class: DB2 Method: getRowComparisonQuery()");
			
		String query="Select "+primaryKeyCol+" as PRIMARY_KEY,"+allColName+" as VALUE " +
		 			"from "+schemaTable+" limit "+startCount+","+offset;
		
		return query;
	}
	
	public String getColumnn_ROWQUERY(LinkedList<String> columnName)
	{
		logger.info("In Class: ORACLE Method: getColumnn_ROWQUERY()");
		String columns="";
		
		for(int index=1;index<=columnName.size();index++)
		  {
			  if(index==columnName.size())
			  {
				  columns+=columnName.get(index-1)+" ";
			  }
			  else
			  {
				  columns+=columnName.get(index-1)+" || ";
				  columns+="'#@@@#' || ";
			  }
		  }//end for
		
		return columns;
	}
	
	@Override
	public String getColascedColName(LinkedList<String> allColumnList) {
	// TODO Auto-generated method stub
		logger.info("In Class: DB2 Method: getColascedColName()");
		String colasceName="";
		for(int i=1;i<=allColumnList.size();i++)
		{
			if(i==allColumnList.size())
				colasceName+="nvl("+allColumnList.get(i-1)+",'-999') ";
			else
				colasceName+="nvl("+allColumnList.get(i-1)+",'-999') || '#@@@#' ||";			
		}
				
		return colasceName;		
	}

	public LinkedList<String> verifyDataFromBloomFilter(
			BloomFilter<CharSequence> bloomFilter,
			DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) {
		// TODO Auto-generated method stub
		logger.info("In Class: DB2 Method: verifyDataFromBloomFilter()");
		
		return null;
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
				resultSet=metadata.getColumns(null, null, tableName.toUpperCase(), null);
					
				
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
			resultSet=metadata.getPrimaryKeys(null, null, tableName.toUpperCase());
			
			while(resultSet.next())
			{
				primaryKeyList.add(resultSet.getString("COLUMN_NAME"));
			}
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
		}
				
		return primaryKeyList;
	}//end getPrimaryKeyColumn()

	@Override
	public void createTemporaryTable(String tableName,String query, String orderByColums) {
		// TODO Auto-generated method stub
		logger.info("In Class: DB2 Method: createTemporaryTable()");
		Statement stmt=null;
		ResultSet resultSet=null;
		String queryTMP="DECLARE GLOBAL TEMPORARY TABLE session."+tableName+" as ("+query+") definition only ON COMMIT PRESERVE ROWS NOT LOGGED";
		logger.info(queryTMP);
		try {
				stmt=super.connection.createStatement();
				stmt.executeUpdate(queryTMP);
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error(e.getMessage(), e);
		}
		
		
		queryTMP="insert into session."+tableName+" ( "+query+" order by "+orderByColums+")";
		logger.info(queryTMP);
		
		try {
			int flag=stmt.executeUpdate(queryTMP);
			 connection.commit();
			 
			 resultSet=stmt.executeQuery("select * from session."+tableName);
			 ResultSetMetaData rsmd = resultSet.getMetaData();
			 
			 
			 if(resultSet.next())
			 logger.info("Temp Table Created Successfully and Loaded with the Data ...");			 
			 else
				 logger.info(" Data Not loaded .. ");
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		
		
	}

	@Override
	public LinkedList<String> getAllColumnName_QE(String tableName,
			Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}
}//end DB2
