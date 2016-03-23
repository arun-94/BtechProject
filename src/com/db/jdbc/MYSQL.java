package com.db.jdbc;

import java.sql.Connection;
import java.sql.ResultSet;
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

public class MYSQL extends DB_JDBC{

	private static Logger logger=Logger.getLogger(MYSQL.class.getName());
	String databaseName="";
	@Override
	public String getSchemaName(Connection connection,String tableName) {
		// TODO Auto-generated method stub
		databaseName=super.databaseName;
		System.out.println("database Name "+databaseName);
		return databaseName;
	}
	
	@Override
	public String getRowComparisonQuery(String primaryKeyCol,String allColName,String schemaTable,int startCount,int offset) {
		String query="";
				
		query="select "+primaryKeyCol+" as PRIMARY_KEY ,"+allColName+" as VALUE " +
					"from "+schemaTable+" limit "+startCount+","+offset;
		
		logger.info("Query MYSQL "+query);
		return query;
	}//end getRowComparisonQuery()
	
	public String getColascedColName(LinkedList<String> allColumnList)
	{
		String colasceName="";
		for(int i=1;i<=allColumnList.size();i++)
		{
			if(i==allColumnList.size())
				colasceName+="coalesce("+allColumnList.get(i-1)+",-999) ";
			else
				colasceName+="coalesce("+allColumnList.get(i-1)+",-999),'#@@@#',";			
		}
				
		colasceName="concat("+colasceName+")";
		
		return colasceName;
	}

	public BloomFilter<CharSequence> addDataToBloomFilter(DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) 
	{
		QueryExecutor queryExecutor=new QueryExecutor();
		
		String primaryKeyCol=super.getColumnn_ROWQUERY(dbSchemaInfo.getPrimaryKeyColList());
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
				logger.error("Error Feacting Records", e);
			}
			
			startCount+=bucketSize;
		}
			
		return bloomFilter_src;
	}
	
	public LinkedList<String> verifyDataFromBloomFilter(
			BloomFilter<CharSequence> bloomFilter,
			DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) {
		// TODO Auto-generated method stub
		return null;
	}

	
	@Override
	public void createTemporaryTable(String tableName,String query, String orderByColums) {
		// TODO Auto-generated method stub
		String querytmp="create temporary table "+tableName+" as "+query+" "+orderByColums;
		
		logger.info("QUERY "+querytmp);
		
		try {			
			Statement statement=super.connection.createStatement();
			statement.execute(querytmp);
			
		} catch (SQLException e) {
			// TODO Auto-generated catch block
		logger.info("Exception while creating Temp table");
			e.printStackTrace();
		}
		
	}

	@Override
	public LinkedList<String> getAllColumnName_QE(String tableName,
			Connection connection) {
		// TODO Auto-generated method stub
		return null;
	}
	
	
}//end MYSQL
