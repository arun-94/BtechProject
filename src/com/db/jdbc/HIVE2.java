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

public class HIVE2 extends DB_JDBC{

	private static Logger logger=Logger.getLogger(DB_JDBC.class.getName());
	
	@Override
	public String getSchemaName(Connection connection,String tableName) {
		// TODO Auto-generated method stub
		String databaseName="";
		
		databaseName=super.databaseName;
		System.out.println("database name"+databaseName);
		return databaseName;
	}
	
	public BloomFilter<CharSequence> addDataToBloomFilter(DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getRowComparisonQuery(String primaryKeyCol,String allColName, String schemaTable, int startCount, int offset) {
		
		String query="";
		
		query="select "+primaryKeyCol+" as PRIMARY_KEY ,"+allColName+" as VALUE " +
					"from "+schemaTable+" as t where "+primaryKeyCol+" is not null and "+primaryKeyCol+" not in";
		
		System.out.println("Query "+query);
		return query;
	}

	public String getInnerQuery(DatabaseSchemaPOJO dbSchemaInfo) {
		// TODO Auto-generated method stub
		String primaryKeyCol=super.getColumnn_ROWQUERY(dbSchemaInfo.getPrimaryKeyColList());
		String query=" (select "+primaryKeyCol+" from "+dbSchemaInfo.getSchemaName()+dbSchemaInfo.getTableName()+" group by("+primaryKeyCol+") having count("+primaryKeyCol+") >1 )";
		return query;
	}

	public LinkedList<String> verifyDataFromBloomFilter(BloomFilter<CharSequence> bloomFilter,DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) {
		
		QueryExecutor queryExecutor=new QueryExecutor();
		LinkedList<String>  misplacedRecord=new LinkedList<String>();
		String primaryKeyCol=this.getColumnn_ROWQUERY(dbSchemaInfo.getPrimaryKeyColList());
		String allColName=this.getColascedColName(dbSchemaInfo.getAllColumnList());
		String schemaTableName=dbSchemaInfo.getSchemaName()+dbSchemaInfo.getTableName();
		String query="",innerQuery="";
		ResultSet resultset=null;
		
		int totalRcordCount=reportInfo.getTotalRecordCount();	int startCount=0;	int bucketSize=500;
		query=this.getRowComparisonQuery(primaryKeyCol, allColName, schemaTableName, startCount, bucketSize);
		innerQuery=this.getInnerQuery(dbSchemaInfo);
			
		query+=innerQuery;
		System.out.println("HIVE QUERY "+query);
		String value="";
		int count=0;
			//while(startCount<totalRcordCount)
			//{
					resultset=queryExecutor.executeQuery(super.connection, query);
		
					try {
						while(resultset.next())
						{
							value=resultset.getString(2);
							if(!bloomFilter.mightContain(value))
							{
								
								//if(count<250)
									misplacedRecord.add(value);
							}
							else
							{
								count++;
							}
						}
					} catch (SQLException e) {
						// TODO Auto-generated catch block
						e.printStackTrace();
					}
					System.out.println("PASS COUNT "+count);
					System.out.println("Fail COUNT "+misplacedRecord.size());
			//}
		
		return misplacedRecord;
	}

	@Override
	public String getColascedColName(LinkedList<String> allColumnList) {
		
		String colasceName="";
		for(int i=1;i<=allColumnList.size();i++)
		{
			if(i==allColumnList.size())
				colasceName+="coalesce(t."+allColumnList.get(i-1)+",'-999') ";
			else
				colasceName+="coalesce(t."+allColumnList.get(i-1)+",'-999'),'#@@@#',";			
		}
				
		colasceName="concat("+colasceName+")";
		
		return colasceName;
	}
	
	public String getColumnn_ROWQUERY(LinkedList<String> columnName)
	{
		String columns="";
		
		for(int index=1;index<=columnName.size();index++)
		  {
			  if(index==columnName.size())
			  {
				  columns+="t."+columnName.get(index-1)+" ";
			  }
			  else
			  {
				  columns+="t."+columnName.get(index-1)+",";
				  columns+="'#@@@#',";
			  }
		  }//end for
		
		if(columnName.size()>1)
		  {
			  columns="concat("+columns+")";  
		  }
		return columns;
	}

	private String getColumns(LinkedList<String> columnName)
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
				  columns+=columnName.get(index-1)+", ";
			  }
		  }//end for
		  
		 		 
		  if(columnName.size()>1)
		  {
			  columns="concat("+columns+")";  
		  }
		return columns;
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
	
}//end HIVE2
