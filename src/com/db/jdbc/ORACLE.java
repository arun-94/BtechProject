package com.db.jdbc;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
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

public class ORACLE extends DB_JDBC{

	private static Logger logger=Logger.getLogger(ORACLE.class.getName());

	@Override
	public String getSchemaName(Connection connection,String tableName) {
		
		logger.info("In Class: ORACLE Method: getSchemaName()");
		
		String schemaName="";
		try {
			String query="select owner,table_name from all_tables where table_name='"+tableName.toUpperCase()+"'";
			Statement statement=connection.createStatement();
		//	logger.info("ORACLE "+query);
			ResultSet resultSet=statement.executeQuery(query);
			
			while(resultSet.next())
				logger.info(schemaName=resultSet.getString(1).trim());
			
			
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return schemaName;
	}

	public LinkedList<String> getAllColumnName(String tableName,Connection connection)
	{
		logger.info("In Class: ORACLE Method: getAllColumnName() ");
			
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
		logger.info("In Class: ORACLE Method: getPrimaryKeyColumn()");
		
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
			e.printStackTrace();
		}
				
		return primaryKeyList;
	}//end getPrimaryKeyColumn()
	
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
	
	public BloomFilter<CharSequence> addDataToBloomFilter(DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) {
		// TODO Auto-generated method stub
		logger.info("In Class: ORACLE Method: addDataToBloomFilter()");
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
		logger.info("In Class: ORACLE Method: getRowComparisonQuery()");
		String query="select "+primaryKeyCol+" as PRIMARY_KEY,"+allColName+" as VALUE " +
				"from "+schemaTable+" where rownum <= "+offset+" and "+primaryKeyCol+" not in" +
				"( select "+primaryKeyCol+" from "+schemaTable+" where rownum <= "+startCount+" )";
		
		logger.info("ORACLE QUERY "+query);
		
		return query;
	}
	
	@Override
	public String getColascedColName(LinkedList<String> allColumnList) {
		// TODO Auto-generated method stub
		logger.info("In Class: ORACLE Method: getColascedColName()");
		String colasceName="";
		for(int i=1;i<=allColumnList.size();i++)
		{
			if(i==allColumnList.size())
				colasceName+="nvl("+allColumnList.get(i-1)+",-999) ";
			else
				colasceName+="nvl("+allColumnList.get(i-1)+",-999) || '#@@@#' ||";			
		}
				
		return colasceName;		
	}

	public LinkedList<String> verifyDataFromBloomFilter(
			BloomFilter<CharSequence> bloomFilter,
			DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo) {
		// TODO Auto-generated method stub
		logger.info("In Class: ORACLE Method: verifyDataFromBloomFilter()");
		
		return null;
	}

	@Override
	public void createTemporaryTable(String tableName,String query, String orderByColums) {
		// TODO Auto-generated method stub
		String queryTMP="CREATE GLOBAL TEMPORARY TABLE "+tableName+" ON COMMIT PRESERVE ROWS AS "+query+" order by "+orderByColums;
		Statement stmt=null;
	
		
		logger.info(queryTMP);
		
		try {
			stmt=connection.createStatement();
			stmt.executeUpdate(queryTMP);
			
			ResultSet rs=stmt.executeQuery("select * from "+tableName);
			
			if(rs.next())
				logger.info("Created Temp table Successfully ... ");
			else
				logger.info("Error: Creating Temp table  ... ");
				
			
	} catch (SQLException e) {
		// TODO Auto-generated catch block
		logger.error(e.getMessage(), e);
	}
		
		
		
		
	}

	@Override
	public LinkedList<String> getAllColumnName_QE(String tableName,Connection connection) {
		
		LinkedList<String> allColumnName=new LinkedList<String>();
		DatabaseMetaData metadata=null;
		ResultSet resultSet=null;
		logger.info(tableName);
		try {
			System.out.println(connection);
			metadata=connection.getMetaData();
			resultSet=metadata.getColumns(null, null, tableName, null);
				
			
			while(resultSet.next())
			{
			    allColumnName.add(resultSet.getString("COLUMN_NAME").toUpperCase());
				//System.out.println(resultSet.getString("COLUMN_NAME"));
			}//end while
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return allColumnName;
	}

}//end ORACLE
