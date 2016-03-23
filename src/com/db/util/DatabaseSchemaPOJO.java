package com.db.util;

import java.util.LinkedList;

public class DatabaseSchemaPOJO {

	private LinkedList<String> allColumnNameList;
	private LinkedList<String> primaryKeyColNameList;
	private String schemaName;
	private String tableName;
	
	public DatabaseSchemaPOJO(LinkedList<String> allColList,LinkedList<String> primaryKeyList,String schemaName,String tableName)
	{
		this.allColumnNameList=allColList;
		this.primaryKeyColNameList=primaryKeyList;
		this.schemaName=schemaName;
		this.tableName=tableName;
	}//end DatabaseSchemaPOJO
	
	public LinkedList<String> getAllColumnList()
	{
		return allColumnNameList;
	}
	
	public LinkedList<String> getPrimaryKeyColList()
	{
		return primaryKeyColNameList;
	}
	
	public String getSchemaName()
	{
		return schemaName+".";
	}
	
	public String getTableName()
	{
		return tableName;
	}
}
