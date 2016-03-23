package com.db.validation_engine;

import com.db.configuration.DBPropertyPOJO;

public class ConfigObjectInitializerPOJO {
	
	private DBPropertyPOJO sourceConfigPOJO=null;
	private DBPropertyPOJO targetConfigPOJO=null;
	private String sourceTableName=null;
	private String targetTableName=null;
	private String sourceQuery=null;
	private String targetQuery=null;
	private String sourceOrderBy=null;
	private String targetOrderBy=null;
	private boolean testTypeTableScan=false;
	
	public void setSourceConfigPOJO(DBPropertyPOJO sourceConfigPOJO)
	{
		this.sourceConfigPOJO=sourceConfigPOJO;
	}//end setSourceConfigPOJO
		
	public DBPropertyPOJO getSourceConfigPOJO()
	{
		return sourceConfigPOJO;
	}
	
	public void setTargetConfigPOJO(DBPropertyPOJO targetConfigPOJO)
	{
		this.targetConfigPOJO=targetConfigPOJO;
	}
	
	public DBPropertyPOJO getTargetConfigPOJO()
	{
		return targetConfigPOJO;
	}
	
	public void setSourceTableName(String sourceTableName)
	{
		this.sourceTableName=sourceTableName;
	}
	
	public String getSourceTableName()
	{
		return sourceTableName;
	}
	
	public void setTargetTableName(String targetTableName)
	{
		this.targetTableName=targetTableName;
	}
	
	public String getTargetTableName()
	{
		return targetTableName;
	}
	
	public void setSourceQuery(String sourceQuery)
	{
		this.sourceQuery=sourceQuery;
	}
	
	public String getSourceQuery()
	{
		return sourceQuery;
	}
	
	public void setTargetQuery(String targetQuery)
	{
		this.targetQuery=targetQuery;
	}
	
	public String getTargetQuery()
	{
		return targetQuery;
	}

	public void setTableScan(boolean testTypeTableScan)
	{
		this.testTypeTableScan=testTypeTableScan;
	}//end setTableScan();
	
	public boolean getTableScan()
	{
		return testTypeTableScan;
	}

	public void setSourceOrderByCol(String sourceOrderBy)
	{
		this.sourceOrderBy=sourceOrderBy;
	}
	
	public String getSourceOrderByCol()
	{
		return sourceOrderBy;
	}

	public void setTargetOrderByCol(String targetOrderBy)
	{
		this.targetOrderBy=targetOrderBy;
	}
	
	public String getTargetOrderByCol()
	{
		return targetOrderBy;
	}
}//end ConfigObjectInitializerPOJO
