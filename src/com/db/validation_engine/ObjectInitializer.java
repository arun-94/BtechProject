package com.db.validation_engine;

import java.util.LinkedHashMap;

import com.db.DB_JDBC;
import com.db.DB_NOSQL;
import com.db.configuration.DBPropertyPOJO;
import com.db.configuration.SourceDBPropertyReader;
import com.db.configuration.TargetDBPropertyReader;
import com.db.jdbc.HIVE2;
import com.db.testsuite.TestSuiteReader;
import com.factory.AbstractFactory;
import com.factory.FactoryProducer;
import com.factory.JDBCFactory;
import com.factory.NOSQLFactory;

public class ObjectInitializer 
{
	private static String srcDBType="",tgtDBType="";
	private static LinkedHashMap<String, String> dbMap=null;
	private static DBPropertyPOJO sourceConfigPOJO=null;
	private static DBPropertyPOJO targetConfigPOJO=null;
	private static String sourceTableName="",targetTableName="";
	private static String sourceQuery="",targetQuery="";
	private static String sourceOrderByCol="",targetOrderByCol="";
	private static TestSuiteReader testSuite=null;//new TestSuiteReader();
	private static ConfigObjectInitializerPOJO configObject=null;
	
	public ObjectInitializer(String srcDBType,String tgtDBType)
	{
		this.srcDBType=srcDBType;
		this.tgtDBType=tgtDBType;
		configObject=new ConfigObjectInitializerPOJO();	
		
		
		mapDB();
		readConfiguration();
		
		testSuite=new TestSuiteReader();
		if(testSuite.isTestTypeTableScan())
		{
			readTableNames();
			configObject.setTableScan(true);
		}
		else
		{
			readQueries();
			configObject.setTableScan(false);
		}
				
		createAbstractFactoryObject();		
	}
	
	private static void readConfiguration()
	{
		sourceConfigPOJO=new SourceDBPropertyReader().getDBPOJO();
		targetConfigPOJO=new TargetDBPropertyReader().getDBPOJO();
	}
	
	private static void mapDB()

	{
		dbMap=new LinkedHashMap<String, String>();
		
		dbMap.put("MYSQL","JDBC"); 
		dbMap.put("ORACLE","JDBC"); 
		dbMap.put("DB2","JDBC"); 
		dbMap.put("HIVE2", "JDBC");
		
		dbMap.put("CASSANDRA", "NOSQL");
		dbMap.put("MONGODB", "NOSQL");
		dbMap.put("HBASE", "NOSQL");
	}
	
	private static void readTableNames()
	{
		sourceTableName=testSuite.getSourceTableName();
		targetTableName=testSuite.getTargetTableName();
		
	}//end readTableName()
	
	private static void readQueries()
	{
		sourceQuery=testSuite.getSourceQuery();
		targetQuery=testSuite.getTargetQuery();
		sourceOrderByCol=testSuite.getSourceOrderByCol();
		targetOrderByCol=testSuite.getTargetOrderByCol();
	}//end readQueries()
	
	private static void createAbstractFactoryObject()
	{
		AbstractFactory srcFactory=null;
		AbstractFactory tgtFactory=null;		
		
		String sourceDBType=dbMap.get(srcDBType);
		srcFactory=FactoryProducer.getDBFactory(sourceDBType);
			
		 String targetDBType=dbMap.get(tgtDBType);
		 tgtFactory=FactoryProducer.getDBFactory(targetDBType);
			
			 if((srcFactory instanceof JDBCFactory) && (tgtFactory instanceof JDBCFactory))
			 {
				 System.out.println("SOURCE and Target Both are ---> JDBC Type");
				 createFactoryObject(srcFactory, tgtFactory,configObject);
				 
			 }else if((srcFactory instanceof JDBCFactory) && (tgtFactory instanceof NOSQLFactory))
			 {
				 System.out.println("SOURCE ==> JDBC TARGET==> NOSQL");
				
				 setDBPropertPOJO(sourceConfigPOJO, targetConfigPOJO);
				 setTablesNames(sourceTableName, targetTableName);
				 setQueries(sourceQuery,targetQuery,sourceOrderByCol,targetOrderByCol);
							 
				 createFactoryObject(srcFactory, tgtFactory,configObject);
				 
			 }else if((srcFactory instanceof NOSQLFactory) && (tgtFactory instanceof JDBCFactory))
			 {
				 System.out.println("SOURCE ==> NOSQL TARGET==> JDBC Swapping src & target ");
				 
				 setDBPropertPOJO(targetConfigPOJO,sourceConfigPOJO);
				 setTablesNames(targetTableName,sourceTableName);
				 setQueries(targetQuery,sourceQuery,targetOrderByCol,sourceOrderByCol);
				 				 
				 createFactoryObject(tgtFactory,srcFactory,configObject);
			 }
			 else
			 {
				 System.out.println("Invalid Combination .. Existing ..");
			 }
			 
	}//end createAbstractFactoryObject
	
	private static void createFactoryObject(AbstractFactory srcFactory,AbstractFactory tgtFactory,ConfigObjectInitializerPOJO configObject)
	{
		DB_JDBC sourceDBObj=null,targetDB_JDBCObj=null;
		DB_NOSQL targetDB_NOSQLObj=null;
		ValidationEngine validationEngine=null;
		
		sourceDBObj=srcFactory.getJDBC_DBObject(srcDBType);
		
		 if(tgtFactory instanceof NOSQLFactory)
		 {
			 targetDB_NOSQLObj=tgtFactory.getNOSQL_DBObject(tgtDBType);
			 validationEngine=new ValidationEngine(sourceDBObj, targetDB_NOSQLObj,configObject);
			 System.out.println("Target NOSQL");
		 }
		 else
		 {
			 targetDB_JDBCObj=tgtFactory.getJDBC_DBObject(tgtDBType);
			 
			 if(sourceDBObj instanceof HIVE2)
			 {
				 System.out.println("SRC ==> HIVE2 SWAP ..");
				 
				 setDBPropertPOJO(targetConfigPOJO,sourceConfigPOJO);
				 setTablesNames(targetTableName,sourceTableName);
				 setQueries(targetQuery,sourceQuery,targetOrderByCol,sourceOrderByCol);
				 				 
				 validationEngine=new ValidationEngine(targetDB_JDBCObj, sourceDBObj,configObject);
				 
			 }else if(targetDB_JDBCObj instanceof HIVE2)
			 {
				 
				 setDBPropertPOJO(sourceConfigPOJO, targetConfigPOJO);
				 setTablesNames(sourceTableName, targetTableName);
				 setQueries(sourceQuery,targetQuery,sourceOrderByCol,targetOrderByCol);
				 
				 validationEngine=new ValidationEngine(sourceDBObj,targetDB_JDBCObj,configObject);
				 System.out.println("TGT ==> HIVE2   ");
			 }
			 
		 }//end if-else
	}
		
	private static void setDBPropertPOJO(DBPropertyPOJO sourceConfigPOJO,DBPropertyPOJO targetConfigPOJO)
	{
		configObject.setSourceConfigPOJO(sourceConfigPOJO);
		configObject.setTargetConfigPOJO(targetConfigPOJO);
	}
	
	private static void setTablesNames(String sourceTableName,String targetTableName)
	{
		configObject.setSourceTableName(sourceTableName);
		configObject.setTargetTableName(targetTableName);
	}
	
	private static void setQueries(String sourceQuery,String targetQuery,String sourceOrderBy,String targetOrderBy)
	{
		configObject.setSourceQuery(sourceQuery);
		configObject.setTargetQuery(targetQuery);
		configObject.setSourceOrderByCol(sourceOrderBy);
		configObject.setTargetOrderByCol(targetOrderBy);
	}
	
	
}//end class
