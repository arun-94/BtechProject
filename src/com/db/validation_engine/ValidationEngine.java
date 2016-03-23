package com.db.validation_engine;

import java.sql.Connection;
import java.util.LinkedList;

import org.apache.log4j.Logger;
import org.apache.log4j.pattern.LogEvent;

import com.db.DB_JDBC;
import com.db.DB_NOSQL;
import com.db.configuration.SourceDBPropertyReader;
import com.db.configuration.TargetDBPropertyReader;
import com.db.jdbc.DB2;
import com.db.reports.ReportPOJO;
import com.db.testsuite.TestSuiteReader;
import com.db.util.DatabaseSchemaPOJO;
import com.google.common.hash.BloomFilter;

public class ValidationEngine 
{
	private static Logger logger=Logger.getLogger(ValidationEngine.class.getName());
	
	private LinkedList<String> srcColumnList,srcPrimaryKeyColumnList;
	private LinkedList<String> tgtColumnList,tgtPrimaryKeyColumnList;
	private String sourceSchemaName,targetschemaName;
	private String srcTableName,tgtTableName;
	private String srcSchemaName,tgtSchemaName;
	private BloomFilter<CharSequence> bloomFilterSrc;
	private LinkedList<String> misplacedRecord;
	
	public ValidationEngine(DB_JDBC srcDBObj,DB_JDBC tgtDBObj,ConfigObjectInitializerPOJO configObject)
	{
		logger.info("Class: ValidationEngine constructor: ValidationEngine()");
				
		if(!configObject.getTableScan())
		{
			logger.info(" ----QueryEditor---");
			queryEditorValidation(srcDBObj, tgtDBObj,configObject);
		}
		else
		{
			tablescanValidation(srcDBObj,tgtDBObj,configObject);
		}
		
	}//end ETLValidator
	
	
	public ValidationEngine(DB_JDBC srcDBObj,DB_NOSQL tgtDBObj,ConfigObjectInitializerPOJO configObject)
	{
		
	}
	
	
	public void tablescanValidation(DB_JDBC srcDBObj,DB_JDBC tgtDBObj,ConfigObjectInitializerPOJO configObject)
	{
		logger.info("Class: ValidationEngine method: tablescanValidation()");
		
		long start = System.currentTimeMillis();
		System.out.println("Start Time "+start);
		
		Connection srcConnection=srcDBObj.connectToDB(configObject.getSourceConfigPOJO());
		logger.info("Connected to SourceDB "+srcConnection);
		Connection tgtConnection=tgtDBObj.connectToDB(configObject.getTargetConfigPOJO());
		logger.info("Connected to TargetDB "+tgtConnection);
		
		this.srcTableName=configObject.getSourceTableName();
		this.tgtTableName=configObject.getTargetTableName();
		logger.info("SOURCE TABLE NAME "+srcTableName);
		logger.info("TARGET TABLE NAME "+tgtTableName);
		
		
		ReportPOJO reportInfoSrc=new ReportPOJO();
		ReportPOJO reportInfoTgt=new ReportPOJO();
		
		srcColumnList=srcDBObj.getAllColumnName(srcTableName, srcConnection);
		tgtColumnList=tgtDBObj.getAllColumnName(tgtTableName, tgtConnection);
		logger.info("Source Column List "+srcColumnList);
		logger.info("Target Column List "+tgtColumnList);
		
		
		
		srcPrimaryKeyColumnList=srcDBObj.getPrimaryKeyColumn(srcTableName, srcConnection);
		
		/*----------Mapping Primary key Column of source*/
		tgtPrimaryKeyColumnList=srcPrimaryKeyColumnList;
		logger.info("SOURCE Primary Key "+srcPrimaryKeyColumnList);
		logger.info("TARGET Primary Key "+tgtPrimaryKeyColumnList);
		
		
		srcSchemaName=srcDBObj.getSchemaName(srcConnection,srcTableName);
		tgtSchemaName=tgtDBObj.getSchemaName(tgtConnection,tgtTableName);
		logger.info("SOURCE SCHEMA NAME "+srcSchemaName);
		logger.info("TARGET SCHEMA NAME "+tgtSchemaName);
		
				
		DatabaseSchemaPOJO srcSchemaInfo=new DatabaseSchemaPOJO(srcColumnList, srcPrimaryKeyColumnList, srcSchemaName,srcTableName);
		DatabaseSchemaPOJO tgtSchemaInfo=new DatabaseSchemaPOJO(tgtColumnList, tgtPrimaryKeyColumnList, tgtSchemaName,tgtTableName);
		
		
		//srcDBObj.getRowComparisonQuery(srcSchemaInfo,0,500);
		srcDBObj.setTotalRecordCount(srcSchemaInfo, reportInfoSrc);
		logger.info("TotalNumber of Records "+reportInfoSrc.getTotalRecordCount());
		tgtDBObj.setTotalRecordCount(tgtSchemaInfo, reportInfoTgt);
		logger.info("TotalNumber of Records "+reportInfoTgt.getTotalRecordCount());
		
		
		bloomFilterSrc=srcDBObj.addDataToBloomFilter(srcSchemaInfo, reportInfoSrc);
		misplacedRecord=tgtDBObj.verifyDataFromBloomFilter(bloomFilterSrc,tgtSchemaInfo,reportInfoTgt);
		
		System.out.println(misplacedRecord);
		
		System.out.println("Process Finished");
        long end = System.currentTimeMillis();
        long sec = end - start;
        System.out.println((float) (sec / 1000.0));
		
		
	}
	
	public void queryEditorValidation(DB_JDBC srcDBObj,DB_JDBC tgtDBObj,ConfigObjectInitializerPOJO configObject)
	{
		logger.info("Class: ValidationEngine method: queryEditorValidation()");
		QueryPreprocessor processQuery=new QueryPreprocessor();
		String sourceQuery=configObject.getSourceQuery();
		String targetQuery=configObject.getTargetQuery();
		String sourceOrderByColumns=configObject.getSourceOrderByCol();
		String targetOrderByColumns=configObject.getTargetOrderByCol();
		
		logger.info("SOURCE QUERY "+sourceQuery);
		logger.info("Target Query "+targetQuery);
		logger.info("sourceOrderByColumns "+sourceOrderByColumns);
		logger.info("targetOrderByColumns "+targetOrderByColumns);
		
		srcPrimaryKeyColumnList=processQuery.getColumnsByList(sourceOrderByColumns);
		tgtPrimaryKeyColumnList=processQuery.getColumnsByList(targetOrderByColumns);
		
		Connection srcConnection=srcDBObj.connectToDB(configObject.getSourceConfigPOJO());
		logger.info("Connected to SourceDB "+srcConnection);
		Connection tgtConnection=tgtDBObj.connectToDB(configObject.getTargetConfigPOJO());
		logger.info("Connected to TargetDB "+tgtConnection);
		
		this.srcTableName="SOURCE_TMP";
		this.tgtTableName="TARGET_TMP";
		
		srcDBObj.createTemporaryTable(srcTableName,sourceQuery, sourceOrderByColumns);
		tgtDBObj.createTemporaryTable(tgtTableName,targetQuery, targetOrderByColumns);
		
		
		
		
	}
	
	
}//end ETLValidator
