package com.db;

import java.sql.Connection;
import java.util.LinkedList;

import com.db.configuration.DBPropertyPOJO;
import com.db.reports.ReportPOJO;
import com.db.util.DatabaseSchemaPOJO;
import com.google.common.hash.BloomFilter;

public interface ETLValidator {

	public Connection connectToDB(DBPropertyPOJO dbProperty);
	public BloomFilter<CharSequence> addDataToBloomFilter(DatabaseSchemaPOJO dbSchemaInfo,ReportPOJO reportInfo);
	public void setTotalRecordCount(DatabaseSchemaPOJO dbSchemaInfo,ReportPOJO reportInfo);
	public LinkedList<String> verifyDataFromBloomFilter(BloomFilter<CharSequence> bloomFilter,DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo);
	public void closeConnection();
}//end ETLValidator
