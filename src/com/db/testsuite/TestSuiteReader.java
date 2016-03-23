package com.db.testsuite;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.StringTokenizer;

public class TestSuiteReader {
	
	private static BufferedReader reader=null;
	private static String srcTableName="",tgtTableName="";
	private static boolean tableScan=false;
	private static String sourceQuery="",targetQuery="";
	private static String sourceOrderByColName="",targetOrderByColName="";
	
	public TestSuiteReader()
	{
		try {
			
			  reader=new BufferedReader(new FileReader(new File("./testsuite/constraint_testing.txt")));
			  
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}	//end try-catch()
		
		identifyTestingType();
	}//end TestSuiteReader()
	
	public static void identifyTestingType()
	{
		String constraintType[]=null;
		String line=null;
		StringTokenizer token=null;
		//logger.info("-- In identifyTestingType() ---");
		try {
			while((line=reader.readLine())!=null)
			{
				//System.out.println(line);
				System.out.println("Line "+line);
				constraintType=line.split("---");
			
				token=new StringTokenizer(constraintType[1],"#%#%#");
				
				if(constraintType[0].equalsIgnoreCase("ROW_COMPARISON"))
				{
					tableScan=true;
					srcTableName=token.nextToken();
					tgtTableName=token.nextToken();
				//	System.out.println(srcTableName+" --- "+tgtTableName);
					
				}else
				{
					tableScan=false;
					System.out.println("QUERY READER Currently Not implemented ....");
					sourceQuery=token.nextToken();
					targetQuery=token.nextToken();
					sourceOrderByColName=token.nextToken();
					targetOrderByColName=token.nextToken();
				}
				
				
			}//end while()
			
		
		} catch (IOException e) {
			// TODO Auto-generated catch block
			System.out.println("Exception while reading file ..");
			e.printStackTrace();
		}
		
	}//end 
	
	public String getSourceTableName()
	{
		return srcTableName;
	}
	
	public String getTargetTableName()
	{
		return tgtTableName;
	}
	
	public boolean isTestTypeTableScan()
	{
		return tableScan;
	}
	
	public String getSourceQuery()
	{
		return sourceQuery;
	}
	
	public String getTargetQuery()
	{
		return targetQuery;
	}
	
	public String getSourceOrderByCol()
	{
		return sourceOrderByColName;
	}
	
	public String getTargetOrderByCol()
	{
		return targetOrderByColName;
	}
	
}//end TestSuiteReader
