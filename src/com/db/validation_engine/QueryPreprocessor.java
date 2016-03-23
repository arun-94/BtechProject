package com.db.validation_engine;

import java.util.LinkedList;
import java.util.StringTokenizer;

public class QueryPreprocessor {

	public LinkedList<String> getColumnsByList(String columns)
	{
		LinkedList<String> list=new LinkedList<>();
		StringTokenizer token=new StringTokenizer(columns,",");
		
		while(token.hasMoreTokens())
		{
			list.add(token.nextToken());
		}
		
		
		return list;
	}//end getColumnsByList
	
}
