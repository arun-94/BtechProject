package com.db.util;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

import org.apache.log4j.Logger;


public class QueryExecutor {

	private static Logger logger=Logger.getLogger(QueryExecutor.class.getName());
	
	public ResultSet executeQuery(Connection con,String query)
	{
		ResultSet resultSet = null;
		try {
				Statement stmt=con.createStatement();
				resultSet=stmt.executeQuery(query);
				
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			logger.error("Error while Executing Query "+query, e);
			//e.printStackTrace();
		}
		
		
		return resultSet;
	}
	
}//end QueryExecutor
