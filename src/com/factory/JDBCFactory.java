package com.factory;

import com.db.DB_JDBC;
import com.db.DB_NOSQL;
import com.db.jdbc.DB2;
import com.db.jdbc.HIVE2;
import com.db.jdbc.MYSQL;
import com.db.jdbc.ORACLE;


public class JDBCFactory extends AbstractFactory{

		
	public DB_NOSQL getNOSQL_DBObject(String dbType) {
		// TODO Auto-generated method stub
		return null;
	}


	@Override
	public DB_JDBC getJDBC_DBObject(String dbType) {
		
		DB_JDBC validator=null;
		
		if(dbType.equalsIgnoreCase("MYSQL"))
		{
			validator=new MYSQL();
		}
		else if(dbType.equalsIgnoreCase("ORACLE"))
		{
			validator=new ORACLE();
		}
		else if(dbType.equalsIgnoreCase("DB2"))
		{
			validator=new DB2();
		}
		else if(dbType.equalsIgnoreCase("HIVE2"))
		{
			validator=new HIVE2();
		}
		
		return validator;
	}
	
	
}
