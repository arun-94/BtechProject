package com.factory;


import com.db.DB_JDBC;
import com.db.DB_NOSQL;
import com.db.nosql.Cassandra;
import com.db.nosql.HBase;
import com.db.nosql.MongoDB;

public class NOSQLFactory extends AbstractFactory{

	
	@Override
	public DB_JDBC getJDBC_DBObject(String dbType) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public DB_NOSQL getNOSQL_DBObject(String dbType) {
		DB_NOSQL nosqlDB=null;
		
		if(dbType.equalsIgnoreCase("Cassandra"))
			 nosqlDB=new Cassandra();
		else if(dbType.equalsIgnoreCase("MONGODB"))
			nosqlDB=new MongoDB();
		else if(dbType.equalsIgnoreCase("HBase"))
			nosqlDB=new HBase();
		
		return nosqlDB;
	}
	
}
