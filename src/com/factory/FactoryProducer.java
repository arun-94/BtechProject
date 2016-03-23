package com.factory;

public class FactoryProducer {

	public static AbstractFactory getDBFactory(String dbType)
	{
		AbstractFactory abstractFactoryObj=null;
		
			if(dbType.equalsIgnoreCase("JDBC"))
			{
				abstractFactoryObj=new JDBCFactory();
			}
			else if(dbType.equalsIgnoreCase("NOSQL"))
			{
				abstractFactoryObj=new NOSQLFactory();
			}
		
		
		return abstractFactoryObj;
		
	}
	
}
