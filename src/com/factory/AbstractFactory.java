package com.factory;

import com.db.DB_JDBC;
import com.db.DB_NOSQL;

public abstract class AbstractFactory {

	public abstract DB_JDBC getJDBC_DBObject(String dbType);
	public abstract DB_NOSQL getNOSQL_DBObject(String dbType);
}
