import java.util.LinkedHashMap;

import com.db.DB_JDBC;
import com.db.DB_NOSQL;
import com.db.jdbc.HIVE2;
import com.db.validation_engine.ObjectInitializer;
import com.factory.AbstractFactory;
import com.factory.FactoryProducer;
import com.factory.JDBCFactory;
import com.factory.NOSQLFactory;


public class Driver {

	/**
	 * @param args
	 */
	static String sourceDB="MYSQL",targetDB="Cassandra";
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
			
		SourceTarget_Reader reader=new SourceTarget_Reader();
		new ObjectInitializer(reader.getSourceDB(), reader.getTargetDB());
		
		
	}//end main()

}
