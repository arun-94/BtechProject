package com.db.nosql;

import com.datastax.driver.core.*;
import com.db.DB_NOSQL;
import com.db.reports.ReportPOJO;
import com.db.util.DatabaseSchemaPOJO;
import com.db.util.QueryExecutor;
import com.google.common.hash.BloomFilter;
import com.google.common.hash.Funnels;

import java.io.InputStream;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.*;

public class Cassandra extends DB_NOSQL {

    private Properties cassandraProperties;
    private Cluster cassandraCluster;
    private Session cassandraSession;

    @Override
    public void connect() {

        getConfig();
        Cluster.Builder builder = Cluster.builder();

        String[] addresses = getCasNodes();
        for (String s : addresses) {
            builder.addContactPoint(s);
        }

        try {
            cassandraCluster = builder.build();
            cassandraSession = cassandraCluster.connect(getCasKeyspace());
            System.out.println("Connected to the Cassandra Database");

        } catch (Exception e) {
            System.out.println("Failed to connect to the Cassandra server");
            e.printStackTrace();
        }
    }

    @Override
    public void close() {
        try {
            cassandraSession.close();
            cassandraCluster.close();
            System.out.println("Closed Cassandra connection");
        } catch (Exception e) {
            //ignore
        }
    }


    private void getConfig() {
        InputStream in = Cassandra.class.getClassLoader().getResourceAsStream("NoSQLDB.properties");
        try {
            Properties cassandraProperties = new Properties();
            cassandraProperties.load(in);
            this.cassandraProperties = cassandraProperties;
        } catch (Exception e) {
            throw new RuntimeException(e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (Exception e) {
                    //ignore
                }
            }
        }
    }


    //Getters

    private String[] getCasNodes() {

        String addressString = cassandraProperties.getProperty("cassandra.node.addresses");
        return addressString.split(",");
    }

    private String getCasUser() {
        return cassandraProperties.getProperty("cassandra.user");
    }


    private String getCasPassword() {
        return cassandraProperties.getProperty("cassandra.password");
    }


    private String getCasKeyspace() {
        return cassandraProperties.getProperty("cassandra.keyspace");
    }

    private Cluster getCassandraCluster() {
        return cassandraCluster;
    }

    public Session getCassandraSession() {
        return cassandraSession;
    }


    public LinkedList<String> verifyDataFromBloomFilter(BloomFilter<CharSequence> bloomFilter, DatabaseSchemaPOJO dbSchemaInfo, ReportPOJO reportInfo, String casTable) {

        String query = generateQueryForTable(casTable, dbSchemaInfo);

        Statement stmt = new SimpleStatement(query);
        com.datastax.driver.core.ResultSet rs = cassandraSession.execute(stmt);
        System.out.println(rs);

        LinkedList<String> misplacedRecord = new LinkedList<>();

        for (Row casRow : rs) {
            int count = casRow.getColumnDefinitions().asList().size();

            String value = "";
            for (int i = 0; i < count; i++) {

                DataType colType = casRow.getColumnDefinitions().asList().get(i).getType();

                Object o = casRow.getObject(i);
                if (colType.getName().equals(DataType.Name.TIMESTAMP)) {
                    o = ((Date) o).getTime();
                }

                if (i < count - 1) {
                    value += o.toString() + "#@@@#";
                } else value += o.toString();
            }
            if (!bloomFilter.mightContain(value)) {
                misplacedRecord.add(value);
            }
        }

        return misplacedRecord;
    }

    private String generateQueryForTable(String casTable, DatabaseSchemaPOJO dbSchemaInfo) {
        String query = "SELECT ";
        int i = 0;
        for (String colName : dbSchemaInfo.getAllColumnList()) {
            query += colName;
            i++;
            if (dbSchemaInfo.getAllColumnList().size() > i)
                query += ", ";
        }

        query += " FROM " + casTable;

        return query;
    }

}
