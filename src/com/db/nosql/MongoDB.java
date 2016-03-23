package com.db.nosql;

import com.db.DB_NOSQL;
import com.mongodb.MongoClient;
import com.mongodb.client.MongoDatabase;

import java.io.InputStream;
import java.util.Properties;

public class MongoDB extends DB_NOSQL {
    private Properties mongodbProperties;
    private MongoClient mongoClient;
    private MongoDatabase mongoDatabase;

    public MongoDatabase getMongoDatabase() {
        return mongoDatabase;
    }

    private void getConfig() {
        InputStream in = MongoDB.class.getClassLoader().getResourceAsStream("NoSQLDB.properties");
        try {
            Properties mongodbProperties = new Properties();
            mongodbProperties.load(in);
            this.mongodbProperties = mongodbProperties;
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

    private String getServer() {
        return mongodbProperties.getProperty("mongodb.server");
    }

    private Integer getPort() {
        String port = mongodbProperties.getProperty("mongodb.port");
        return Integer.parseInt(port);
    }

    private String getUsername() {
        return mongodbProperties.getProperty("mongodb.username");
    }

    private String getPassword() {
        return mongodbProperties.getProperty("mongodb.password");
    }

    private String getDatabaseName() {
        return mongodbProperties.getProperty("mongodb.database");
    }

    @Override
    public void connect() {
        getConfig();
        mongoClient = new MongoClient(getServer(), getPort());
        mongoDatabase = mongoClient.getDatabase(getDatabaseName());
        return;
    }

    @Override
    public void close() {
        mongoClient.close();
        return;
    }
}
