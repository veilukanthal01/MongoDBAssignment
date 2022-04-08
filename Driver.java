package com.upgrad;

import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class Driver {
    public static void main(String[] args) throws SQLException {
        MongoClient mongoClient = null;
        Connection mysqlConnection = null;
        try {

            String url = "jdbc:mysql://pgc-sd-bigdata.cyaielc9bmnf.us-east-1.rds.amazonaws.com:3306/pgcdata";
            String user = "student";
            String password = "STUDENT123";
            Class.forName("com.mysql.cj.jdbc.Driver");
            /**
             * Creating a connection to MYSQL
             */
            mysqlConnection = DriverManager.getConnection(url, user, password);

            /**
             * Checking if the connection is not null
             */
            if (mysqlConnection != null) {
                System.out.println("Connected to the MYSQL database");
            }

            /**
             * Creating a MongoClient , Database and Collection
             */
            mongoClient = MongoClients.create("mongodb://ec2-54-87-52-111.compute-1.amazonaws.com");
            if (mongoClient != null)
                System.out.println("Connected to the Mongo DB" + mongoClient);

            MongoDatabase db = mongoClient.getDatabase("upgrad");
            //db.getCollection("products").drop();
            MongoCollection<Document> productsCollection = db.getCollection("products");
            System.out.println("Collection Created " + productsCollection);

            /**
             * Importing Data from MYSQL to Mongodb and displaying all the imported documents
             */
            Driver driver = new Driver();
            //driver.importDataToMongoDB(mysqlConnection, productsCollection);

            // List all products in the inventory
            CRUDHelper.displayAllProducts(productsCollection);

            // Display top 5 Mobiles
            CRUDHelper.displayTop5Mobiles(productsCollection);


        } catch (Exception ex) {
            System.out.println("Got Exception.");
            ex.printStackTrace();
        } finally {
            mysqlConnection.close();
            mongoClient.close();
        }

    }

    public void importDataToMongoDB(Connection mysqlConnection, MongoCollection<Document> productsCollection) throws SQLException, ClassNotFoundException {
        Statement statement = null;
        String sql = null;
        List<Document> productsList = new ArrayList<Document>();
        String[] categoryNames = {"Mobiles", "Cameras", "Headphones"};
        ResultSet resultSet = null;
        ResultSetMetaData metadata = null;
        int columnCount = 0;

        for (int i = 0; i < categoryNames.length; i++) {
            /**
             * Creating sql query to be executed based on the Category name
             */
            if (categoryNames[i].equalsIgnoreCase("Mobiles"))
                sql = "select * from mobiles";
            else if (categoryNames[i].equalsIgnoreCase("Cameras"))
                sql = "select * from cameras";
            else
                sql = "select * from headphones";
            /**
             * Retrieving result sets from mysql
             */
            statement = mysqlConnection.createStatement();
            resultSet = statement.executeQuery(sql);

            /**
             * Getting  Column count using metadata interface
             */
            metadata = resultSet.getMetaData();
            columnCount = metadata.getColumnCount();

            /**
             * Traversing the result set and creating documents to be imported to MongoDB
             */
            while (resultSet.next()) {
                Document document = new Document("ProductId", resultSet.getString("ProductId"))
                        .append("Category", categoryNames[i]);
                for (int j = 2; j <= columnCount; j++) {
                    String columnName = metadata.getColumnName(j);
                    document.append(columnName, resultSet.getString(columnName));
                }
                productsList.add(document);
            }
            /**
             * Inserting created documents into mongodb
             */
            productsCollection.insertMany(productsList);

            statement.close();
            resultSet = null;
            metadata = null;
            columnCount = 0;
            productsList.clear();
        }
    }
}
