package com.upgrad;

import co.upgrad.entities.Mobiles;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoClients;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;
import org.bson.Document;

import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;

public class Driver {
    public static void main(String[] args) throws SQLException, ClassNotFoundException {
        /**
         * Creating a MongoClient , Database and Collection
         */
        MongoClient mongoClient = MongoClients.create("mongodb://ec2-54-173-40-93.compute-1.amazonaws.com");
        if (mongoClient != null)
            System.out.println("Connected to the Mongo DB" + mongoClient);

        MongoDatabase db = mongoClient.getDatabase("upgrad");
        //db.getCollection("products").drop();
        MongoCollection<Document> productsCollection = db.getCollection("products");
        System.out.println("Collection Created " + productsCollection);

        /**
         * Importing Data from MYSQL to Mongodb and displaying all the documents
         */
        Driver driver = new Driver();
        //driver.importDataToMongoDB(productsCollection);
        for (Document document : productsCollection.find()) {
            System.out.println(document.toJson());
        }

    }

    public void importDataToMongoDB(MongoCollection<Document> productsCollection) throws SQLException, ClassNotFoundException {
        /**
         * Importing mobiles, cameras and headphones data to MongoDB
         */
        LoadMobileTabletoProductCollection(productsCollection, "Mobiles");
        LoadMobileTabletoProductCollection(productsCollection, "Cameras");
        LoadMobileTabletoProductCollection(productsCollection, "Headphones");
    }


    public void LoadMobileTabletoProductCollection(MongoCollection<Document> productsCollection, String categoryName) throws SQLException, ClassNotFoundException {
        String url = "jdbc:mysql://pgc-sd-bigdata.cyaielc9bmnf.us-east-1.rds.amazonaws.com:3306/pgcdata";
        String user = "student";
        String password = "STUDENT123";
        List<Document> productsList = new ArrayList<Document>();
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = null;
        Statement statement = null;
        String sql = null;
        try {
            /**
             * Creating a connection to MYSQL
             */
            connection = DriverManager.getConnection(url, user, password);

            /**
             * Checking if the connection is not null
             */
            if (connection != null) {
                System.out.println("Connected to the MYSQL database");
            }

            /**
             * Creating sql query to be executed based on the Category name
             */
            if (categoryName.equalsIgnoreCase("Mobiles"))
                sql = "select * from mobiles";
            else if (categoryName.equalsIgnoreCase("Cameras"))
                sql = "select * from cameras";
            else
                sql = "select * from headphones";

            /**
             * Retrieving result sets from mysql
             */
            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);

            /**
             * Getting  Column count using metadata interface
             */
            ResultSetMetaData metadata = resultSet.getMetaData();
            int columnCount = metadata.getColumnCount();

            /**
             * Traversing the result set and creating documents to be imported to MongoDB
             */
            while (resultSet.next()) {
                Document document = new Document("ProductId", resultSet.getString("ProductId"))
                        .append("CategoryName", categoryName);
                for (int i = 2; i <= columnCount; i++) {
                    String columnName = metadata.getColumnName(i);
                    document.append(columnName, resultSet.getString(columnName));
                }
                productsList.add(document);
            }
            /**
             * Inserting created documents into mongodb
             */
            productsCollection.insertMany(productsList);


        } finally {
            statement.close();
            connection.close();
        }

    }

}
