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
        MongoClient mongoClient = MongoClients.create("mongodb://ec2-54-173-40-93.compute-1.amazonaws.com");
        if (mongoClient != null)
            System.out.println("Connected to the Mongo DB" + mongoClient);

        MongoDatabase db = mongoClient.getDatabase("upgrad");
        //db.getCollection("products").drop();
        MongoCollection<Document> productsCollection = db.getCollection("products");
        System.out.println("Collection Created " + productsCollection);
        Driver driver = new Driver();
        //driver.importDataToMongoDB(productsCollection);
        for (Document document : productsCollection.find()) {
            System.out.println(document.toJson());
        }

    }

    public void importDataToMongoDB(MongoCollection<Document> productsCollection) throws SQLException, ClassNotFoundException {
        LoadMobileTabletoProductCollection(productsCollection, "Mobiles");
        LoadMobileTabletoProductCollection(productsCollection, "Cameras");
        LoadMobileTabletoProductCollection(productsCollection, "Headphones");
    }


    public void LoadMobileTabletoProductCollection(MongoCollection<Document> productsCollection, String categoryName) throws SQLException, ClassNotFoundException {
        String url = "jdbc:mysql://pgc-sd-bigdata.cyaielc9bmnf.us-east-1.rds.amazonaws.com:3306/pgcdata";
        String user = "student";
        String password = "STUDENT123";
        List<Document> mobilesList = new ArrayList<Document>();
        Class.forName("com.mysql.cj.jdbc.Driver");
        Connection connection = null;
        Statement statement = null;
        String sql = null;
        try {
            /**
             * Creating a connection with MYSQL
             */
            connection = DriverManager.getConnection(url, user, password);

            /**
             * Checking if the connection is not null
             */
            if (connection != null) {
                System.out.println("Connected to the MYSQL database");
            }

            if (categoryName.equalsIgnoreCase("Mobiles"))
                sql = "select * from mobiles";
            else if (categoryName.equalsIgnoreCase("Cameras"))
                sql = "select * from cameras";
            else
                sql = "select * from headphones";

            statement = connection.createStatement();
            ResultSet resultSet = statement.executeQuery(sql);
            ResultSetMetaData metadata = resultSet.getMetaData();
            int columnCount = metadata.getColumnCount();

            while (resultSet.next()) {
                Document document = new Document("ProductId", resultSet.getString("ProductId"))
                        .append("CategoryName", categoryName);
                for (int i = 2; i <= columnCount; i++) {
                    String columnName = metadata.getColumnName(i);
                    document.append(columnName, resultSet.getString(columnName));
                }
                mobilesList.add(document);
            }
            productsCollection.insertMany(mobilesList);


        } finally {
            statement.close();
            connection.close();
        }

    }

}
