package com.upgrad;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;
import org.bson.Document;

import java.sql.*;
import java.util.Arrays;

import static com.mongodb.client.model.Projections.excludeId;
import static com.mongodb.client.model.Projections.fields;

import static com.mongodb.client.model.Filters.eq;

public class CRUDHelper {
    /**
     * Display ALl products
     *
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");

        MongoCursor<Document> cursor = collection.find().cursor();
        while (cursor.hasNext()) {
            PrintHelper.printSingleCommonAttributes(cursor.next());
        }
    }

    /**
     * Display top 5 Mobiles
     *
     * @param collection
     */
    public static void displayTop5Mobiles(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Top 5 Mobiles ------");
        Bson filter = eq("Category", "Mobiles");
        MongoCursor<Document> cursor = collection.find(filter).cursor();
        while (cursor.hasNext()) {
            PrintHelper.printAllAttributes(cursor.next());
        }
    }

    /**
     * Display products ordered by their categories in Descending order without auto generated Id
     *
     * @param collection
     */
    public static void displayCategoryOrderedProductsDescending(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Products ordered by categories ------");
        // Call printAllAttributes to display the attributes on the Screen
    }

    /**
     * Display number of products in each group
     * @param collection
     */
    public static void displayProductCountByCategory(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Product Count by categories ------");
        // Call printProductCountInCategory to display the attributes on the Screen
    }

    /**
     * Display Wired Headphones
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        // Call printAllAttributes to display the attributes on the Screen
    }

}
