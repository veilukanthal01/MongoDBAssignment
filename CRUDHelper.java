package com.upgrad;

import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.model.Filters;
import com.mongodb.client.model.Sorts;
import org.bson.Document;
import org.bson.conversions.Bson;
import com.mongodb.client.model.Accumulators;
import com.mongodb.client.model.Aggregates;


import java.sql.*;
import java.util.Arrays;

import static com.mongodb.client.model.Aggregates.group;
import static com.mongodb.client.model.Aggregates.sort;
import static com.mongodb.client.model.Filters.eq;
import static com.mongodb.client.model.Projections.*;
import static com.mongodb.client.model.Sorts.descending;

public class CRUDHelper {
    /**
     * Display ALl products
     *
     * @param collection
     */
    public static void displayAllProducts(MongoCollection<Document> collection) {
        System.out.println("------ Displaying All Products ------");
        // Call printSingleCommonAttributes to display the attributes on the Screen
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
        // Call printAllAttributes to display the attributes on the Screen
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
        Bson filter = Filters.empty();
        Bson projection = fields(excludeId());
        MongoCursor<Document> cursor = collection.find(filter).projection(projection).sort(descending("Category")).cursor();
        while (cursor.hasNext()) {
            PrintHelper.printAllAttributes(cursor.next());
        }

    }

    /**
     * Display number of products in each group
     *
     * @param collection
     */
    public static void displayProductCountByCategory(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Product Count by categories ------");
        // Call printProductCountInCategory to display the attributes on the Screen

        MongoCursor<Document> cursor = collection.aggregate(Arrays.asList(Aggregates.unwind("$Category"), Aggregates.group("$Category", Accumulators.sum("count", 1)))).cursor();
        while (cursor.hasNext()) {
            System.out.println(cursor.next().toJson());

        }

    }

    /**
     * Display Wired Headphones
     *
     * @param collection
     */
    public static void displayWiredHeadphones(MongoCollection<Document> collection) {
        System.out.println("------ Displaying Wired headphones ------");
        // Call printAllAttributes to display the attributes on the Screen
        Bson filter = eq("ConnectorType", "Wired");
        MongoCursor<Document> cursor = collection.find(filter).cursor();
        while (cursor.hasNext()) {
            PrintHelper.printAllAttributes(cursor.next());
        }
    }

}
