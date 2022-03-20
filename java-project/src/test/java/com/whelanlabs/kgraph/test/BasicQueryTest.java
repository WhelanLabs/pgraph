package com.whelanlabs.kgraph.test;

import static org.junit.Assert.fail;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.mapping.ArangoJack;

public class BasicQueryTest {

   @Test
   public void test() {

      // Connection
//      ArangoDB arangoDB = new ArangoDB.Builder()
//            .serializer(new ArangoJack())
//            .build();

      // connection
      ArangoDB arangoDB = new ArangoDB.Builder().user("root").password("openSesame").serializer(new ArangoJack()).build();

      // drop old database
      /// arangoDB.

      // Creating a database
      String dbName = "mydb";
      try {
         arangoDB.createDatabase(dbName);
         System.out.println("Database created: " + dbName);
      } catch (ArangoDBException e) {
         System.err.println("Failed to create database: " + dbName + "; " + e.getMessage());
      }

      // delete existing collection if exists

      // create a collection
      String collectionName = "firstCollection";
      arangoDB.db(dbName).collection(collectionName).drop(); // JSW - delete collection if previously exists
      try {
         CollectionEntity myArangoCollection = arangoDB.db(dbName).createCollection(collectionName);
         System.out.println("Collection created: " + myArangoCollection.getName());
      } catch (ArangoDBException e) {
         System.err.println("Failed to create collection: " + collectionName + "; " + e.getMessage());
      }

      // Execute AQL queries - create docs
      ArangoCollection collection = arangoDB.db(dbName).collection(collectionName);
      for (int i = 0; i < 10; i++) {
         BaseDocument value = new BaseDocument();
         value.setKey(String.valueOf(i));
         value.addAttribute("xname", "Homer");
         collection.insertDocument(value);
      }
      
      // Add some negative data
      BaseDocument value = new BaseDocument();
      value.setKey(String.valueOf(11));
      value.addAttribute("name", "Homer");
      collection.insertDocument(value);

      // Execute AQL queries - get docs
      List<BaseDocument> results = new ArrayList<BaseDocument>();
      try {
         String query = "FOR t IN firstCollection FILTER t.xname == @xname RETURN t";
         Map<String, Object> bindVars = Collections.singletonMap("xname", "Homer");
         ArangoCursor<BaseDocument> cursor = arangoDB.db(dbName).query(query, bindVars, null, BaseDocument.class);
         cursor.forEachRemaining(aDocument -> {
            System.out.println("Key: " + aDocument.getKey());
            results.add(aDocument);
         });
      } catch (ArangoDBException e) {
         System.err.println("Failed to execute query. " + e.getMessage());
      }

      assert(10 == results.size());
      // fail("Not yet implemented");
   }

}
