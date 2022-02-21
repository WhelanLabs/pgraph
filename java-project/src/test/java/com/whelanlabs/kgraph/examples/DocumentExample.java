package com.whelanlabs.kgraph.examples;

import java.util.Collections;
import java.util.Map;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.mapping.ArangoJack;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class DocumentExample {

   public static void run() {

      // Connection
      ArangoDB arangoDB = new ArangoDB.Builder().user("root").password("openSesame").serializer(new ArangoJack()).build();

      // Creating a database
      String dbName = "mydb";
      try {
         arangoDB.createDatabase(dbName);
         System.out.println("Database created: " + dbName);
      } catch (ArangoDBException e) {
         System.err.println("Failed to create database: " + dbName + "; " + e.getMessage());
      }

      // Creating a collection
      String collectionName = "firstCollection";
      try {
         CollectionEntity myArangoCollection = arangoDB.db(dbName).createCollection(collectionName);
         System.out.println("Collection created: " + myArangoCollection.getName());
      } catch (ArangoDBException e) {
         System.err.println("Failed to create collection: " + collectionName + "; " + e.getMessage());
      }

      // Creating a document
      BaseDocument myObject = new BaseDocument();
      myObject.setKey("myKey");
      myObject.addAttribute("a", "Foo");
      myObject.addAttribute("b", 42);
      try {
         arangoDB.db(dbName).collection(collectionName).insertDocument(myObject);
         System.out.println("Document created");
      } catch (ArangoDBException e) {
         System.err.println("Failed to create document. " + e.getMessage());
      }

      // Read a document
      BaseDocument myBaseDocument = arangoDB.db(dbName).collection(collectionName).getDocument("myKey", BaseDocument.class);
      if (myBaseDocument != null) {
         System.out.println("Key: " + myBaseDocument.getKey());
         System.out.println("Attribute a: " + myBaseDocument.getAttribute("a"));
         System.out.println("Attribute b: " + myBaseDocument.getAttribute("b"));
      } else {
         System.err.println("Failed to get document: myKey");
      }

      // Read a document as Jackson JsonNode
      ObjectNode myObjectNodeDocument = arangoDB.db(dbName).collection(collectionName).getDocument("myKey", ObjectNode.class);
      if (myObjectNodeDocument != null) {
         System.out.println("Key: " + myObjectNodeDocument.get("_key").textValue());
         System.out.println("Attribute a: " + myObjectNodeDocument.get("a").textValue());
         System.out.println("Attribute b: " + myObjectNodeDocument.get("b").intValue());
      } else {
         System.err.println("Failed to get document: myKey");
      }

      // Update a document
      myObject.addAttribute("c", "Bar");
      try {
         arangoDB.db(dbName).collection(collectionName).updateDocument("myKey", myObject);
      } catch (ArangoDBException e) {
         System.err.println("Failed to update document. " + e.getMessage());
      }

      // Read the document again
      try {
         BaseDocument myUpdatedDocument = arangoDB.db(dbName).collection(collectionName).getDocument("myKey", BaseDocument.class);
         System.out.println("Key: " + myUpdatedDocument.getKey());
         System.out.println("Attribute a: " + myUpdatedDocument.getAttribute("a"));
         System.out.println("Attribute b: " + myUpdatedDocument.getAttribute("b"));
         System.out.println("Attribute c: " + myUpdatedDocument.getAttribute("c"));
      } catch (ArangoDBException e) {
         System.err.println("Failed to get document: myKey; " + e.getMessage());
      }

      // Delete a document
      try {
         arangoDB.db(dbName).collection(collectionName).deleteDocument("myKey");
      } catch (ArangoDBException e) {
         System.err.println("Failed to delete document. " + e.getMessage());
      }

      // Execute AQL queries
      // First we need to create some documents with the name Homer in collection
      // firstCollection
      ArangoCollection collection = arangoDB.db(dbName).collection(collectionName);
      for (int i = 0; i < 10; i++) {
         BaseDocument value = new BaseDocument();
         value.setKey(String.valueOf(i));
         value.addAttribute("name", "Homer");
         collection.insertDocument(value);
      }
      // Get all documents with the name Homer from collection firstCollection and
      // iterate over the result:
      try {
         String query = "FOR t IN firstCollection FILTER t.name == @name RETURN t";
         Map<String, Object> bindVars = Collections.singletonMap("name", "Homer");
         ArangoCursor<BaseDocument> cursor = arangoDB.db(dbName).query(query, bindVars, null, BaseDocument.class);
         cursor.forEachRemaining(aDocument -> {
            System.out.println("Key: " + aDocument.getKey());
         });
      } catch (ArangoDBException e) {
         System.err.println("Failed to execute query. " + e.getMessage());
      }

      // Delete a document with AQL
      try {
         String query = "FOR t IN firstCollection FILTER t.name == @name " + "REMOVE t IN firstCollection LET removed = OLD RETURN removed";
         Map<String, Object> bindVars = Collections.singletonMap("name", "Homer");
         ArangoCursor<BaseDocument> cursor = arangoDB.db(dbName).query(query, bindVars, null, BaseDocument.class);
         cursor.forEachRemaining(aDocument -> {
            System.out.println("Removed document " + aDocument.getKey());
         });
      } catch (ArangoDBException e) {
         System.err.println("Failed to execute query. " + e.getMessage());
      }
   }
}
