package com.whelanlabs.kgraph.engine;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.whelanlabs.kgraph.engine.KnowledgeGraph;
import com.whelanlabs.kgraph.loader.StockDataLoader;

public class KnowledgeGraphTest {
   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "KnowledgeGraphTests_db";

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      // KnowledgeGraph.removeTablespace(tablespace_name);
      kGraph = new KnowledgeGraph(tablespace_name);
      kGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      kGraph.cleanup();
   }

   @Test(expected = ArangoDBException.class)
   public void upsertNode_edgeCollection_exception() {
      String edgeCollectionName = "C" + UUID.randomUUID().toString();
      final ArangoCollection collection = kGraph.getEdgeCollection(edgeCollectionName);
      final BaseDocument baseDoc = new BaseDocument();
      String key = "K" + UUID.randomUUID().toString();
      baseDoc.setKey(key);
      baseDoc.addAttribute("foo", "bar");
      kGraph.upsertNode(collection, baseDoc);
   }
   
   @Test(expected = RuntimeException.class)
   public void getEdgeCollection_isNodeCollection_exception() {
      kGraph.getNodeCollection("node_collection");
      kGraph.getEdgeCollection("node_collection");
   }

   @Test(expected = RuntimeException.class)
   public void getNodeCollection_isEdgeCollection_exception() {
      kGraph.getEdgeCollection("edge_collection");
      kGraph.getNodeCollection("edge_collection");
   }
   
   @Test
   public void upsertNode_newNode_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("dates");
      final BaseDocument badDate = new BaseDocument();
      String key = UUID.randomUUID().toString();
      badDate.setKey(key);
      badDate.addAttribute("foo", "bbar");
      kGraph.upsertNode(dates, badDate);
     
      BaseDocument result = kGraph.getNodeByKey(key, "dates");
      String attr = (String) result.getAttribute("foo");
      assert ("bbar".equals(attr));
   }
   
   @Test
   public void upsertNode_existingNode_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("dates");
      final BaseDocument badDate = new BaseDocument();
      String key = UUID.randomUUID().toString();
      badDate.setKey(key);
      kGraph.upsertNode(dates, badDate);
      badDate.addAttribute("foo", "bar");
      kGraph.upsertNode(dates, badDate);
      BaseDocument result = kGraph.getNodeByKey(key, "dates");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_newEdge_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");
      final BaseDocument leftNode = new BaseDocument();
      final BaseDocument rightNode = new BaseDocument();
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      leftNode.setKey(key1);
      rightNode.setKey(key2);
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection");
      BaseEdgeDocument edge = new BaseEdgeDocument();
      edge.setFrom(leftNode.getId());
      edge.setTo(rightNode.getId());
      edge.addAttribute("foo", "bar");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edgeCollection, edge);

      BaseDocument result = kGraph.getNodeByKey(edgeKey, "testEdgeCollection");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_existingEdge_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");
      
      final BaseDocument leftNode = new BaseDocument();
      final BaseDocument rightNode = new BaseDocument();
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      leftNode.setKey(key1);
      rightNode.setKey(key2);
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection");
      BaseEdgeDocument edge = new BaseEdgeDocument();
      edge.setFrom(leftNode.getId());
      edge.setTo(rightNode.getId());
      edge.addAttribute("foo", "bar");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edgeCollection, edge);
      edge.addAttribute("foo-foo", "bar-bar");
      edge = kGraph.upsertEdge(edgeCollection, edge);

      BaseDocument result = kGraph.getNodeByKey(edgeKey, "testEdgeCollection");
      String fooAttr = (String) result.getAttribute("foo");
      assert ("bar".equals(fooAttr)) : "foo value is " + fooAttr;
      String fooFooAttr = (String) result.getAttribute("foo-foo");
      assert ("bar-bar".equals(fooFooAttr)) : "foo-foo value is " + fooFooAttr;
   }
   
   
   @Test(expected = NullPointerException.class)
   public void upsertEdge_collectionIsNull_exception() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");
      
      final BaseDocument leftNode = new BaseDocument();
      final BaseDocument rightNode = new BaseDocument();
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      leftNode.setKey(key1);
      rightNode.setKey(key2);
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = null;
      BaseEdgeDocument edge = new BaseEdgeDocument();
      edge.setFrom(leftNode.getId());
      edge.setTo(rightNode.getId());
      edge.addAttribute("foo", "bar");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edgeCollection, edge);
   }
   
   @Test(expected = NullPointerException.class)
   public void upsertEdge_edgeIsNull_exception() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");
      
      final BaseDocument leftNode = new BaseDocument();
      final BaseDocument rightNode = new BaseDocument();
      String key1 = UUID.randomUUID().toString();
      String key2 = UUID.randomUUID().toString();
      leftNode.setKey(key1);
      rightNode.setKey(key2);
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection");
      BaseEdgeDocument edge = null;
      kGraph.upsertEdge(edgeCollection, edge);
   }
   
   
   @Test
   public void load_freshAndValid_collectionsExist() throws Exception {
      Long beginSize = kGraph.getTotalCount();
      final ArangoCollection dates = kGraph.getNodeCollection("dates");
      final BaseDocument badDate = new BaseDocument();
      String key = UUID.randomUUID().toString();
      badDate.setKey(key);
      badDate.addAttribute("foo", "bbar");
      kGraph.upsertNode(dates, badDate);

      Long endSize = kGraph.getTotalCount();
      assert (endSize == beginSize + 1) : "{beginSize, endsize} is {" + beginSize + ", " + endSize + "}";
   }
}
