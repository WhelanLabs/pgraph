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
   }

   @Test(expected = ArangoDBException.class)
   public void upsertNode_edgeCollection_exception() {
      String edgeCollectionName = UUID.randomUUID().toString();
      kGraph.getEdgeCollection(edgeCollectionName);
      final ArangoCollection collection = kGraph._userDB.collection(edgeCollectionName);
      final BaseDocument baseDoc = new BaseDocument();
      String key = UUID.randomUUID().toString();
      baseDoc.setKey(key);
      baseDoc.addAttribute("foo", "bar");
      kGraph.upsertNode(collection, baseDoc);
   }

   @Test
   public void upsertNode_existingNode_added() {
      kGraph.getNodeCollection("dates");
      final ArangoCollection dates = kGraph._userDB.collection("dates");
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
      kGraph.getNodeCollection("testNodeCollection");
      final ArangoCollection dates = kGraph._userDB.collection("testNodeCollection");
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
      kGraph.getNodeCollection("testNodeCollection");
      final ArangoCollection dates = kGraph._userDB.collection("testNodeCollection");
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
}
