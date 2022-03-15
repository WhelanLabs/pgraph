package com.whelanlabs.kgraph;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

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

   @Test
   public void upsertNode_newNode_added() {
      kGraph.createNodeCollection("dates");
      final ArangoCollection dates = kGraph._userDB.collection("dates");
      final BaseDocument badDate = new BaseDocument();
      String key = UUID.randomUUID().toString();
      badDate.setKey(key);
      badDate.addAttribute("foo", "bar");
      kGraph.upsertNode(dates, badDate);
      BaseDocument result = kGraph.getNodeByKey(key, "dates");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertNode_existingNode_added() {
      kGraph.createNodeCollection("dates");
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
      kGraph.createNodeCollection("testNodeCollection");
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
      assert (false);
   }
}
