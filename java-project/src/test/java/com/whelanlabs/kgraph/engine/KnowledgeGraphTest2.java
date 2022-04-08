package com.whelanlabs.kgraph.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

public class KnowledgeGraphTest2 {
   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "KnowledgeGraphTests_db";

   private static Logger logger = LogManager.getLogger(KnowledgeGraphTest2.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      kGraph = new KnowledgeGraph(tablespace_name);
      kGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      kGraph.cleanup();
   }

   @Test(expected = RuntimeException.class)
   public void getEdgeCollection_isNodeCollection_getException() {
      logger.debug("getEdgeCollection_isNodeCollection_getException");
      String collectionName = KnowledgeGraph.generateName();
      Node testNode = new Node(KnowledgeGraph.generateKey(), collectionName);
      testNode.addAttribute("foo", "bbar");
      testNode = kGraph.upsert(testNode).getNodes().get(0);
      kGraph.getEdgeCollection(collectionName);
   }

   @Test(expected = RuntimeException.class)
   public void getNodeCollection_isEdgeCollection_getException() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeCollection");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeCollection");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      String testEdgeCollection = KnowledgeGraph.generateName();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, testEdgeCollection);
      kGraph.upsert(edge);

      kGraph.getNodeCollection(testEdgeCollection);
   }

   @Test(expected = RuntimeException.class)
   public void upsert_nodeKeyIsNull_exception() {
      final Node node = new Node(KnowledgeGraph.generateKey(), "testNodeCollection");
      node.setKey(null);
      kGraph._upsert(node);
   }

   @Test(expected = RuntimeException.class)
   public void upsert_edgeKeyIsNull_exception() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeCollection");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeCollection");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection");
      edge.setKey(null);
      kGraph._upsert(edge);
   }

   @Test(expected = RuntimeException.class)
   public void upsert_edgeIsNull_exception() {
      kGraph._upsert((Edge) null);
   }
}
