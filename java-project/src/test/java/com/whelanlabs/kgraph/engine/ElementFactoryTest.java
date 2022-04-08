package com.whelanlabs.kgraph.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.model.TraversalOptions.Direction;

public class ElementFactoryTest {

   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "ElementFactoryTest_db";

   private static Logger logger = LogManager.getLogger(ElementFactoryTest.class);

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
   public void getCollectionName_Element_idIsNull_exception() {
      logger.debug("getCollectionName_Element_idIsNull_exception");
      Node node = new Node(KnowledgeGraph.generateKey(), "someCollection");
      node.setId("something_without_a_slash");
      ElementFactory.getCollectionName(node);
   }

   @Test(expected = RuntimeException.class)
   public void getCollectionName_string_idIsNull_exception() {
      ElementFactory.getCollectionName("something_without_a_slash");
   }

   @Test(expected = RuntimeException.class)
   public void getLeftAttrString_badDirection_exception() {
      ElementFactory.getLeftAttrString(Direction.any);
   }

   @Test(expected = RuntimeException.class)
   public void getRightIdString_badDirection_exception() {
      String testCollectionName = "someCollection";
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), testCollectionName);
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), testCollectionName);
      
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection");
      ElementFactory.getRightIdString(Direction.any, edge);
   }

}
