package com.whelanlabs.pgraph.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.model.TraversalOptions.Direction;
import com.whelanlabs.pgraph.engine.Edge;
import com.whelanlabs.pgraph.engine.ElementHelper;
import com.whelanlabs.pgraph.engine.PropertyGraph;
import com.whelanlabs.pgraph.engine.Node;

public class ElementFactoryTest {

   private static PropertyGraph pGraph = null;
   private static String tablespace_name = "ElementFactoryTest_db";

   private static Logger logger = LogManager.getLogger(ElementFactoryTest.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      logger.debug("Running ElementFactoryTest tests...");
      pGraph = new PropertyGraph(tablespace_name);
      pGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      pGraph.cleanup();
   }


   @Test(expected = RuntimeException.class)
   public void getLeftAttrString_badDirection_exception() {
      ElementHelper.getLeftAttrString(Direction.any);
   }

   @Test(expected = RuntimeException.class)
   public void getRightIdString_badDirection_exception() {
      String testCollectionName = "someCollection";
      final Node leftNode = new Node(ElementHelper.generateKey(), testCollectionName);
      final Node rightNode = new Node(ElementHelper.generateKey(), testCollectionName);
      
      pGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection");
      ElementHelper.getRightIdString(Direction.any, edge);
   }

}
