package com.whelanlabs.kgraph.engine;

import org.junit.Test;

import com.arangodb.model.TraversalOptions.Direction;

public class ElementFactoryTest {

   @Test(expected = RuntimeException.class)
   public void getCollectionName_Element_idIsNull_exception() {
      Node node = new Node(KnowledgeGraph.generateKey());
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
      Edge edge = new Edge(KnowledgeGraph.generateKey(), null, null);
      ElementFactory.getRightIdString(Direction.any, edge);
   }

}
