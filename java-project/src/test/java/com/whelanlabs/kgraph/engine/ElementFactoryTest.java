package com.whelanlabs.kgraph.engine;

import org.junit.Test;

public class ElementFactoryTest {

   @Test(expected = RuntimeException.class)
   public void getCollectionName_idIsNull_exception() {
      Node node = new Node(KnowledgeGraph.generateKey());
      node.setId("something_without_a_slash");
      ElementFactory.getCollectionName(node);
   }
}
