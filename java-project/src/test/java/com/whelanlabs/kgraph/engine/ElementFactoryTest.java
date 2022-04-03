package com.whelanlabs.kgraph.engine;

import static org.junit.Assert.*;

import org.junit.Test;

import com.whelanlabs.kgraph.engine.ElementFactory;
import com.whelanlabs.kgraph.engine.KnowledgeGraph;
import com.whelanlabs.kgraph.engine.Node;

public class ElementFactoryTest {

   @Test(expected = RuntimeException.class)
   public void getCollectionName_idIsNull_exception() {
      Node node = new Node(KnowledgeGraph.generateKey());
      node.setId("something_without_a_slash");
      ElementFactory.getCollectionName(node);
   }
}
