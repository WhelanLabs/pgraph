package com.whelanlabs.kgraph.engine;

import com.arangodb.entity.BaseEdgeDocument;

public class Triple {

   private Node _left;
   private BaseEdgeDocument _rel;
   private Node _right;

   public Triple (Node left, BaseEdgeDocument rel, Node right) {
      _left = left;
      _rel = rel;
      _right = right;
   }
}
