package com.whelanlabs.kgraph.engine;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

public class Triple {

   private BaseDocument _left;
   private BaseEdgeDocument _rel;
   private BaseDocument _right;

   public Triple (BaseDocument left, BaseEdgeDocument rel, BaseDocument right) {
      _left = left;
      _rel = rel;
      _right = right;
   }
}
