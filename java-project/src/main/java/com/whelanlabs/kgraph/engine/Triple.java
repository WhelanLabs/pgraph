package com.whelanlabs.kgraph.engine;

import com.arangodb.entity.BaseDocument;

public class Triple {

   private BaseDocument _left;
   private BaseDocument _rel;
   private BaseDocument _right;

   public Triple (BaseDocument left, BaseDocument rel, BaseDocument right) {
      _left = left;
      _rel = rel;
      _right = right;
   }
}
