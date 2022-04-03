package com.whelanlabs.kgraph.engine;

import com.arangodb.entity.BaseEdgeDocument;


public class Edge extends BaseEdgeDocument {

   public Edge(String edgeKey, String leftID, String rightID) {
      super(edgeKey, leftID, rightID);
   }

   public Edge() {
      super();
   }

   private static final long serialVersionUID = -4801740400388403434L;

}
