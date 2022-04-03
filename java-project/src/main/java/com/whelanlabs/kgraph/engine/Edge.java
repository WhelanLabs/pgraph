package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseEdgeDocument;

public class Edge extends BaseEdgeDocument {

   public Edge(String edgeKey, String leftID, String rightID) {
      super(edgeKey, leftID, rightID);
   }

   public Edge(final Map<String, Object> properties) {
      super(properties);
   }

   private static final long serialVersionUID = -4801740400388403434L;

}
