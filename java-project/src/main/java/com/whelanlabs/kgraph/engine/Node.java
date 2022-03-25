package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseDocument;

public class Node extends BaseDocument {

   private static final long serialVersionUID = 4105187908291902366L;

   public Node(String key) {
      super(key);
   }

   public Node() {
      super();
   }

   public Node(final Map<String, Object> properties) {
      super(properties);
   }
}
