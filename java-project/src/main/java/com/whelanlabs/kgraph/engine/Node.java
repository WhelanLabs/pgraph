package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseDocument;

public class Node extends BaseDocument {

   public Node(String generateKey) {
      super(generateKey);
   }
   
   public Node(final Map<String, Object> properties) {
      super(properties);
   }
   private static final long serialVersionUID = -1109802074556650431L;

}
