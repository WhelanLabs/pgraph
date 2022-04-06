package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseDocument;

public class Node extends BaseDocument implements Element {

   public Node(String key, String type) {
      super(key);
      this.addAttribute(typeAttrName, type);
   }

   public Node(final Map<String, Object> properties) {
      super(properties);
   }

   private static final long serialVersionUID = -1109802074556650431L;
}
