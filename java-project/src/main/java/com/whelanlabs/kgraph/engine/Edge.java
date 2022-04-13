package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseEdgeDocument;

public class Edge extends BaseEdgeDocument implements Element {

   public static final String leftTypeAttrName = "__left_collection";
   public static final String rightTypeAttrName = "__right_collection";
   public static final String edgeTypeAttrName = "__edge_collection";

   public Edge(String edgeKey, Node left, Node right, String type) {
      super(edgeKey, left.getId(), right.getId());
      this.addAttribute(typeAttrName, type);
      String leftCollectionName = ElementFactory.getTypeName(left);
      String rightCollectionName = ElementFactory.getTypeName(right);
      this.addAttribute(leftTypeAttrName, leftCollectionName);
      this.addAttribute(rightTypeAttrName, rightCollectionName);
   }

   public Edge(final Map<String, Object> properties) {
      super(properties);
   }

   private static final long serialVersionUID = -4801740400388403434L;

}
