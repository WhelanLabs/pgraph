package com.whelanlabs.kgraph.engine;

import com.arangodb.model.TraversalOptions.Direction;

public class ElementFactory {

   public static final String leftCollectionAttrName = "left_collection";
   public static final String rightCollectionAttrName = "right_collection";

   private ElementFactory() {
      // do nothing!
   }

   public static Edge createEdge(String edgeKey, Node leftNode, Node rightNode) {
      Edge result = new Edge(edgeKey, leftNode.getId(), rightNode.getId());

      String leftCollectionName = getCollectionName(leftNode);
      result.addAttribute(leftCollectionAttrName, leftCollectionName);

      String rightCollectionName = getCollectionName(rightNode);
      result.addAttribute(rightCollectionAttrName, rightCollectionName);
      return result;
   }

   public static String getCollectionName(Element element) {
      String id = element.getId();
      int iend = id.indexOf("/");
      String collectionName;
      if (iend != -1) {
         collectionName = id.substring(0, iend);
      } else {
         throw new RuntimeException("improper format.");
      }
      return collectionName;
   }

   public static String getCollectionName(String id) {
      int iend = id.indexOf("/");
      String collectionName;
      if (iend != -1) {
         collectionName = id.substring(0, iend);
      } else {
         throw new RuntimeException("improper format.");
      }
      return collectionName;
   }

   public static String getLeftAttrString(Direction direction) {
      if (direction == Direction.outbound) {
         return "_from";
      } else if (direction == Direction.inbound) {
         return "_to";
      }
      throw new RuntimeException("unsupported direction.");
   }

   public static Object getRightIdString(Direction direction, Edge edge) {
      // getTo()
      if (direction == Direction.outbound) {
         return edge.getTo();
      } else if (direction == Direction.inbound) {
         return edge.getFrom();
      }
      throw new RuntimeException("unsupported direction.");
   }
}
