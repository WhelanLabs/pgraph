package com.whelanlabs.kgraph.engine;

import com.arangodb.model.TraversalOptions.Direction;

public class ElementFactory {



   private ElementFactory() {
      // do nothing!
   }

   public static String getTypeName(Element element) {
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

   public static String getTypeName(String id) {
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
