package com.whelanlabs.kgraph.engine;

import com.arangodb.model.TraversalOptions.Direction;

public class ElementHelper {

   private ElementHelper() {
      // do nothing!
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
