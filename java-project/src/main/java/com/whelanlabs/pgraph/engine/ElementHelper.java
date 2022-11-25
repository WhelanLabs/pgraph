package com.whelanlabs.pgraph.engine;

import com.arangodb.model.TraversalOptions.Direction;

/**
 * The Class ElementHelper.  This class is intended to provide 
 * static convenience functions for working with Elements.
 * 
 * This class is not complete in the sense of supporting all
 * possible combinations.  For example, it currently lacks a
 * "getRightAttrString()" method because there isn't (yet?)
 * a need for such a function. (Given the Direction flipping
 * logic in PGraph, it is likely to have complete traversal
 * abilities without having symmetric helper functions.)
 */
public class ElementHelper {

   /** The count. */
   private static Long count = 0L;
   
   /**
    * Instantiates a new element helper.
    */
   private ElementHelper() {
      // do nothing!
   }

   /**
    * Gets the left attribute string in the context of the direction
    * of the expand, meaning that if expanding from right to left,
    * it instead returns the right attribute.
    *
    * @param direction the direction of the traversal.
    * @return the left attr string, AKA the "from" side.
    */
   public static String getLeftAttrString(Direction direction) {
      if (direction == Direction.outbound) {
         return "_from";
      } else if (direction == Direction.inbound) {
         return "_to";
      }
      throw new RuntimeException("unsupported direction.");
   }

   /**
    * Gets the right id string in the context of the direction
    * of the expand, meaning that if expanding from right to left,
    * it instead returns the left id.
    *
    * @param direction the direction of the traversal.
    * @param edge the edge
    * @return the right id string, AKA the "to" side.
    */
   public static Object getRightIdString(Direction direction, Edge edge) {
      // getTo()
      if (direction == Direction.outbound) {
         return edge.getTo();
      } else if (direction == Direction.inbound) {
         return edge.getFrom();
      }
      throw new RuntimeException("unsupported direction.");
   }

   /**
    * Generates a unique random string that can be used as a key.
    *
    * @return the string
    */
   public static String generateKey() {
      return "KEY_" + System.currentTimeMillis() + count++;
   }

   /**
    * Generate a random and unique name that fits for being a ArangoDB
    * element ID or Collection Name.
    *
    * @return the string
    */
   public static String generateName() {
      return "NAME_" + System.currentTimeMillis() + count++;
   }
   
   
}
