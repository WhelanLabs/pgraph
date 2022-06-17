package com.whelanlabs.kgraph.engine;

import java.util.Collection;

/**
 * The Interface Element.  Elements are the logical supertype for
 * Nodes and Edges in a Property Graph.
 */
public interface Element {

   /** The Constant typeAttrName. This correlates to the name of the 
    *  Collection in ArangoDB.*/
   public static final String typeAttrName = "__type";

   /**
    * Gets the id.
    *
    * @return the id
    */
   String getId();
   
   String getKey();

   /**
    * Gets the value of the attribute.
    *
    * @param key the key value for the given attribute.
    * @return the attribute
    */
   Object getAttribute(final String key);

   /**
    * Gets the type.
    *
    * @return the type
    */
   default String getType() {
      String typeName = (String) getAttribute(typeAttrName);
      return typeName;
   }
   
   String toJson();
   
   static String toJson(Collection<Element> elements) {
      String result = "";
      Boolean first = true;

      result += "{\n";
      for(Element element : elements) {
         if(first) {
            first = false;
         }
         else {
            result += ",\n";
         }
         result += element.toJson();
      }
      result += "}\n";

      return result;
   }
}