package com.whelanlabs.kgraph.engine;

// TODO: Auto-generated Javadoc
/**
 * The Interface Element.
 */
public interface Element {

   /** The Constant typeAttrName. */
   public static final String typeAttrName = "__type";

   /**
    * Gets the id.
    *
    * @return the id
    */
   String getId();

   /**
    * Gets the attribute.
    *
    * @param key the key
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
}