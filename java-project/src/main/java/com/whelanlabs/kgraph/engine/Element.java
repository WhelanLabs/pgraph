package com.whelanlabs.kgraph.engine;

public interface Element {

   public static final String typeAttrName = "__type";
   
   String getId();
   Object getAttribute(final String key);
   
   default String getType() {
      String typeName = (String)getAttribute(typeAttrName);
      return typeName;
   }
}