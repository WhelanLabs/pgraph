package com.whelanlabs.kgraph.engine;

import java.util.Collection;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * The Interface Element.  Elements are the logical parent type for
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

   String toJson() throws Exception;

   static String toJson(Collection<Element> elements) throws Exception {

      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      String json = ow.writeValueAsString(elements);
      return json;
   }

   static String toDot(Collection<Element> elements) throws Exception {
      // see also: https://renenyffenegger.ch/notes/tools/Graphviz/examples/index
//      digraph L {
//         node [shape=record fontname=Arial];
//         a  [label="one\ltwo three\lfour five six seven\l"]
//         b  [label="one\ntwo three\nfour five six seven"]
//         c  [label="one\rtwo three\rfour five six seven\r"]
//         a -> b -> c
//       }
      StringBuilder s = new StringBuilder();
      s.append("digraph G {\n");
      s.append("node [shape=record fontname=Arial];\n");
      for (Element e : elements) {
         s.append(e.toDot());
      }

      s.append("node [shape=record fontname=Arial];\n");

      return s.toString();
   }

   String toDot();

}