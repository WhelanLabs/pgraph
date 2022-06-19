package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;

/**
 * The Class Node.
 */
public class Node extends BaseDocument implements Element {

   /**
    * Instantiates a new node.
    *
    * @param key the key
    * @param type the type
    */
   public Node(String key, String type) {
      super(key);
      id = type + "/" + key;
      this.addAttribute(typeAttrName, type);
   }

   /**
    * Instantiates a new node.
    * 
    * This constructor is not intended for use by client code.
    *
    * @param properties the properties
    */
   protected Node(final Map<String, Object> properties) {
      super(properties);
   }

   /** The Constant serialVersionUID. */
   private static final long serialVersionUID = -1109802074556650431L;

   @Override
   public String toJson() throws Exception {
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      String json = ow.writeValueAsString(this);
      return json;
   }
}
