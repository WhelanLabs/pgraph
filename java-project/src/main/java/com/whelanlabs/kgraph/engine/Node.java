package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseDocument;

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
}
