package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseEdgeDocument;

// TODO: Auto-generated Javadoc
/**
 * The Class Edge.
 */
public class Edge extends BaseEdgeDocument implements Element {

   /** The Constant leftTypeAttrName. Identifies the collection for the left
    *  end-point of an edge.*/
   public static final String leftTypeAttrName = "__left_collection";

   /** The Constant rightTypeAttrName. Identifies the collection for the right
    *  end-point of an edge.*/
   public static final String rightTypeAttrName = "__right_collection";

   /** The Constant edgeTypeAttrName. Identifies the collection for a
    *  given edge.  Also used in
    *  end-point of an edge.*/
   public static final String edgeTypeAttrName = "__edge_collection";

   /**
    * Instantiates a new edge.
    *
    * @param edgeKey the edge key
    * @param left the left
    * @param right the right
    * @param type the type
    */
   public Edge(String edgeKey, Node left, Node right, String type) {
      super(edgeKey, left.getId(), right.getId());
      this.addAttribute(typeAttrName, type);
      String leftCollectionName = ElementFactory.getTypeName(left);
      String rightCollectionName = ElementFactory.getTypeName(right);
      this.addAttribute(leftTypeAttrName, leftCollectionName);
      this.addAttribute(rightTypeAttrName, rightCollectionName);
   }

   /**
    * Instantiates a new edge.
    *
    * @param properties the properties
    */
   public Edge(final Map<String, Object> properties) {
      super(properties);
   }

   /** The Constant serialVersionUID. */
   private static final long serialVersionUID = -4801740400388403434L;

}
