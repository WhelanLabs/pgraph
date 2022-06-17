package com.whelanlabs.kgraph.engine;

import java.util.Map;

import com.arangodb.entity.BaseEdgeDocument;

/**
 * The Class Edge.
 */
public class Edge extends BaseEdgeDocument implements Element {

   /** The Constant serialVersionUID. */
   private static final long serialVersionUID = -4801740400388403434L;

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
    * Periodic errors may occur if using non-persisted nodes.
    *
    * @param edgeKey the edge key
    * @param left the left
    * @param right the right
    * @param type the type
    */
   public Edge(String edgeKey, Node left, Node right, String type) {
      super(edgeKey, left.getId(), right.getId());
      this.addAttribute(typeAttrName, type);
      String leftCollectionName = left.getType();
      String rightCollectionName = right.getType();
      this.addAttribute(leftTypeAttrName, leftCollectionName);
      this.addAttribute(rightTypeAttrName, rightCollectionName);
   }

   public Edge(String edgeKey, String leftId, String rightId, String leftType, String rightType, String type) {
      super(edgeKey, leftType + "/" + leftId, rightType + "/" + rightId);
      this.addAttribute(typeAttrName, type);
      this.addAttribute(leftTypeAttrName, leftType);
      this.addAttribute(rightTypeAttrName, rightType);
   }
   
   /**
    * Instantiates a new edge.
    * 
    * This constructor is largely intended for use in deserializing
    * objects, not for initial construction by calling applications.
    *
    * @param properties the properties for the created edge
    */
   protected Edge(final Map<String, Object> properties) {
      super(properties);
   }

   public String getLeftType() {
      return getAttribute(leftTypeAttrName).toString();
   }

   public String getRightType() {
      return getAttribute(rightTypeAttrName).toString();
   }

   @Override
   public String toJson() {
      // TODO Auto-generated method stub
      return null;
   }
   
}
