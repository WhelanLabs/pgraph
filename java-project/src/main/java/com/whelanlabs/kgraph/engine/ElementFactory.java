package com.whelanlabs.kgraph.engine;

public class ElementFactory {

   public static final String leftCollectionAttrName = "left_collection";
   public static final String rightCollectionAttrName = "right_collection";

   private ElementFactory() {
      // do nothing!
   }
   
   public static Edge createEdge(String edgeKey, Node leftNode, Node rightNode) {
      Edge result = new Edge(edgeKey, leftNode.getId(), rightNode.getId());

      String leftCollectionName = getCollectionName(leftNode);
      result.addAttribute(leftCollectionAttrName, leftCollectionName);

      String rightCollectionName = getCollectionName(rightNode);
      result.addAttribute(rightCollectionAttrName, rightCollectionName);
      return result;
   }

   protected static String getCollectionName(Node node) {
      String id = node.getId();
      int iend = id.indexOf("/");
      String collectionName;
      if (iend != -1) {
         collectionName = id.substring(0, iend);
      } else {
         throw new RuntimeException("improper format.");
      }
      return collectionName;
   }

}
