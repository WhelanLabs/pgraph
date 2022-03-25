package com.whelanlabs.kgraph.engine;

import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;

public class ElementFactory {

   public static final String leftCollectionAttrName = "left_collection";
   public static final String rightCollectionAttrName = "right_collection";

   public static BaseEdgeDocument createEdge(String edgeKey, BaseDocument leftNode, BaseDocument rightNode) {
      BaseEdgeDocument result = new BaseEdgeDocument(edgeKey, leftNode.getId(), rightNode.getId());

      String leftCollectionName = getCollectionName(leftNode);
      result.addAttribute(leftCollectionAttrName, leftCollectionName);

      String rightCollectionName = getCollectionName(rightNode);
      result.addAttribute(rightCollectionAttrName, rightCollectionName);
      return result;
   }

   private static String getCollectionName(BaseDocument node) {
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
