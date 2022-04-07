package com.whelanlabs.kgraph.engine;

import java.util.ArrayList;
import java.util.List;

public class ElementList<T> extends ArrayList<T> {

   private static final long serialVersionUID = 473662690195525876L;

   public List<Node> getNodes() {
      List<Node> results = new ArrayList<>();
      for (Object element : this) {
         if (element instanceof Node) {
            results.add((Node) element);
         }
      }
      return results;
   }

   public List<Edge> getEdges() {
      List<Edge> results = new ArrayList<>();
      for (Object element : this) {
         if (element instanceof Edge) {
            results.add((Edge) element);
         }
      }
      return results;
   }
}
