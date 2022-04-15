package com.whelanlabs.kgraph.engine;

import java.util.ArrayList;
import java.util.List;

/**
 * The Class ElementList.  This class is a List specifically designed to
 * allow sublists of Nodes and Edges to be generated easily.
 *
 * @param <T> the generic type
 */
public class ElementList<T> extends ArrayList<T> {

   /** The Constant serialVersionUID. */
   private static final long serialVersionUID = 473662690195525876L;

   /**
    * Gets the nodes.
    *
    * @return the nodes
    */
   public List<Node> getNodes() {
      List<Node> results = new ArrayList<>();
      for (Object element : this) {
         if (element instanceof Node) {
            results.add((Node) element);
         }
      }
      return results;
   }

   /**
    * Gets the edges.
    *
    * @return the edges
    */
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
