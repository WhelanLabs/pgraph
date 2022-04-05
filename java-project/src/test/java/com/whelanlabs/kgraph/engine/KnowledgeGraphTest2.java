package com.whelanlabs.kgraph.engine;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.whelanlabs.kgraph.engine.Edge;
import com.whelanlabs.kgraph.engine.ElementFactory;
import com.whelanlabs.kgraph.engine.KnowledgeGraph;
import com.whelanlabs.kgraph.engine.Node;
import com.whelanlabs.kgraph.engine.QueryClause;
import com.whelanlabs.kgraph.engine.QueryClause.Operator;

public class KnowledgeGraphTest2 {
   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "KnowledgeGraphTests_db";

   private static Logger logger = LogManager.getLogger(KnowledgeGraphTest2.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      kGraph = new KnowledgeGraph(tablespace_name);
      kGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      kGraph.cleanup();
   }

   @Test(expected = ArangoDBException.class)
   public void upsertNode_edgeCollection_exception() {
      final ArangoCollection collection = kGraph.getEdgeCollection(KnowledgeGraph.generateName());
      final Node baseDoc = new Node(KnowledgeGraph.generateKey());
      baseDoc.addAttribute("foo", "bar");
      kGraph.upsertNode(collection, baseDoc);
   }

   @Test(expected = RuntimeException.class)
   public void getEdgeCollection_isNodeCollection_exception() {
      kGraph.getNodeCollection("node_collection");
      kGraph.getEdgeCollection("node_collection");
   }

   @Test(expected = RuntimeException.class)
   public void getNodeCollection_isEdgeCollection_exception() {
      kGraph.getEdgeCollection("edge_collection");
      kGraph.getNodeCollection("edge_collection");
   }

}
