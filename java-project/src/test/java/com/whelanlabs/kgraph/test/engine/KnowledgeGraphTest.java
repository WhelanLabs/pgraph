package com.whelanlabs.kgraph.test.engine;

import static org.junit.Assert.assertNotNull;

import java.util.Collection;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDBException;
import com.arangodb.entity.PathEntity;
import com.whelanlabs.kgraph.engine.Edge;
import com.whelanlabs.kgraph.engine.ElementFactory;
import com.whelanlabs.kgraph.engine.KnowledgeGraph;
import com.whelanlabs.kgraph.engine.Node;
import com.whelanlabs.kgraph.engine.QueryClause;

public class KnowledgeGraphTest {
   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "KnowledgeGraphTests_db";

   private static Logger logger = LogManager.getLogger(KnowledgeGraphTest.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      // KnowledgeGraph.removeTablespace(tablespace_name);
      kGraph = new KnowledgeGraph(tablespace_name);
      kGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      kGraph.cleanup();
   }

   @Test(expected = ArangoDBException.class)
   public void upsertNode_edgeCollection_exception() {
      final ArangoCollection collection = kGraph.getEdgeCollection(kGraph.generateName());
      final Node baseDoc = new Node(kGraph.generateKey());
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

   @Test
   public void upsertNode_newNode_added() {
      String collectionName = kGraph.generateName();
      final ArangoCollection testNodeCollection = kGraph.getNodeCollection(collectionName);
      Node testNode = new Node(kGraph.generateKey());
      testNode.addAttribute("foo", "bbar");
      testNode = kGraph.upsertNode(testNodeCollection, testNode);

      Node result = kGraph.getNodeByKey(testNode.getKey(), collectionName);
      String attr = (String) result.getAttribute("foo");
      assert ("bbar".equals(attr));
   }

   @Test
   public void upsertNode_existingNode_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("dates");
      final Node badDate = new Node(kGraph.generateKey());
      kGraph.upsertNode(dates, badDate);
      badDate.addAttribute("foo", "bar");
      kGraph.upsertNode(dates, badDate);
      Node result = kGraph.getNodeByKey(badDate.getKey(), "dates");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_newEdge_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");
      final Node leftNode = new Node(kGraph.generateKey());
      final Node rightNode = new Node(kGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection");
      Edge edge = new Edge();
      edge.setFrom(leftNode.getId());
      edge.setTo(rightNode.getId());
      edge.addAttribute("foo", "bar");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edgeCollection, edge);

      Edge result = kGraph.getEdgeByKey(edgeKey, "testEdgeCollection");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_existingEdge_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");

      final Node leftNode = new Node(kGraph.generateKey());
      final Node rightNode = new Node(kGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection");
      Edge edge = new Edge();
      edge.setFrom(leftNode.getId());
      edge.setTo(rightNode.getId());
      edge.addAttribute("foo", "bar");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edgeCollection, edge);
      edge.addAttribute("foo-foo", "bar-bar");
      edge = kGraph.upsertEdge(edgeCollection, edge);

      Edge result = kGraph.getEdgeByKey(edgeKey, "testEdgeCollection");
      String fooAttr = (String) result.getAttribute("foo");
      assert ("bar".equals(fooAttr)) : "foo value is " + fooAttr;
      String fooFooAttr = (String) result.getAttribute("foo-foo");
      assert ("bar-bar".equals(fooFooAttr)) : "foo-foo value is " + fooFooAttr;
   }

   @Test(expected = NullPointerException.class)
   public void upsertEdge_collectionIsNull_exception() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");

      final Node leftNode = new Node(kGraph.generateKey());
      final Node rightNode = new Node(kGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = null;
      Edge edge = new Edge();
      edge.setFrom(leftNode.getId());
      edge.setTo(rightNode.getId());
      edge.addAttribute("foo", "bar");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edgeCollection, edge);
   }

   @Test(expected = NullPointerException.class)
   public void upsertEdge_edgeIsNull_exception() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");

      final Node leftNode = new Node(kGraph.generateKey());
      final Node rightNode = new Node(kGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection");
      Edge edge = null;
      kGraph.upsertEdge(edgeCollection, edge);
   }

   @Test
   public void upsertNode_freshAndValid_collectionsExist() throws Exception {
      Long beginSize = kGraph.getTotalCount();
      final ArangoCollection dates = kGraph.getNodeCollection("dates");
      final Node badDate = new Node(kGraph.generateKey());
      badDate.addAttribute("foo", "bbar");
      kGraph.upsertNode(dates, badDate);

      Long endSize = kGraph.getTotalCount();
      assert (endSize == beginSize + 1) : "{beginSize, endsize} is {" + beginSize + ", " + endSize + "}";
   }

   @Test
   public void queryNodes_singleClause_getResult() throws Exception {
      final ArangoCollection testCollection = kGraph.getNodeCollection("testCollection");
      final Node testDoc = new Node(kGraph.generateKey());
      testDoc.addAttribute("foo", "bar");
      testDoc.addAttribute("xname", "Homer");
      kGraph.upsertNode(testCollection, testDoc);
      Node addedDoc = kGraph.getNodeByKey(testDoc.getKey(), testCollection.name());
      assertNotNull(addedDoc);
      logger.debug("addedDoc = " + addedDoc.toString());

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      List<Node> results = kGraph.queryNodes(testCollection, queryClause);
      assertNotNull(results);
      logger.debug("results = " + results);
      assert (results.size() > 0) : "results.size() = " + results.size();
   }

   @Test
   public void queryNodes_multipleClauses_getResult() throws Exception {
      final ArangoCollection testCollection = kGraph.getNodeCollection("testCollection");
      final Node testDoc = new Node(kGraph.generateKey());
      testDoc.addAttribute("foo", "bar");
      testDoc.addAttribute("foofoo", "barbar");
      kGraph.upsertNode(testCollection, testDoc);
      Node addedDoc = kGraph.getNodeByKey(testDoc.getKey(), testCollection.name());
      assertNotNull(addedDoc);
      logger.debug("addedDoc = " + addedDoc.toString());

      QueryClause queryClause1 = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      QueryClause queryClause2 = new QueryClause("foofoo", QueryClause.Operator.EQUALS, "barbar");
      List<Node> results = kGraph.queryNodes(testCollection, queryClause1, queryClause2);
      assertNotNull(results);
      logger.debug("results = " + results);
      assert (results.size() > 0) : "results.size() = " + results.size();
   }

   @Test(expected = NullPointerException.class)
   public void queryNodes_nullClauses_exception() throws Exception {
      final ArangoCollection testCollection = null;
      QueryClause queryClause1 = new QueryClause("foo", QueryClause.Operator.EQUALS, null);
      List<Node> results = kGraph.queryNodes(testCollection, queryClause1);
   }

   @Test
   public void expandRight_tripleExists_getResults() {
      final Node leftNode = new Node(kGraph.generateKey());
      final Node rightNode = new Node(kGraph.generateKey());
      final ArangoCollection testCollection = kGraph.getNodeCollection("testNodeCollection");
      kGraph.upsertNode(testCollection, leftNode, rightNode);
      logger.debug("leftNode.id: " + leftNode.getId());
      logger.debug("rightNode.id: " + rightNode.getId());

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = ElementFactory.createEdge(edgeKey, leftNode, rightNode);
      edge = kGraph.upsertEdge(edgeCollection, edge);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      Collection<PathEntity<Node, Edge>> results = kGraph.expandRight(leftNode, edgeCollection, relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();

      for (PathEntity<Node, Edge> result : results) {
         logger.debug("result.vertices: " + result.getVertices());
         logger.debug("result.edges: " + result.getEdges());
      }
   }
   
   @Test
   public void queryEdge_edgeExists_getResults() {
      final Node leftNode = new Node(kGraph.generateKey());
      final Node rightNode = new Node(kGraph.generateKey());
      final ArangoCollection testCollection = kGraph.getNodeCollection("testNodeCollection");
      kGraph.upsertNode(testCollection, leftNode, rightNode);
      logger.debug("leftNode.id: " + leftNode.getId());
      logger.debug("rightNode.id: " + rightNode.getId());

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection2");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = ElementFactory.createEdge(edgeKey, leftNode, rightNode);
      edge.addAttribute("foo", "bar");
      edge = kGraph.upsertEdge(edgeCollection, edge);
      logger.debug("edge.id: " + edge.getId());
      

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      List<Edge> results = kGraph.queryEdges(edgeCollection, queryClause);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }
   
   @Test(expected = NullPointerException.class)
   public void queryEdge_nullCollection_exception() {
      final Node leftNode = new Node(kGraph.generateKey());
      final Node rightNode = new Node(kGraph.generateKey());
      final ArangoCollection testCollection = kGraph.getNodeCollection("testNodeCollection");
      kGraph.upsertNode(testCollection, leftNode, rightNode);
      logger.debug("leftNode.id: " + leftNode.getId());
      logger.debug("rightNode.id: " + rightNode.getId());

      ArangoCollection edgeCollection = kGraph.getEdgeCollection("testEdgeCollection2");
      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = ElementFactory.createEdge(edgeKey, leftNode, rightNode);
      edge.addAttribute("foo", "bar");
      edge = kGraph.upsertEdge(edgeCollection, edge);
      logger.debug("edge.id: " + edge.getId());
      

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      List<Edge> results = kGraph.queryEdges(null, queryClause);
   }
}
