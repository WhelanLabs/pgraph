package com.whelanlabs.kgraph.test.engine;

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
import com.whelanlabs.kgraph.engine.Edge;
import com.whelanlabs.kgraph.engine.KnowledgeGraph;
import com.whelanlabs.kgraph.engine.Node;
import com.whelanlabs.kgraph.engine.QueryClause;
import com.whelanlabs.kgraph.engine.QueryClause.Operator;

public class KnowledgeGraphTest {
   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "KnowledgeGraphTests_db";

   private static Logger logger = LogManager.getLogger(KnowledgeGraphTest.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      kGraph = new KnowledgeGraph(tablespace_name);
      kGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      kGraph.cleanup();
   }

   @Test
   public void upsertNode_newNode_added() {
      String collectionName = KnowledgeGraph.generateName();
      final ArangoCollection testNodeCollection = kGraph.getNodeCollection(collectionName);
      Node testNode = new Node(KnowledgeGraph.generateKey());
      testNode.addAttribute("foo", "bbar");
      testNode = kGraph.upsertNode(testNodeCollection, testNode);

      Node result = kGraph.getNodeByKey(testNode.getKey(), collectionName);
      String attr = (String) result.getAttribute("foo");
      assert ("bbar".equals(attr));
   }

   @Test
   public void upsertNode_existingNode_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("dates");
      final Node badDate = new Node(KnowledgeGraph.generateKey());
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
      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection");
      edge.addAttribute("foo", "bar");

      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edge);

      Edge result = kGraph.getEdgeByKey(edgeKey, "testEdgeCollection");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_existingEdge_added() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");

      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection");
      edge.addAttribute("foo", "bar");

      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edge);
      edge.addAttribute("foo-foo", "bar-bar");
      edge = kGraph.upsertEdge(edge);

      Edge result = kGraph.getEdgeByKey(edgeKey, "testEdgeCollection");
      String fooAttr = (String) result.getAttribute("foo");
      assert ("bar".equals(fooAttr)) : "foo value is " + fooAttr;
      String fooFooAttr = (String) result.getAttribute("foo-foo");
      assert ("bar-bar".equals(fooFooAttr)) : "foo-foo value is " + fooFooAttr;
   }

   @Test(expected = NullPointerException.class)
   public void upsertEdge_collectionIsNull_exception() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");

      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, null);
      edge.addAttribute("foo", "bar");

      edge.setKey(edgeKey);
      edge = kGraph.upsertEdge(edge);
   }

   @Test(expected = NullPointerException.class)
   public void upsertEdge_edgeIsNull_exception() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");

      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      Edge edge = null;
      kGraph.upsertEdge(edge);
   }

   @Test(expected = NullPointerException.class)
   public void upsertEdge_keyIsNull_exception() {
      final ArangoCollection dates = kGraph.getNodeCollection("testNodeCollection");

      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      kGraph.upsertNode(dates, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "someEdgeCollection");
      edge.addAttribute("foo", "bar");

      edge.setKey(null);
      edge = kGraph.upsertEdge(edge);
   }

   @Test
   public void upsertNode_freshAndValid_collectionsExist() throws Exception {
      Long beginSize = kGraph.getTotalCount();
      final ArangoCollection dates = kGraph.getNodeCollection("dates");
      final Node badDate = new Node(KnowledgeGraph.generateKey());
      badDate.addAttribute("foo", "bbar");
      kGraph.upsertNode(dates, badDate);

      Long endSize = kGraph.getTotalCount();
      assert (endSize == beginSize + 1) : "{beginSize, endsize} is {" + beginSize + ", " + endSize + "}";
   }

   @Test
   public void queryNodes_singleClause_getResult() throws Exception {
      final ArangoCollection testCollection = kGraph.getNodeCollection("testCollection");
      final Node testDoc = new Node(KnowledgeGraph.generateKey());
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
      final Node testDoc = new Node(KnowledgeGraph.generateKey());
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
      kGraph.queryNodes(testCollection, queryClause1);
   }

   @Test
   public void expandRight_noFilters_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      final ArangoCollection testCollection = kGraph.getNodeCollection("testNodeCollection");
      kGraph.upsertNode(testCollection, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection");
      edge = kGraph.upsertEdge(edge);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, "testEdgeCollection", relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void queryEdge_edgeExists_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      final ArangoCollection testCollection = kGraph.getNodeCollection("testNodeCollection");
      kGraph.upsertNode(testCollection, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection2");
      edge.addAttribute("foo", "bar");
      edge = kGraph.upsertEdge(edge);
      logger.debug("edge.id: " + edge.getId());

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      List<Edge> results = kGraph.queryEdges("testEdgeCollection2", queryClause);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test(expected = NullPointerException.class)
   public void queryEdge_nullCollection_exception() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      final ArangoCollection testCollection = kGraph.getNodeCollection("testNodeCollection");
      kGraph.upsertNode(testCollection, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection2");
      edge.addAttribute("foo", "bar");
      edge = kGraph.upsertEdge(edge);
      logger.debug("edge.id: " + edge.getId());

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      kGraph.queryEdges(null, queryClause);
   }

   @Test
   public void expandLeft_noFilters_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      final ArangoCollection testCollection = kGraph.getNodeCollection("testNodeCollection");
      kGraph.upsertNode(testCollection, leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeCollection");
      edge = kGraph.upsertEdge(edge);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandLeft(rightNode, "testEdgeCollection", relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void expandRight_oneRelClause_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode1 = new Node(KnowledgeGraph.generateKey());
      final Node rightNode2 = new Node(KnowledgeGraph.generateKey());
      final Node rightNode3 = new Node(KnowledgeGraph.generateKey());

      final ArangoCollection testCollection = kGraph.getNodeCollection(KnowledgeGraph.generateName());
      kGraph.upsertNode(testCollection, leftNode, rightNode1, rightNode2, rightNode3);

      String edgeCollectionName = KnowledgeGraph.generateName();
      String edgeKey1 = leftNode.getKey() + ":" + rightNode1.getKey();
      String edgeKey2 = leftNode.getKey() + ":" + rightNode2.getKey();
      String edgeKey3 = leftNode.getKey() + ":" + rightNode3.getKey();

      Edge edge1 = new Edge(edgeKey1, leftNode, rightNode1, edgeCollectionName);
      Edge edge2 = new Edge(edgeKey2, leftNode, rightNode2, edgeCollectionName);
      Edge edge3 = new Edge(edgeKey3, leftNode, rightNode3, edgeCollectionName);
      edge1.addAttribute("edgeVal", "good");
      edge3.addAttribute("edgeVal", "good");

      ArrayList<Edge> edges = kGraph.upsertEdge(edge1, edge2, edge3);
      assert (3 == edges.size()) : "edges.size() = " + edges.size();

      List<QueryClause> relClauses = new ArrayList<>();
      relClauses.add(new QueryClause("edgeVal", Operator.EQUALS, "good"));
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, edgeCollectionName, relClauses, otherSideClauses);

      assert (2 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void expandLeft_oneOtherSideClause_getResults() {
      final Node rightNode = new Node(KnowledgeGraph.generateKey());
      final Node leftNode1 = new Node(KnowledgeGraph.generateKey());
      final Node leftNode2 = new Node(KnowledgeGraph.generateKey());
      final Node leftNode3 = new Node(KnowledgeGraph.generateKey());
      leftNode1.addAttribute("nodeVal", "good");
      leftNode3.addAttribute("nodeVal", "good");

      final ArangoCollection testCollection = kGraph.getNodeCollection(KnowledgeGraph.generateName());
      kGraph.upsertNode(testCollection, rightNode, leftNode1, leftNode2, leftNode3);

      String edgeCollectionName = KnowledgeGraph.generateName();
      String edgeKey1 = leftNode1.getKey() + ":" + rightNode.getKey();
      String edgeKey2 = leftNode2.getKey() + ":" + rightNode.getKey();
      String edgeKey3 = leftNode3.getKey() + ":" + rightNode.getKey();

      Edge edge1 = new Edge(edgeKey1, leftNode1, rightNode, edgeCollectionName);
      Edge edge2 = new Edge(edgeKey2, leftNode2, rightNode, edgeCollectionName);
      Edge edge3 = new Edge(edgeKey3, leftNode3, rightNode, edgeCollectionName);

      ArrayList<Edge> edges = kGraph.upsertEdge(edge1, edge2, edge3);
      assert (3 == edges.size()) : "edges.size() = " + edges.size();

      List<QueryClause> otherSideClauses = new ArrayList<>();
      otherSideClauses.add(new QueryClause("nodeVal", Operator.EQUALS, "good"));
      List<QueryClause> relClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandLeft(rightNode, edgeCollectionName, relClauses, otherSideClauses);

      assert (2 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void expandRight_multiRelMultiOtherClauses_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey());
      final Node rightNode1 = new Node(KnowledgeGraph.generateKey());
      final Node rightNode2 = new Node(KnowledgeGraph.generateKey());
      final Node rightNode3 = new Node(KnowledgeGraph.generateKey());
      rightNode1.addAttribute("nodeVal1", "good");
      rightNode2.addAttribute("nodeVal1", "good");
      rightNode1.addAttribute("nodeVal2", "better");
      rightNode2.addAttribute("nodeVal2", "better");
      rightNode3.addAttribute("nodeVal2", "better");

      final ArangoCollection testCollection = kGraph.getNodeCollection(KnowledgeGraph.generateName());
      kGraph.upsertNode(testCollection, leftNode, rightNode1, rightNode2, rightNode3);
      String rn1id = rightNode1.getId();

      String edgeCollectionName = KnowledgeGraph.generateName();
      String edgeKey1 = leftNode.getKey() + ":" + rightNode1.getKey();
      String edgeKey2 = leftNode.getKey() + ":" + rightNode2.getKey();
      String edgeKey3 = leftNode.getKey() + ":" + rightNode3.getKey();

      Edge edge1 = new Edge(edgeKey1, leftNode, rightNode1, edgeCollectionName);
      Edge edge2 = new Edge(edgeKey2, leftNode, rightNode2, edgeCollectionName);
      Edge edge3 = new Edge(edgeKey3, leftNode, rightNode3, edgeCollectionName);
      edge1.addAttribute("edgeVal1", "good");
      edge3.addAttribute("edgeVal1", "good");
      edge1.addAttribute("edgeVal2", "better");
      edge2.addAttribute("edgeVal2", "better");
      edge3.addAttribute("edgeVal2", "better");

      ArrayList<Edge> edges = kGraph.upsertEdge(edge1, edge2, edge3);
      assert (3 == edges.size()) : "edges.size() = " + edges.size();

      List<QueryClause> relClauses = new ArrayList<>();
      relClauses.add(new QueryClause("edgeVal1", Operator.EQUALS, "good"));
      relClauses.add(new QueryClause("edgeVal2", Operator.EQUALS, "better"));
      List<QueryClause> otherSideClauses = new ArrayList<>();
      otherSideClauses.add(new QueryClause("nodeVal1", Operator.EQUALS, "good"));
      otherSideClauses.add(new QueryClause("nodeVal2", Operator.EQUALS, "better"));
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, edgeCollectionName, relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();

      String resultID = results.get(0).getRight().getId();
      assert (rn1id.equals(resultID)) : "rn1id = " + rn1id + ", resultID = " + resultID;
   }
}
