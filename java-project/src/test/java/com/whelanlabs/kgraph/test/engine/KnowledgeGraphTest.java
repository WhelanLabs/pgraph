package com.whelanlabs.kgraph.test.engine;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoDBException;
import com.whelanlabs.kgraph.engine.Edge;
import com.whelanlabs.kgraph.engine.Element;
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
      String typeName = KnowledgeGraph.generateName();
      Node testNode = new Node(KnowledgeGraph.generateKey(), typeName);
      testNode.addAttribute("foo", "bbar");
      testNode = kGraph.upsert(testNode).getNodes().get(0);

      Node result = kGraph.getNodeByKey(testNode.getKey(), typeName);
      String attr = (String) result.getAttribute("foo");
      assert ("bbar".equals(attr));
   }

   @Test
   public void upsertNode_existingNode_added() {
      final Node badDate = new Node(KnowledgeGraph.generateKey(), "dates");
      kGraph.upsert(badDate);
      badDate.addAttribute("foo", "bar");
      kGraph.upsert(badDate);
      Node result = kGraph.getNodeByKey(badDate.getKey(), "dates");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_newEdge_added() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType");
      edge.addAttribute("foo", "bar");

      edge.setKey(edgeKey);
      edge = kGraph.upsert(edge).getEdges().get(0);

      Edge result = kGraph.getEdgeByKey(edgeKey, "testEdgeType");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_existingEdge_added() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType");
      edge.addAttribute("foo", "bar");

      edge = kGraph.upsert(edge).getEdges().get(0);
      edge.addAttribute("foo-foo", "bar-bar");
      edge = kGraph.upsert(edge).getEdges().get(0);

      Edge result = kGraph.getEdgeByKey(edgeKey, "testEdgeType");
      String fooAttr = (String) result.getAttribute("foo");
      assert ("bar".equals(fooAttr)) : "foo value is " + fooAttr;
      String fooFooAttr = (String) result.getAttribute("foo-foo");
      assert ("bar-bar".equals(fooFooAttr)) : "foo-foo value is " + fooFooAttr;
   }

   @Test(expected = NullPointerException.class)
   public void upsertEdge_typeIsNull_exception() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, null);
      edge.addAttribute("foo", "bar");

      edge.setKey(edgeKey);
      edge = kGraph.upsert(edge).getEdges().get(0);
   }

   @Test(expected = RuntimeException.class)
   public void upsertEdge_edgeIsNull_exception() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      Edge edge = null;
      kGraph.upsert(edge);
   }

   @Test(expected = NullPointerException.class)
   public void upsertEdge_keyIsNull_exception() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "someEdgeType");
      edge.addAttribute("foo", "bar");

      edge.setKey(null);
      edge = kGraph.upsert(edge).getEdges().get(0);
   }

   @Test
   public void upsertNode_freshAndValid_typesExist() throws Exception {
      Long beginSize = kGraph.getTotalCount();
      final Node badDate = new Node(KnowledgeGraph.generateKey(), "dates");
      badDate.addAttribute("foo", "bbar");
      kGraph.upsert(badDate);

      Long endSize = kGraph.getTotalCount();
      assert (endSize > beginSize) : "{beginSize, endsize} is {" + beginSize + ", " + endSize + "}";
   }

   @Test
   public void queryNodes_singleClause_getResult() throws Exception {
      final Node testDoc = new Node(KnowledgeGraph.generateKey(), "testType");
      testDoc.addAttribute("foo", "bar");
      testDoc.addAttribute("xname", "Homer");
      kGraph.upsert(testDoc);
      Node addedDoc = kGraph.getNodeByKey(testDoc.getKey(), "testType");
      assertNotNull(addedDoc);
      logger.debug("addedDoc = " + addedDoc.toString());

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      List<Node> results = kGraph.queryNodes("testType", queryClause);
      assertNotNull(results);
      logger.debug("results = " + results);
      assert (results.size() > 0) : "results.size() = " + results.size();
   }

   @Test
   public void queryNodes_multipleClauses_getResult() throws Exception {
      final Node testDoc = new Node(KnowledgeGraph.generateKey(), "testType");
      testDoc.addAttribute("foo", "bar");
      testDoc.addAttribute("foofoo", "barbar");
      kGraph.upsert(testDoc);
      Node addedDoc = kGraph.getNodeByKey(testDoc.getKey(), "testType");
      assertNotNull(addedDoc);
      logger.debug("addedDoc = " + addedDoc.toString());

      QueryClause queryClause1 = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      QueryClause queryClause2 = new QueryClause("foofoo", QueryClause.Operator.EQUALS, "barbar");
      List<Node> results = kGraph.queryNodes("testType", queryClause1, queryClause2);
      assertNotNull(results);
      logger.debug("results = " + results);
      assert (results.size() > 0) : "results.size() = " + results.size();
   }

   @Test(expected = ArangoDBException.class)
   public void queryNodes_nullType_exception() throws Exception {
      QueryClause queryClause1 = new QueryClause("foo", QueryClause.Operator.EQUALS, null);
      kGraph.queryNodes(null, queryClause1);
   }

   @Test
   public void expandRight_noFilters_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType");
      edge = kGraph.upsert(edge).getEdges().get(0);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, "testEdgeType", relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void queryEdge_edgeExists_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType2");
      edge.addAttribute("foo", "bar");
      edge = kGraph.upsert(edge).getEdges().get(0);
      logger.debug("edge.id: " + edge.getId());

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      List<Edge> results = kGraph.queryEdges("testEdgeType2", queryClause);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test(expected = NullPointerException.class)
   public void queryEdge_nullType_exception() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType2");
      edge.addAttribute("foo", "bar");
      edge = kGraph.upsert(edge).getEdges().get(0);
      logger.debug("edge.id: " + edge.getId());

      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      kGraph.queryEdges(null, queryClause);
   }

   @Test
   public void expandLeft_noFilters_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType1");
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), "testNodeType2");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType");
      edge = kGraph.upsert(edge).getEdges().get(0);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandLeft(rightNode, "testEdgeType", relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void expandRight_oneRelClause_getResults() {
      String testTypeName = KnowledgeGraph.generateName();
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node rightNode1 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node rightNode2 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node rightNode3 = new Node(KnowledgeGraph.generateKey(), testTypeName);

      kGraph.upsert(leftNode, rightNode1, rightNode2, rightNode3);

      String edgeTypeName = KnowledgeGraph.generateName();
      String edgeKey1 = leftNode.getKey() + ":" + rightNode1.getKey();
      String edgeKey2 = leftNode.getKey() + ":" + rightNode2.getKey();
      String edgeKey3 = leftNode.getKey() + ":" + rightNode3.getKey();

      Edge edge1 = new Edge(edgeKey1, leftNode, rightNode1, edgeTypeName);
      Edge edge2 = new Edge(edgeKey2, leftNode, rightNode2, edgeTypeName);
      Edge edge3 = new Edge(edgeKey3, leftNode, rightNode3, edgeTypeName);
      edge1.addAttribute("edgeVal", "good");
      edge3.addAttribute("edgeVal", "good");

      List<Element> edges = kGraph.upsert(edge1, edge2, edge3);
      assert (3 == edges.size()) : "edges.size() = " + edges.size();

      List<QueryClause> relClauses = new ArrayList<>();
      relClauses.add(new QueryClause("edgeVal", Operator.EQUALS, "good"));
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, edgeTypeName, relClauses, otherSideClauses);

      assert (2 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void expandLeft_oneOtherSideClause_getResults() {
      String testTypeName = KnowledgeGraph.generateName();
      final Node rightNode = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node leftNode1 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node leftNode2 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node leftNode3 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      leftNode1.addAttribute("nodeVal", "good");
      leftNode3.addAttribute("nodeVal", "good");

      kGraph.upsert(rightNode, leftNode1, leftNode2, leftNode3);

      String edgeCollectionName = KnowledgeGraph.generateName();
      String edgeKey1 = leftNode1.getKey() + ":" + rightNode.getKey();
      String edgeKey2 = leftNode2.getKey() + ":" + rightNode.getKey();
      String edgeKey3 = leftNode3.getKey() + ":" + rightNode.getKey();

      Edge edge1 = new Edge(edgeKey1, leftNode1, rightNode, edgeCollectionName);
      Edge edge2 = new Edge(edgeKey2, leftNode2, rightNode, edgeCollectionName);
      Edge edge3 = new Edge(edgeKey3, leftNode3, rightNode, edgeCollectionName);

      List<Element> edges = kGraph.upsert(edge1, edge2, edge3);
      assert (3 == edges.size()) : "edges.size() = " + edges.size();

      List<QueryClause> otherSideClauses = new ArrayList<>();
      otherSideClauses.add(new QueryClause("nodeVal", Operator.EQUALS, "good"));
      List<QueryClause> relClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandLeft(rightNode, edgeCollectionName, relClauses, otherSideClauses);

      assert (2 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void expandRight_multiRelMultiOtherClauses_getResults() {
      String testTypeName = KnowledgeGraph.generateName();
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node rightNode1 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node rightNode2 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      final Node rightNode3 = new Node(KnowledgeGraph.generateKey(), testTypeName);
      rightNode1.addAttribute("nodeVal1", "good");
      rightNode2.addAttribute("nodeVal1", "good");
      rightNode1.addAttribute("nodeVal2", "better");
      rightNode2.addAttribute("nodeVal2", "better");
      rightNode3.addAttribute("nodeVal2", "better");

      kGraph.upsert(leftNode, rightNode1, rightNode2, rightNode3);
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

      List<Element> edges = kGraph.upsert(edge1, edge2, edge3);
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

   @Test
   public void getCount_countIsOne_getAnswer() {
      String typeName = KnowledgeGraph.generateName();
      Node testNode = new Node(KnowledgeGraph.generateKey(), typeName);
      testNode.addAttribute("foo", "bbar");
      testNode = kGraph.upsert(testNode).getNodes().get(0);

      Long count = kGraph.getCount(typeName);
      assert (1 == count);
   }

   @Test
   public void expandRight_mixedOthersideTypes_getResults() {
      final Node leftNode = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      final Node rightNode1 = new Node(KnowledgeGraph.generateKey(), KnowledgeGraph.generateName());
      final Node rightNode2 = new Node(KnowledgeGraph.generateKey(), KnowledgeGraph.generateName());
      kGraph.upsert(leftNode, rightNode1, rightNode2);

      String edgeKey1 = leftNode.getKey() + ":" + rightNode1.getKey();
      String edgeKey2 = leftNode.getKey() + ":" + rightNode2.getKey();
      Edge edge1 = new Edge(edgeKey1, leftNode, rightNode1, "testEdgeType");
      Edge edge2 = new Edge(edgeKey2, leftNode, rightNode2, "testEdgeType");
      kGraph.upsert(edge1, edge2);

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, "testEdgeType", relClauses, otherSideClauses);

      assert (2 == results.size()) : "results.size() = " + results.size();
   }
   
   @Test
   public void getEdgeTypes_typesExist_getResults() {
      Node node1 = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      Node node2 = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      Node node3 = new Node(KnowledgeGraph.generateKey(), "testNodeType");
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = KnowledgeGraph.generateName();
      String edgeType2 = KnowledgeGraph.generateName();
      String edgeType3 = KnowledgeGraph.generateName();
      Edge edge1 = new Edge(KnowledgeGraph.generateKey(), node1, node2, edgeType1);
      Edge edge2 = new Edge(KnowledgeGraph.generateKey(), node1, node3, edgeType2);
      Edge edge3 = new Edge(KnowledgeGraph.generateKey(), node2, node3, edgeType3);
      kGraph.upsert(edge1, edge2, edge3);

      List<Node> edgeTypes = kGraph.getEdgeTypes();
      assert (edgeTypes.size() >= 3) : "edgeTypes = " + edgeTypes;
      String edge1Str = node1.getType() + ":" + edge1.getType() + ":" + node2.getType();
      logger.debug("edge1Str = " + edge1Str);
      String edge2Str = node1.getType() + ":" + edge2.getType() + ":" + node3.getType();
      logger.debug("edge2Str = " + edge2Str);
      String edge3Str = node2.getType() + ":" + edge3.getType() + ":" + node3.getType();
      logger.debug("edge1Str = " + edge3Str);

      List<String> edgeTypesIDs = edgeTypes.stream().map(object -> object.getKey()).collect(Collectors.toList());
      assert (edgeTypesIDs.contains(edge1Str)) : "edgeTypes = " + edgeTypes;
      assert (edgeTypesIDs.contains(edge1Str)) : "edgeTypes = " + edgeTypes;
      assert (edgeTypesIDs.contains(edge2Str)) : "edgeTypes = " + edgeTypes;
      assert (edgeTypesIDs.contains(edge3Str)) : "edgeTypes = " + edgeTypes;
   }

   @Test
   public void getEdgeTypesForLeftType_givenEdgeTypes_getResults() {
      String nodeType1 = KnowledgeGraph.generateName();
      String nodeType2 = KnowledgeGraph.generateName();
      String nodeType3 = KnowledgeGraph.generateName();
      Node node1 = new Node(KnowledgeGraph.generateKey(), nodeType1);
      Node node2 = new Node(KnowledgeGraph.generateKey(), nodeType2);
      Node node3 = new Node(KnowledgeGraph.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = KnowledgeGraph.generateName();
      String edgeType2 = KnowledgeGraph.generateName();
      Edge edge1 = new Edge(KnowledgeGraph.generateKey(), node1, node2, edgeType1);
      Edge edge2 = new Edge(KnowledgeGraph.generateKey(), node1, node3, edgeType1);
      Edge edge3 = new Edge(KnowledgeGraph.generateKey(), node1, node3, edgeType2);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getEdgeTypesForLeftType(nodeType1);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(edgeType1)) : "results = " + results;
      assert (results.contains(edgeType2)) : "results = " + results;
   }

   @Test
   public void getEdgeTypesForRightType_givenEdgeTypes_getResults() {
      String nodeType1 = KnowledgeGraph.generateName();
      String nodeType2 = KnowledgeGraph.generateName();
      String nodeType3 = KnowledgeGraph.generateName();
      Node node1 = new Node(KnowledgeGraph.generateKey(), nodeType1);
      Node node2 = new Node(KnowledgeGraph.generateKey(), nodeType2);
      Node node3 = new Node(KnowledgeGraph.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = KnowledgeGraph.generateName();
      String edgeType2 = KnowledgeGraph.generateName();
      Edge edge1 = new Edge(KnowledgeGraph.generateKey(), node1, node3, edgeType1);
      Edge edge2 = new Edge(KnowledgeGraph.generateKey(), node2, node3, edgeType1);
      Edge edge3 = new Edge(KnowledgeGraph.generateKey(), node3, node3, edgeType2);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getEdgeTypesForRightType(nodeType3);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(edgeType1)) : "results = " + results;
      assert (results.contains(edgeType2)) : "results = " + results;
   }

   @Test
   public void getLeftTypesForEdgeType_givenEdgeTypes_getResults() {
      String nodeType1 = KnowledgeGraph.generateName();
      String nodeType2 = KnowledgeGraph.generateName();
      String nodeType3 = KnowledgeGraph.generateName();
      Node node1 = new Node(KnowledgeGraph.generateKey(), nodeType1);
      Node node2 = new Node(KnowledgeGraph.generateKey(), nodeType2);
      Node node3 = new Node(KnowledgeGraph.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = KnowledgeGraph.generateName();
      Edge edge1 = new Edge(KnowledgeGraph.generateKey(), node1, node3, edgeType1);
      Edge edge2 = new Edge(KnowledgeGraph.generateKey(), node1, node2, edgeType1);
      Edge edge3 = new Edge(KnowledgeGraph.generateKey(), node2, node3, edgeType1);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getLeftTypesForEdgeType(edgeType1);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(nodeType1)) : "results = " + results;
      assert (results.contains(nodeType2)) : "results = " + results;
   }

   @Test
   public void getRightTypesforEdgeType_givenEdgeTypes_getResults() {
      String nodeType1 = KnowledgeGraph.generateName();
      String nodeType2 = KnowledgeGraph.generateName();
      String nodeType3 = KnowledgeGraph.generateName();
      Node node1 = new Node(KnowledgeGraph.generateKey(), nodeType1);
      Node node2 = new Node(KnowledgeGraph.generateKey(), nodeType2);
      Node node3 = new Node(KnowledgeGraph.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = KnowledgeGraph.generateName();
      Edge edge1 = new Edge(KnowledgeGraph.generateKey(), node1, node3, edgeType1);
      Edge edge2 = new Edge(KnowledgeGraph.generateKey(), node1, node2, edgeType1);
      Edge edge3 = new Edge(KnowledgeGraph.generateKey(), node2, node3, edgeType1);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getRightTypesforEdgeType(edgeType1);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(nodeType2)) : "results = " + results;
      assert (results.contains(nodeType3)) : "results = " + results;
   }
   
   

}
