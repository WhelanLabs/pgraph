package com.whelanlabs.kgraph.test.engine;

import static org.junit.Assert.assertNotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
import com.whelanlabs.kgraph.engine.ElementHelper;
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
      String typeName = ElementHelper.generateName();
      Node testNode = new Node(ElementHelper.generateKey(), typeName);
      testNode.addAttribute("foo", "bbar");
      testNode = kGraph.upsert(testNode).getNodes().get(0);

      Node result = kGraph.getNodeByKey(testNode.getKey(), typeName);
      String attr = (String) result.getAttribute("foo");
      assert ("bbar".equals(attr));
   }

   @Test(expected = RuntimeException.class)
   public void getNodeByKey_nodeDoesNotExist_exception() {
      String typeName = ElementHelper.generateName();
      Node testNode = new Node(ElementHelper.generateKey(), typeName);
      kGraph.upsert(testNode);
      Node result = kGraph.getNodeByKey(ElementHelper.generateKey(), typeName);
      logger.debug("result = " + result);
   }

   @Test
   public void upsertNode_existingNode_added() {
      final Node badDate = new Node(ElementHelper.generateKey(), "dates");
      kGraph.upsert(badDate);
      badDate.addAttribute("foo", "bar");
      kGraph.upsert(badDate);
      Node result = kGraph.getNodeByKey(badDate.getKey(), "dates");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_newEdge_added() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
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
   public void upsertEdge_nonNodeArgs_newEdge_added() {

      String leftID = ElementHelper.generateKey();
      String rightID = ElementHelper.generateKey();

      final Node leftNode = new Node(leftID, "testNodeType");
      final Node rightNode = new Node(rightID, "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftID, rightID, "testNodeType", "testNodeType", "testEdgeType");
      edge.addAttribute("foo", "bar");

      edge.setKey(edgeKey);
      edge = kGraph.upsert(edge).getEdges().get(0);

      Edge result = kGraph.getEdgeByKey(edgeKey, "testEdgeType");
      String attr = (String) result.getAttribute("foo");
      assert ("bar".equals(attr));
   }

   @Test
   public void upsertEdge_existingEdge_added() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
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
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, null);
      edge.addAttribute("foo", "bar");

      edge.setKey(edgeKey);
      edge = kGraph.upsert(edge).getEdges().get(0);
   }

   @Test(expected = RuntimeException.class)
   public void upsertEdge_edgeIsNull_exception() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      Edge edge = null;
      kGraph.upsert(edge);
   }

   @Test
   public void upsertEdge_keyIsNull_keyAutoPopulated() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "someEdgeType");
      edge.addAttribute("foo", "bar");

      edge.setKey(null);
      edge = kGraph.upsert(edge).getEdges().get(0);
      
      assert (null != edge.getKey());

      logger.debug("edge = " + edge);
   }

   @Test
   public void upsertNode_freshAndValid_typesExist() throws Exception {
      String testTypeName = ElementHelper.generateName();
      kGraph.flush();
      Long beginSize = kGraph.getTotalCount();
      assert (beginSize == 0) : "beginSize: " + beginSize;

      final Node badDate = new Node(ElementHelper.generateKey(), testTypeName);
      badDate.addAttribute("foo", "bbar");
      kGraph.upsert(badDate);

      Long endSize = kGraph.getTotalCount();
      // the "+1" below shows that the new node is added, but does not show the new
      // schema node.
      assert (endSize == beginSize + 1) : "{beginSize, endsize} is {" + beginSize + ", " + endSize + "}";
   }

   @Test
   public void getTotalCount_hasNodesAndEdges_getProperCount() throws Exception {
      String testTypeName = ElementHelper.generateName();
      kGraph.flush();
      Long beginSize = kGraph.getTotalCount();
      assert (beginSize == 0) : "beginSize: " + beginSize;

      final Node n1 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node n2 = new Node(ElementHelper.generateKey(), testTypeName);
      kGraph.upsert(n1, n2);
      Edge e1 = new Edge(ElementHelper.generateKey(), n1, n2, ElementHelper.generateName());
      kGraph.upsert(e1);

      Long endSize = kGraph.getTotalCount();
      assert (endSize == beginSize + 3) : "{beginSize, endsize} is {" + beginSize + ", " + endSize + "}";
   }

   @Test
   public void getTotalCount_nonpersistedNodesForNewEdge_success() throws Exception {
      // TODO: Address "Periodic errors may occur if using non-persisted nodes" issue.
      // see also: getTotalCount_hasNodesAndEdges_getProperCount
      String testTypeName = ElementHelper.generateName();
      kGraph.flush();
      Long beginSize = kGraph.getTotalCount();
      assert (beginSize == 0) : "beginSize: " + beginSize;

      final Node n1 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node n2 = new Node(ElementHelper.generateKey(), testTypeName);

      Edge e1 = new Edge(ElementHelper.generateKey(), n1, n2, ElementHelper.generateName());
      kGraph.upsert(e1, n1, n2);

      Long endSize = kGraph.getTotalCount();
      assert (endSize == beginSize + 3) : "{beginSize, endsize} is {" + beginSize + ", " + endSize + "}";
   }

   @Test
   public void queryNodes_singleClause_getResult() throws Exception {
      final Node testDoc = new Node(ElementHelper.generateKey(), "testType");
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
      final Node testDoc = new Node(ElementHelper.generateKey(), "testType");
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
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      logger.debug("leftNode = " + leftNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType");

      logger.debug("edge = " + edge);
      edge = kGraph.upsert(edge).getEdges().get(0);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, "testEdgeType", relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void expandRight_noNodeEdgeCreation_noFilters_getResults() {
      String leftId = ElementHelper.generateKey();
      String rightId = ElementHelper.generateKey();
      final Node leftNode = new Node(leftId, "testNodeType");
      final Node rightNode = new Node(rightId, "testNodeType");
      kGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftId, rightId, "testNodeType", "testNodeType", "testEdgeType");
      edge = kGraph.upsert(edge).getEdges().get(0);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;
      List<Triple<Node, Edge, Node>> results = kGraph.expandRight(leftNode, "testEdgeType", relClauses, otherSideClauses);

      assert (1 == results.size()) : "results.size() = " + results.size();
   }

   @Test
   public void queryEdge_edgeExists_getResults() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
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
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
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
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType1");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType2");
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
      String testTypeName = ElementHelper.generateName();
      final Node leftNode = new Node(ElementHelper.generateKey(), testTypeName);
      final Node rightNode1 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node rightNode2 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node rightNode3 = new Node(ElementHelper.generateKey(), testTypeName);

      kGraph.upsert(leftNode, rightNode1, rightNode2, rightNode3);

      String edgeTypeName = ElementHelper.generateName();
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
      String testTypeName = ElementHelper.generateName();
      final Node rightNode = new Node(ElementHelper.generateKey(), testTypeName);
      final Node leftNode1 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node leftNode2 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node leftNode3 = new Node(ElementHelper.generateKey(), testTypeName);
      leftNode1.addAttribute("nodeVal", "good");
      leftNode3.addAttribute("nodeVal", "good");

      kGraph.upsert(rightNode, leftNode1, leftNode2, leftNode3);

      String edgeCollectionName = ElementHelper.generateName();
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
      String testTypeName = ElementHelper.generateName();
      final Node leftNode = new Node(ElementHelper.generateKey(), testTypeName);
      final Node rightNode1 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node rightNode2 = new Node(ElementHelper.generateKey(), testTypeName);
      final Node rightNode3 = new Node(ElementHelper.generateKey(), testTypeName);
      rightNode1.addAttribute("nodeVal1", "good");
      rightNode2.addAttribute("nodeVal1", "good");
      rightNode1.addAttribute("nodeVal2", "better");
      rightNode2.addAttribute("nodeVal2", "better");
      rightNode3.addAttribute("nodeVal2", "better");

      kGraph.upsert(leftNode, rightNode1, rightNode2, rightNode3);
      String rn1id = rightNode1.getId();

      String edgeCollectionName = ElementHelper.generateName();
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
      String typeName = ElementHelper.generateName();
      Node testNode = new Node(ElementHelper.generateKey(), typeName);
      testNode.addAttribute("foo", "bbar");
      testNode = kGraph.upsert(testNode).getNodes().get(0);

      Long count = kGraph.getCount(typeName);
      assert (1 == count);
   }

   @Test
   public void getCount_typeDoesNoteExist_getZero() {
      String typeName = ElementHelper.generateName();
      ;
      Long count = kGraph.getCount(typeName);
      assert (0 == count);
   }

   @Test(expected = NullPointerException.class)
   public void getCount_typeIsNull_getZero() {
      Long count = kGraph.getCount(null);
      assert (0 == count);
   }

   @Test
   public void expandRight_mixedOthersideTypes_getResults() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode1 = new Node(ElementHelper.generateKey(), ElementHelper.generateName());
      final Node rightNode2 = new Node(ElementHelper.generateKey(), ElementHelper.generateName());
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
      Node node1 = new Node(ElementHelper.generateKey(), "testNodeType");
      Node node2 = new Node(ElementHelper.generateKey(), "testNodeType");
      Node node3 = new Node(ElementHelper.generateKey(), "testNodeType");
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = ElementHelper.generateName();
      String edgeType2 = ElementHelper.generateName();
      String edgeType3 = ElementHelper.generateName();
      Edge edge1 = new Edge(ElementHelper.generateKey(), node1, node2, edgeType1);
      Edge edge2 = new Edge(ElementHelper.generateKey(), node1, node3, edgeType2);
      Edge edge3 = new Edge(ElementHelper.generateKey(), node2, node3, edgeType3);
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
      String nodeType1 = ElementHelper.generateName();
      String nodeType2 = ElementHelper.generateName();
      String nodeType3 = ElementHelper.generateName();
      Node node1 = new Node(ElementHelper.generateKey(), nodeType1);
      Node node2 = new Node(ElementHelper.generateKey(), nodeType2);
      Node node3 = new Node(ElementHelper.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = ElementHelper.generateName();
      String edgeType2 = ElementHelper.generateName();
      Edge edge1 = new Edge(ElementHelper.generateKey(), node1, node2, edgeType1);
      Edge edge2 = new Edge(ElementHelper.generateKey(), node1, node3, edgeType1);
      Edge edge3 = new Edge(ElementHelper.generateKey(), node1, node3, edgeType2);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getEdgeTypesForLeftType(nodeType1);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(edgeType1)) : "results = " + results;
      assert (results.contains(edgeType2)) : "results = " + results;
   }

   @Test
   public void getEdgeTypesForRightType_givenEdgeTypes_getResults() {
      String nodeType1 = ElementHelper.generateName();
      String nodeType2 = ElementHelper.generateName();
      String nodeType3 = ElementHelper.generateName();
      Node node1 = new Node(ElementHelper.generateKey(), nodeType1);
      Node node2 = new Node(ElementHelper.generateKey(), nodeType2);
      Node node3 = new Node(ElementHelper.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = ElementHelper.generateName();
      String edgeType2 = ElementHelper.generateName();
      Edge edge1 = new Edge(ElementHelper.generateKey(), node1, node3, edgeType1);
      Edge edge2 = new Edge(ElementHelper.generateKey(), node2, node3, edgeType1);
      Edge edge3 = new Edge(ElementHelper.generateKey(), node3, node3, edgeType2);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getEdgeTypesForRightType(nodeType3);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(edgeType1)) : "results = " + results;
      assert (results.contains(edgeType2)) : "results = " + results;
   }

   @Test
   public void getLeftTypesForEdgeType_givenEdgeTypes_getResults() {
      String nodeType1 = ElementHelper.generateName();
      String nodeType2 = ElementHelper.generateName();
      String nodeType3 = ElementHelper.generateName();
      Node node1 = new Node(ElementHelper.generateKey(), nodeType1);
      Node node2 = new Node(ElementHelper.generateKey(), nodeType2);
      Node node3 = new Node(ElementHelper.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = ElementHelper.generateName();
      Edge edge1 = new Edge(ElementHelper.generateKey(), node1, node3, edgeType1);
      Edge edge2 = new Edge(ElementHelper.generateKey(), node1, node2, edgeType1);
      Edge edge3 = new Edge(ElementHelper.generateKey(), node2, node3, edgeType1);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getLeftTypesForEdgeType(edgeType1);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(nodeType1)) : "results = " + results;
      assert (results.contains(nodeType2)) : "results = " + results;
   }

   @Test
   public void getRightTypesforEdgeType_givenEdgeTypes_getResults() {
      String nodeType1 = ElementHelper.generateName();
      String nodeType2 = ElementHelper.generateName();
      String nodeType3 = ElementHelper.generateName();
      Node node1 = new Node(ElementHelper.generateKey(), nodeType1);
      Node node2 = new Node(ElementHelper.generateKey(), nodeType2);
      Node node3 = new Node(ElementHelper.generateKey(), nodeType3);
      kGraph.upsert(node1, node2, node3);

      String edgeType1 = ElementHelper.generateName();
      Edge edge1 = new Edge(ElementHelper.generateKey(), node1, node3, edgeType1);
      Edge edge2 = new Edge(ElementHelper.generateKey(), node1, node2, edgeType1);
      Edge edge3 = new Edge(ElementHelper.generateKey(), node2, node3, edgeType1);
      kGraph.upsert(edge1, edge2, edge3);

      List<String> results = kGraph.getRightTypesforEdgeType(edgeType1);

      assert (results.size() == 2) : "results = " + results;
      assert (results.contains(nodeType2)) : "results = " + results;
      assert (results.contains(nodeType3)) : "results = " + results;
   }

   @Test
   public void queryNodes_typeDoesNotExist_getZeroResults() throws Exception {
      QueryClause linearDatasetInfoQuery = new QueryClause("dataset_id", QueryClause.Operator.EQUALS, "datasetInfoID");
      String nodeType = ElementHelper.generateName();
      List<Node> results = kGraph.queryNodes(nodeType, linearDatasetInfoQuery);
      assert (results.size() == 0) : "results = " + results;
   }

   @Test
   public void queryEdges_typeDoesNotExist_getZeroResults() throws Exception {
      QueryClause linearDatasetInfoQuery = new QueryClause("dataset_id", QueryClause.Operator.EQUALS, "datasetInfoID");
      String edgeType = ElementHelper.generateName();
      List<Edge> results = kGraph.queryEdges(edgeType, linearDatasetInfoQuery);
      assert (results.size() == 0) : "results = " + results;
   }

   @Test
   public void queryNodes_hasResults_getResults() throws Exception {
      String nodeType = ElementHelper.generateName();
      Node node1 = new Node(ElementHelper.generateKey(), nodeType);
      node1.addAttribute("time", 1);
      Node node2 = new Node(ElementHelper.generateKey(), nodeType);
      node2.addAttribute("time", 2);
      Node node3 = new Node(ElementHelper.generateKey(), nodeType);
      node3.addAttribute("time", 3);
      kGraph.upsert(node1, node2, node3);

      String query = "FOR t IN " + nodeType + " FILTER t.time <= @time SORT t.time DESC LIMIT 1 RETURN t";
      logger.debug("query: " + query);
      Map<String, Object> bindVars = Collections.singletonMap("time", 2);

      List<Node> results = kGraph.queryNodes(query, bindVars);
      assert (results.size() == 1) : "results = " + results;
      assert (results.get(0).getKey().equals(node2.getKey())) : "results = " + results;
   }

   @Test
   public void queryEdges_hasResults_getResults() throws Exception {
      String nodeType = ElementHelper.generateName();
      String edgeType = ElementHelper.generateName();

      Node node1 = new Node(ElementHelper.generateKey(), nodeType);
      node1.addAttribute("time", 1);
      Node node2 = new Node(ElementHelper.generateKey(), nodeType);
      node2.addAttribute("time", 2);
      Edge edge1 = new Edge(ElementHelper.generateKey(), node1, node2, edgeType);
      kGraph.upsert(node1, node2, edge1);

      String query = "FOR t IN " + edgeType + " FILTER t._from == @left AND t._to == @to RETURN t";
      logger.debug("query: " + query);
      Map<String, Object> bindVars = new HashMap<>();
      bindVars.put("left", node1.getId());
      bindVars.put("to", node2.getId());

      List<Edge> results = kGraph.queryEdges(query, bindVars);
      assert (results.size() == 1) : "results = " + results;
      assert (results.get(0).getKey().equals(edge1.getKey())) : "results = " + results;
   }
   @Test
   public void delete_nodeExists_deleted() {
      Boolean pass = false;
      String typeName = ElementHelper.generateName();
      Node testNode = new Node(ElementHelper.generateKey(), typeName);
      testNode = kGraph.upsert(testNode).getNodes().get(0);

      Node result = kGraph.getNodeByKey(testNode.getKey(), typeName);
      assert (null != result);

      kGraph.delete(result);
      
      try {
         kGraph.getNodeByKey(testNode.getKey(), typeName);
      } catch (RuntimeException e) {
         pass = true;
      }
      assert (pass == true);
   }

   @Test(expected = RuntimeException.class)
   public void delete_nodeNotExists_exception() {
      String typeName = ElementHelper.generateName();
      Node testNode = new Node(ElementHelper.generateKey(), typeName);

      kGraph.delete(testNode);
      kGraph.getNodeByKey(testNode.getKey(), typeName);
   }
}
