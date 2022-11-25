package com.whelanlabs.pgraph.engine;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.model.TraversalOptions.Direction;
import com.whelanlabs.pgraph.engine.Edge;
import com.whelanlabs.pgraph.engine.Element;
import com.whelanlabs.pgraph.engine.ElementHelper;
import com.whelanlabs.pgraph.engine.ElementList;
import com.whelanlabs.pgraph.engine.PropertyGraph;
import com.whelanlabs.pgraph.engine.Node;
import com.whelanlabs.pgraph.engine.QueryClause;

public class PropertyGraphTest2 {
   private static PropertyGraph pGraph = null;
   private static String tablespace_name = "PropertyGraphTests_db";

   private static Logger logger = LogManager.getLogger(PropertyGraphTest2.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      pGraph = new PropertyGraph(tablespace_name);
      pGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      pGraph.cleanup();
   }

   @Test(expected = RuntimeException.class)
   public void getEdgeCollection_isNodeCollection_getException() {
      logger.debug("getEdgeCollection_isNodeCollection_getException");
      String collectionName = ElementHelper.generateName();
      Node testNode = new Node(ElementHelper.generateKey(), collectionName);
      testNode.addAttribute("foo", "bbar");
      testNode = pGraph.upsert(testNode).getNodes().get(0);
      pGraph.getEdgeCollection(collectionName);
   }

   @Test(expected = RuntimeException.class)
   public void getNodeCollection_isEdgeCollection_getException() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeCollection");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeCollection");
      pGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      String testEdgeCollection = ElementHelper.generateName();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, testEdgeCollection);
      pGraph.upsert(edge);

      pGraph.getNodeCollection(testEdgeCollection);
   }

   @Test
   public void getNodeTypes_typesExist_getResults() {
      String nodeType1 = ElementHelper.generateName();
      String nodeType2 = ElementHelper.generateName();
      String nodeType3 = ElementHelper.generateName();
      Node node1 = new Node(ElementHelper.generateKey(), nodeType1);
      Node node2 = new Node(ElementHelper.generateKey(), nodeType2);
      Node node3 = new Node(ElementHelper.generateKey(), nodeType3);
      pGraph.upsert(node1, node2, node3);

      List<String> nodeTypes = pGraph.getNodeTypes();
      assert (nodeTypes.size() >= 3) : "nodeTypes = " + nodeTypes;
      assert (nodeTypes.contains(nodeType1)) : "nodeTypes = " + nodeTypes;
      assert (nodeTypes.contains(nodeType2)) : "nodeTypes = " + nodeTypes;
      assert (nodeTypes.contains(nodeType3)) : "nodeTypes = " + nodeTypes;

      assert (!nodeTypes.contains(PropertyGraph.nodeTypesCollectionName)) : "nodeTypes = " + nodeTypes;
      assert (!nodeTypes.contains(PropertyGraph.edgeTypesCollectionName)) : "nodeTypes = " + nodeTypes;
   }

   @Test(expected = RuntimeException.class)
   public void expandBoth_noFilters_exception() {
      final Node leftNode = new Node(ElementHelper.generateKey(), "testNodeType");
      final Node rightNode = new Node(ElementHelper.generateKey(), "testNodeType");
      pGraph.upsert(leftNode, rightNode);

      String edgeKey = leftNode.getKey() + ":" + rightNode.getKey();
      Edge edge = new Edge(edgeKey, leftNode, rightNode, "testEdgeType");
      edge = pGraph.upsert(edge).getEdges().get(0);
      logger.debug("edge.id: " + edge.getId());

      List<QueryClause> relClauses = null;
      List<QueryClause> otherSideClauses = null;

      pGraph.expand(leftNode, "testEdgeType", relClauses, otherSideClauses, Direction.any);

   }

   @Test
   public void toJson_goodNode_getJson() throws Exception {
      Node node1 = new Node(ElementHelper.generateKey(), "TestNodeType");
      node1.addAttribute("foo", 123);
      node1.addAttribute("bar", "abc");
      String jsonString1 = node1.toJson();
      logger.debug("jsonString1: " + jsonString1);
      pGraph.upsert(node1);
      String jsonString2 = node1.toJson();
      logger.debug("jsonString2: " + jsonString2);
      assert (jsonString1.contains("\"bar\" : \"abc\","));
      assert (jsonString2.contains("\"type\" : \"TestNodeType\""));

   }

   @Test
   public void toJson_goodEdge_getJson() throws Exception {

      Node node1 = new Node(ElementHelper.generateKey(), "TestNodeType");
      Node node2 = new Node(ElementHelper.generateKey(), "TestNodeType");
      node1.addAttribute("foo", 123);
      node1.addAttribute("bar", "abc");
      Edge edge = new Edge(ElementHelper.generateKey(), node1, node2, "TestEdgeType");

      String jsonString1 = edge.toJson();
      logger.debug("jsonString1: " + jsonString1);
      assert (jsonString1.contains("_key\" : \"KEY_"));

      pGraph.upsert(node1, node2, edge);
      String jsonString2 = edge.toJson();
      logger.debug("jsonString2: " + jsonString2);
      assert (jsonString2.contains("\"type\" : \"TestEdgeType\""));

   }

   @Test
   public void toJson_goodElements_getJson() throws Exception {

      Node node1 = new Node(ElementHelper.generateKey(), "TestNodeType");
      Node node2 = new Node(ElementHelper.generateKey(), "TestNodeType");
      node1.addAttribute("foo", 123);
      node1.addAttribute("bar", "abc");
      Edge edge = new Edge(ElementHelper.generateKey(), node1, node2, "TestEdgeType");
      List<Element> elements = new ArrayList<>();
      elements.add(node1);
      elements.add(edge);
      elements.add(node2);

      String jsonString1 = Element.toJson(elements);
      logger.debug("jsonString1: " + jsonString1);
      pGraph.upsert(node1, node2, edge);
      String jsonString2 = Element.toJson(elements);
      logger.debug("jsonString2: " + jsonString2);

      assert (jsonString1.contains("\"_key\" : \"KEY_"));
      assert (jsonString2.contains("\"type\" : \"TestNodeType\""));
   }

   @Test
   public void toDot_goodElements_getDot() throws Exception {

      Node node1 = new Node(ElementHelper.generateKey(), "TestNodeType");
      Node node2 = new Node(ElementHelper.generateKey(), "TestNodeType");
      node1.addAttribute("foo", 123);
      node1.addAttribute("bar", "abc_node");
      Edge edge = new Edge(ElementHelper.generateKey(), node1, node2, "TestEdgeType");
      edge.addAttribute("foo", 123456);
      edge.addAttribute("bar", "abc_edge");
      List<Element> elements = new ArrayList<>();
      
      ElementList<Element> updated = pGraph.upsert(node1, node2, edge);
      elements.addAll(updated);
      
      String dotString = Element.toDot(elements);
      logger.debug("dotString: " + dotString);

      assert (dotString.contains("id = \\\"TestEdgeType/KEY"));
      assert (dotString.contains("digraph G {"));
   }

   @Test
   public void importJson_goodJson_loaded() throws Exception {
      String filePath = "./src/test/resources/test_load_data.json";
      String content = new String(Files.readAllBytes(Paths.get(filePath)));

      logger.debug("content = " + content);

      JSONArray jsonArr = new JSONArray(content);
      List<Element> results = pGraph.loadFromJson(jsonArr);

      logger.debug("results: " + results);

      assert (results.size() == 25) : "results size = " + results.size();

      ElementList<Element> testNodes = pGraph.upsert(results);

      assert (testNodes.size() == 25) : "testNodes size = " + testNodes.size();
   }
}
