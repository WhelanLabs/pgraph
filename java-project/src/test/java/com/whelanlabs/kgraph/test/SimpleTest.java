package com.whelanlabs.kgraph.test;

import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.whelanlabs.kgraph.engine.KnowledgeGraph;
import com.whelanlabs.kgraph.engine.Node;
import com.whelanlabs.kgraph.engine.QueryClause;
public class SimpleTest {

   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "SimpleTests_db";
   private static Logger logger = LogManager.getLogger(SimpleTest.class);

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

   @Test
   public void queryNodes_singleClause_getResult() throws Exception {
      
      String collectionName = kGraph.generateName();
      final ArangoCollection testCollection = kGraph.getNodeCollection(collectionName);
      final Node testDoc1 = new Node(kGraph.generateKey());
      testDoc1.addAttribute("foo", "bar");
      testDoc1.addAttribute("xname", "Homer");
      kGraph.upsertNode(testCollection, testDoc1);

      final Node testDoc2 = new Node(kGraph.generateKey());
      testDoc2.addAttribute("foo", "not-bar");
      testDoc2.addAttribute("xname", "Homer");
      kGraph.upsertNode(testCollection, testDoc2);
      
      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
      List<Node> results = kGraph.queryNodes(testCollection, queryClause);
      assertNotNull(results);
      logger.debug("results = " + results);
      assert (results.size() == 1) : "results.size() = " + results.size();
   }

//   @Test
//   public void queryBaseDocument_singleClause_getResult() throws Exception {
//      
//      String collectionName = kGraph.generateName();
//      final ArangoCollection testCollection = kGraph.getNodeCollection(collectionName);
//      final BaseDocument testDoc = new BaseDocument(kGraph.generateKey());
//      testDoc.addAttribute("foo", "bar");
//      testDoc.addAttribute("xname", "Homer");
//      kGraph.upsertNode(testCollection, testDoc);
//
//      QueryClause queryClause = new QueryClause("foo", QueryClause.Operator.EQUALS, "bar");
//      List<BaseDocument> results = kGraph.queryBaseDocument(testCollection, queryClause);
//      assertNotNull(results);
//      logger.debug("results = " + results);
//      assert (results.size() == 1) : "results.size() = " + results.size();
//   }
}
