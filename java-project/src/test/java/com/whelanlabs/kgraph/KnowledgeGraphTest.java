package com.whelanlabs.kgraph;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.fail;

import java.util.UUID;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;

public class KnowledgeGraphTest {
   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "KnowledgeGraphTests_db";

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      KnowledgeGraph.removeTablespace(tablespace_name);
      kGraph = new KnowledgeGraph(tablespace_name);
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
   }

   @Test
   public void upsertNode_newNode_added() {
      kGraph.createNodeCollection("dates");
      final ArangoCollection dates = kGraph.db.collection("dates");
      final BaseDocument badDate = new BaseDocument();
      String key = UUID.randomUUID().toString();
      badDate.setKey(key);
      kGraph.upsertNode(dates, badDate);
      BaseDocument result = kGraph.getNodeByKey(key, "dates");
      assertNotNull(result);
      //fail("Not yet implemented");
   }

}
