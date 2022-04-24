package com.whelanlabs.kgraph.test.loader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.whelanlabs.kgraph.engine.KnowledgeGraph;

public class StockDataLoaderTest {

   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "stock_data_graph_db";

   private static Logger logger = LogManager.getLogger(KnowledgeGraph.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      kGraph = new KnowledgeGraph(tablespace_name);
      kGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      // do nothing
   }

//   @Test
//   public void load_freshAndValid_collectionsExist() throws Exception {
//      long startTime = System.currentTimeMillis();
//      Long beginSize = kGraph.getTotalCount();
//      assert (0 == beginSize) : "beginSize is " + beginSize;
//      StockDataLoader.load(tablespace_name);
//      Long endSize = kGraph.getTotalCount();
//      long endTime = System.currentTimeMillis();
//      assert (0 < endSize);
//      long timeElapsed = endTime - startTime;
//      logger.debug("Execution time in seconds: " + timeElapsed/1000);
//      logger.debug("Note: The average time on laptop is 177 seconds");
//
//      assert (timeElapsed < 200): "time elapsed >200. (" + timeElapsed + ")";
//   }

   @Test
   public void test_class_not_empty() throws Exception {
      logger.debug("have a remaining test for cases where perf tests not run.");
      assert (true);
   }
}
