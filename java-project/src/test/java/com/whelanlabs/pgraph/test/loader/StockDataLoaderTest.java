package com.whelanlabs.pgraph.test.loader;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.whelanlabs.pgraph.engine.PropertyGraph;

public class StockDataLoaderTest {

   private static PropertyGraph pGraph = null;
   private static String tablespace_name = "stock_data_graph_db";

   private static Logger logger = LogManager.getLogger(PropertyGraph.class);

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      pGraph = new PropertyGraph(tablespace_name);
      pGraph.flush();
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
      // do nothing
   }


   @Test
   public void test_class_not_empty() throws Exception {
      logger.debug("have a remaining test for cases where perf tests not run.");
      assert (true);
   }
}
