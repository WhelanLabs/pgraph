package com.whelanlabs.kgraph;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.whelanlabs.kgraph.loader.StockDataLoader;

public class StockDataLoaderTest {

   private static KnowledgeGraph kGraph = null;
   private static String tablespace_name = "stock_data_graph_db";

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
      KnowledgeGraph.removeTablespace(tablespace_name);
      kGraph = new KnowledgeGraph(tablespace_name);
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
   }

   @Test
   public void load_freshAndValid_collectionsExist() throws Exception {
      Integer beginSize = kGraph.getCollections().size();
      assert (0 == beginSize);
      StockDataLoader.load(tablespace_name);
      Integer endSize = kGraph.getCollections().size();
      assert (0 < endSize);
   }

}
