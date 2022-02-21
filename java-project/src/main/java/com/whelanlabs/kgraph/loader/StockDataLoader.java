package com.whelanlabs.kgraph.loader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionPropertiesEntity;
import com.whelanlabs.kgraph.KnowledgeGraph;

public class StockDataLoader {

   private static final LocalDate epoch = LocalDate.ofEpochDay(0);

   private static Logger logger = LogManager.getLogger(StockDataLoader.class);

   public static void main(String[] args) throws Exception {
      load();
   }

public static void load() throws Exception, InterruptedException, ExecutionException {
	logger.info("loader starting");
      KnowledgeGraph kGraph = new KnowledgeGraph("andrew_graph_db");

      kGraph.createNodeCollection("node_types");
      final ArangoCollection node_types = kGraph.db.collection("node_types");
      kGraph.createNodeCollection("edge_types");
      final ArangoCollection edge_types = kGraph.db.collection("edge_types");

      BaseDocument v1 = new BaseDocument();
      v1.setKey("dates");
      v1 = kGraph.upsertNode(node_types, v1);

      BaseDocument v2 = new BaseDocument();
      v2.setKey("tickers");
      v2 = kGraph.upsertNode(node_types, v2);

      BaseDocument v3 = new BaseDocument();
      v3.setKey("marketData");
      v3.addAttribute("left_type", "dates");
      v3.addAttribute("right_type", "tickers");
      v3 = kGraph.upsertNode(edge_types, v3);

      kGraph.createNodeCollection("dates");
      final ArangoCollection dates = kGraph.db.collection("dates");

      kGraph.createNodeCollection("tickers");
      final ArangoCollection tickers = kGraph.db.collection("tickers");

      kGraph.createEdgeCollection("marketData");
      final ArangoCollection marketData = kGraph.db.collection("marketData");

      BufferedReader reader;
      try {
         String filename = "D:\\sandbox\\2022_andrew\\andrew\\andrew\\fetchers\\stock_data_fetcher\\data\\AA_2020-05-07.txt";

         BaseDocument ticker = new BaseDocument();
         ticker.setKey("AA");
         ticker = kGraph.upsertNode(tickers, ticker);

         logger.info("reading: " + filename);
         reader = new BufferedReader(new FileReader(filename));
         String line = reader.readLine();
         Boolean headerLine = true;
         while (line != null) {
            if (headerLine) {
               headerLine = false;
            } else {
               // System.out.println(line);
               String[] tokens = line.split(",");
               LocalDate date = LocalDate.parse(tokens[0]);

               BaseDocument stockDate = new BaseDocument();
               stockDate.setKey(date.toString());
               Long dayNumber = ChronoUnit.DAYS.between(epoch, date);
               // System.out.println("Days: " + dayNumber);
               stockDate.addAttribute("date", dayNumber);
               stockDate = kGraph.upsertNode(dates, stockDate);

               BaseEdgeDocument stockDay = new BaseEdgeDocument();
               stockDay.setFrom(stockDate.getId());
               stockDay.setTo(ticker.getId());
               stockDay.setKey(stockDate.getKey() + ":" + ticker.getKey());
               stockDay.addAttribute("open", tokens[1]);
               stockDay.addAttribute("high", tokens[2]);
               stockDay.addAttribute("low", tokens[3]);
               stockDay.addAttribute("close", tokens[4]);
               stockDay.addAttribute("adjClose", tokens[5]);
               stockDay.addAttribute("volume", tokens[6]);
               stockDay = kGraph.upsertEdge(marketData, stockDay);

            }

            // read next line
            line = reader.readLine();
         }
         reader.close();

         final BaseDocument badDate = new BaseDocument();
         badDate.setKey(UUID.randomUUID().toString());
         kGraph.upsertNode(dates, badDate);

         // FOR doc IN collection COLLECT WITH COUNT INTO length RETURN length
         CollectionPropertiesEntity count = dates.count();
         logger.info("count: " + count.getCount());

      } catch (IOException e) {
         e.printStackTrace();
      } finally {
         kGraph.tearDown();
         logger.info("loader complete");
      }
}

}
