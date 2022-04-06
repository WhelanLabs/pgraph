package com.whelanlabs.kgraph.test.loader;

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
import com.arangodb.entity.CollectionPropertiesEntity;
import com.whelanlabs.kgraph.engine.Edge;
import com.whelanlabs.kgraph.engine.KnowledgeGraph;
import com.whelanlabs.kgraph.engine.Node;

public class StockDataLoader {

   private static final LocalDate epoch = LocalDate.ofEpochDay(0);
   private static String default_tablespace_name = "stock_data_graph_db";
   private static Logger logger = LogManager.getLogger(StockDataLoader.class);

   public static void main(String[] args) throws Exception {
      load(default_tablespace_name);
   }

   public static void load(String tablespace_name) throws Exception, InterruptedException, ExecutionException {
      logger.info("loader starting");
      KnowledgeGraph kGraph = new KnowledgeGraph(tablespace_name);

      String node_types_name = "node_types";
      String edge_types_name = "edge_types";

      Node v1 = new Node("dates", edge_types_name);
      v1.setKey("dates");
      v1 = kGraph.upsert(v1);

      Node v2 = new Node("tickers", edge_types_name);
      v2 = kGraph.upsert(v2);

      Node v3 = new Node("marketData", node_types_name);
      v3.addAttribute("left_type", "dates");
      v3.addAttribute("right_type", "tickers");
      v3 = kGraph.upsert(v3);

      String MarketDataEdgeCollectionName = "marketData";

      BufferedReader reader;
      try {
         String filename = "../fetchers/stock_data_fetcher/data/AA_2020-05-07.txt";

         Node ticker = new Node("AA", "tickers");
         ticker = kGraph.upsert(ticker);

         logger.debug("reading: " + filename);
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

               Node stockDate = new Node(date.toString(), "dates");
               Long dayNumber = ChronoUnit.DAYS.between(epoch, date);
               // System.out.println("Days: " + dayNumber);
               stockDate.addAttribute("date", dayNumber);
               stockDate = kGraph.upsert(stockDate);

               String edgeKey = stockDate.getKey() + ":" + ticker.getKey();
               Edge stockDay = new Edge(edgeKey, stockDate, ticker, MarketDataEdgeCollectionName);
               // stockDay.setFrom(stockDate.getId());
               // stockDay.setTo(ticker.getId());
               // stockDay.setKey(stockDate.getKey() + ":" + ticker.getKey());
               stockDay.addAttribute("open", tokens[1]);
               stockDay.addAttribute("high", tokens[2]);
               stockDay.addAttribute("low", tokens[3]);
               stockDay.addAttribute("close", tokens[4]);
               stockDay.addAttribute("adjClose", tokens[5]);
               stockDay.addAttribute("volume", tokens[6]);
               stockDay = kGraph.upsert(stockDay);

            }

            // read next line
            line = reader.readLine();
         }
         reader.close();

         final Node badDate = new Node(UUID.randomUUID().toString(), "dates");
         kGraph.upsert(badDate);

         logger.debug("count: " + kGraph.getCount("dates"));

      } catch (IOException e) {
         e.printStackTrace();
      } finally {
         kGraph.cleanup();
         logger.info("loader complete");
      }
   }

}
