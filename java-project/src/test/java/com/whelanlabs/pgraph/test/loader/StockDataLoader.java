package com.whelanlabs.pgraph.test.loader;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.whelanlabs.pgraph.engine.Edge;
import com.whelanlabs.pgraph.engine.PropertyGraph;
import com.whelanlabs.pgraph.engine.Node;

public class StockDataLoader {

   private static final LocalDate epoch = LocalDate.ofEpochDay(0);
   private static String default_tablespace_name = "stock_data_graph_db";
   private static Logger logger = LogManager.getLogger(StockDataLoader.class);

   public static void main(String[] args) throws Exception {
      load(default_tablespace_name);
   }

   public static void load(String tablespace_name) throws Exception, InterruptedException, ExecutionException {
      logger.info("loader starting");
      PropertyGraph pGraph = new PropertyGraph(tablespace_name);

      String node_types_name = "node_types";
      String edge_types_name = "edge_types";

      Node v1 = new Node("dates", edge_types_name);
      v1.setKey("dates");
      v1 = pGraph.upsert(v1).getNodes().get(0);

      Node v2 = new Node("tickers", edge_types_name);
      v2 = pGraph.upsert(v2).getNodes().get(0);

      Node v3 = new Node("marketData", node_types_name);
      v3.addAttribute("left_type", "dates");
      v3.addAttribute("right_type", "tickers");
      v3 = pGraph.upsert(v3).getNodes().get(0);

      String MarketDataEdgeCollectionName = "marketData";

      BufferedReader reader;
      try {
         String filename = "./src/test/resources/AA_2020-05-07.txt";

         Node ticker = new Node("AA", "tickers");
         ticker = pGraph.upsert(ticker).getNodes().get(0);

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
               stockDate = pGraph.upsert(stockDate).getNodes().get(0);

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
               stockDay = pGraph.upsert(stockDay).getEdges().get(0);

            }

            // read next line
            line = reader.readLine();
         }
         reader.close();

         final Node badDate = new Node(UUID.randomUUID().toString(), "dates");
         pGraph.upsert(badDate);

         logger.debug("count: " + pGraph.getCount("dates"));

      } catch (IOException e) {
         e.printStackTrace();
      } finally {
         pGraph.cleanup();
         logger.info("loader complete");
      }
   }

}
