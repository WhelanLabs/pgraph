package com.whelanlabs.andrew3.loaders;

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
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionPropertiesEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;

public class StockDataLoader {

   private static final String ANDREW_DB = "andrew_graph_db";
   private static ArangoDB arangoDB;
   private static ArangoDatabase db;

   private static final LocalDate epoch = LocalDate.ofEpochDay(0);

   private static Logger logger = LogManager.getLogger(StockDataLoader.class);

   public static void setUp() throws InterruptedException, ExecutionException {
      arangoDB = new ArangoDB.Builder().user("root").password("openSesame").serializer(new ArangoJack()).build();

      if (arangoDB.db(ANDREW_DB).exists()) {
         // arangoDB.db(ANDREW_DB).drop();
      } else {
         arangoDB.createDatabase(ANDREW_DB);
      }
      db = arangoDB.db(ANDREW_DB);

   }

   public static void tearDown() throws InterruptedException, ExecutionException {
      // db.drop();
      // arangoDB.shutdown();
      // arangoDB = null;
   }

   public static void main(String[] args) throws InterruptedException, ExecutionException {
      setUp();

      createNodeCollection("node_types");
      final ArangoCollection node_types = db.collection("node_types");
      createNodeCollection("edge_types");
      final ArangoCollection edge_types = db.collection("edge_types");

      BaseDocument v1 = new BaseDocument();
      v1.setKey("dates");
      v1 = upsertNode(node_types, v1);

      BaseDocument v2 = new BaseDocument();
      v2.setKey("tickers");
      v2 = upsertNode(node_types, v2);

      BaseDocument v3 = new BaseDocument();
      v3.setKey("marketData");
      v3.addAttribute("left_type", "dates");
      v3.addAttribute("right_type", "tickers");
      v3 = upsertNode(edge_types, v3);

      createNodeCollection("dates");
      final ArangoCollection dates = db.collection("dates");

      createNodeCollection("tickers");
      final ArangoCollection tickers = db.collection("tickers");

      createEdgeCollection("marketData");
      final ArangoCollection marketData = db.collection("marketData");

      BufferedReader reader;
      try {
         String filename = "D:\\sandbox\\2022_andrew\\andrew\\andrew\\fetchers\\stock_data_fetcher\\data\\AA_2020-05-07.txt";

         BaseDocument ticker = new BaseDocument();
         ticker.setKey("AA");
         ticker = upsertNode(tickers, ticker);

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
               stockDate = upsertNode(dates, stockDate);

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
               stockDay = upsertEdge(marketData, stockDay);

            }

            // read next line
            line = reader.readLine();
         }
         reader.close();

         final BaseDocument badDate = new BaseDocument();
         badDate.setKey(UUID.randomUUID().toString());
         upsertNode(dates, badDate);

         // FOR doc IN collection COLLECT WITH COUNT INTO length RETURN length
         CollectionPropertiesEntity count = dates.count();
         logger.info("count: " + count.getCount());

      } catch (IOException e) {
         e.printStackTrace();
      } finally {
         tearDown();
      }

   }

   private static BaseDocument upsertNode(final ArangoCollection collection, final BaseDocument element) {
      BaseDocument result = null;
      try {
         if (!collection.documentExists(element.getKey())) {
            collection.insertDocument(element);
            result = element;
         } else {
            logger.debug("Fetch already existing element. (key=" + element.getKey() + ")");
            result = collection.getDocument(element.getKey(), BaseDocument.class);
         }
      } catch (Exception e) {
         logger.error(element.toString());
         throw e;
      }
      return result;
   }

   private static BaseEdgeDocument upsertEdge(final ArangoCollection collection, final BaseEdgeDocument element) {
      BaseEdgeDocument result = null;
      try {
         if (!collection.documentExists(element.getKey())) {
            collection.insertDocument(element);
            result = element;
         } else {
            logger.debug("Fetch already existing element. (key=" + element.getKey() + ")");
            result = collection.getDocument(element.getKey(), BaseEdgeDocument.class);
         }
      } catch (Exception e) {
         logger.error(element.toString());
         throw e;
      }
      return result;
   }

   private static CollectionEntity createEdgeCollection(String collectionName) {
      CollectionEntity result = null;
      ArangoCollection collection = db.collection(collectionName);
      if (!collection.exists()) {
         result = db.createCollection(collectionName, new CollectionCreateOptions().type(CollectionType.EDGES));
      } else {
         result = collection.getInfo();
         logger.debug("createEdgeCollection - result.getType() = " + result.getType());
         if (!"EDGES".equals(result.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + result.getType() + ")");
         }
      }
      return result;
   }

   private static CollectionEntity createNodeCollection(String collectionName) {
      CollectionEntity result = null;
      ArangoCollection collection = db.collection(collectionName);
      if (!collection.exists()) {
         result = db.createCollection(collectionName);
      } else {
         result = collection.getInfo();
         if (!"DOCUMENT".equals(result.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + result.getType() + ")");
         }
      }
      return result;
   }

}
