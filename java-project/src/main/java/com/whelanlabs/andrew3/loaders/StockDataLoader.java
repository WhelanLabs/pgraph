package com.whelanlabs.andrew3.loaders;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.time.LocalDate;
import java.time.temporal.ChronoUnit;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionPropertiesEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;

public class StockDataLoader {

   private static final String ANDREW_DB = "andrew_graph_db";
   private static ArangoDB arangoDB;
   private static ArangoDatabase db;

   private static final LocalDate epoch = LocalDate.ofEpochDay(0);

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

      createCollection("node_types");
      final ArangoCollection node_types = db.collection("node_types");
      createCollection("edge_types");
      final ArangoCollection edge_types = db.collection("edge_types");

      final BaseDocument v1 = new BaseDocument();
      v1.setKey("dates");
      node_types.insertDocument(v1);

      final BaseDocument v2 = new BaseDocument();
      v2.setKey("tickers");
      node_types.insertDocument(v2);

      final BaseDocument v3 = new BaseDocument();
      v3.setKey("marketData");
      v3.addAttribute("left_type", "dates");
      v3.addAttribute("right_type", "tickers");
      edge_types.insertDocument(v3);

      createCollection("dates");
      final ArangoCollection dates = db.collection("dates");

      createCollection("tickers");
      final ArangoCollection tickers = db.collection("tickers");

      createEdgeCollection("marketData");
      final ArangoCollection marketData = db.collection("marketData");

      BufferedReader reader;
      try {
         String filename = "D:\\sandbox\\2022_andrew\\andrew\\andrew\\fetchers\\stock_data_fetcher\\data\\AA_2020-05-07.txt";

         final BaseDocument ticker = new BaseDocument();
         ticker.setKey("AA");
         tickers.insertDocument(ticker);

         System.out.println("reading: " + filename);
         reader = new BufferedReader(new FileReader(filename));
         String line = reader.readLine();
         Boolean headerLine = true;
         while (line != null) {
            if (headerLine) {
               headerLine = false;
            } else {
               System.out.println(line);
               String[] tokens = line.split(",");
               LocalDate date = LocalDate.parse(tokens[0]);

               final BaseDocument stockDate = new BaseDocument();
               stockDate.setKey(date.toString());
               Long dayNumber = ChronoUnit.DAYS.between(epoch, date);
               System.out.println("Days: " + dayNumber);
               stockDate.addAttribute("date", dayNumber);
               dates.insertDocument(stockDate);

               // TODO: add marketData (Open,High,Low,Close,Adj Close,Volume)
               BaseEdgeDocument stockDay = new BaseEdgeDocument();
               stockDay.setFrom(stockDate.getId());
               stockDay.setTo(ticker.getId());
               stockDay.addAttribute("open", tokens[1]);
               stockDay.addAttribute("high", tokens[2]);
               stockDay.addAttribute("low", tokens[3]);
               stockDay.addAttribute("close", tokens[4]);
               stockDay.addAttribute("adjClose", tokens[5]);
               stockDay.addAttribute("volume", tokens[6]);
               marketData.insertDocument(stockDay);

            }

            // read next line
            line = reader.readLine();
         }
         reader.close();

         final BaseDocument badDate = new BaseDocument();
         badDate.setKey(UUID.randomUUID().toString());
         dates.insertDocument(badDate);

         // FOR doc IN collection COLLECT WITH COUNT INTO length RETURN length
         CollectionPropertiesEntity count = dates.count();
         System.out.println("count: " + count.getCount());

      } catch (IOException e) {
         e.printStackTrace();
      } finally {
         tearDown();
      }

   }

   private static void createEdgeCollection(String collectionType) {

      db.createCollection(collectionType, new CollectionCreateOptions().type(CollectionType.EDGES));

   }

   private static void createCollection(String collectionType) {
      db.createCollection(collectionType);
   }

}
