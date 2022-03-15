package com.whelanlabs.kgraph;

import java.util.ArrayList;
import java.util.Collection;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;

public class KnowledgeGraph {

   // private ArangoDB arangoDB;
   public ArangoDatabase _userDB;
   private DbName _db_name;
   private ArangoDB _systemDB = null;

   private static Logger logger = LogManager.getLogger(KnowledgeGraph.class);

   public KnowledgeGraph(String db_name) throws Exception {
      _db_name = DbName.normalize(db_name);
      setUp(_db_name);
   }

   private void setUp(DbName db_name) throws InterruptedException, ExecutionException {
      setSystemDB();
      if (!_systemDB.db(db_name).exists()) {
         _systemDB.createDatabase(db_name);
      }
      _userDB = _systemDB.db(db_name);
   }

   private synchronized ArangoDB setSystemDB() {
      if (null == _systemDB) {
         _systemDB = new ArangoDB.Builder().user("root").password("openSesame").serializer(new ArangoJack()).build();
      }
      return _systemDB;
   }

   public void tearDown() throws InterruptedException, ExecutionException {
      // db.drop();
      _systemDB.shutdown();
      _systemDB = null;
   }

   public BaseDocument upsertNode(final ArangoCollection collection, final BaseDocument element) {
      logger.debug("upsertNode " + element.getKey());
      try {
         if (!collection.documentExists(element.getKey())) {
            collection.insertDocument(element);
         } else {
            logger.debug("Fetch already existing element. (key=" + element.getKey() + ")");
            collection.updateDocument(element.getKey(), element);
         }
      } catch (Exception e) {
         logger.error(element.toString());
         throw e;
      }
      return element;
   }

   public ArrayList<BaseDocument> upsertNode(final ArangoCollection collection, final BaseDocument... elements) {
      // TODO: address ACID requirements
      ArrayList<BaseDocument> results = new ArrayList<BaseDocument>();
      for (BaseDocument element : elements) {
         results.add(upsertNode(collection, element));
      }
      return results;
   }

   public BaseEdgeDocument upsertEdge(final ArangoCollection collection, final BaseEdgeDocument element) {
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

   public ArangoCollection getEdgeCollection(String collectionName) {
      CollectionEntity result = null;
      ArangoCollection collection = _userDB.collection(collectionName);
      if (!collection.exists()) {
         result = _userDB.createCollection(collectionName, new CollectionCreateOptions().type(CollectionType.EDGES));
      } else {
         result = collection.getInfo();
         logger.debug("createEdgeCollection - result.getType() = " + result.getType());
         if (!"EDGES".equals(result.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + result.getType() + ")");
         }
      }
      return collection;
   }

   public CollectionEntity createNodeCollection(String collectionName) {
      CollectionEntity result = null;
      ArangoCollection collection = _userDB.collection(collectionName);
      if (!collection.exists()) {
         result = _userDB.createCollection(collectionName);
      } else {
         result = collection.getInfo();
         if (!"DOCUMENT".equals(result.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + result.getType() + ")");
         }
      }
      return result;
   }

   public BaseDocument getNodeByKey(String key, String type) {
      BaseDocument doc = _userDB.collection(type).getDocument(key, BaseDocument.class);
      return doc;
   }

   public Collection<CollectionEntity> getCollections() {
      return _systemDB.db(_db_name).getCollections();
   }

   public void flush() throws InterruptedException, ExecutionException {
      if (_systemDB.db(_db_name).exists()) {

         Boolean isDropped = _userDB.drop();
         System.out.println("\nIs the database " + _db_name + " dropped : " + isDropped);

         logger.info("database " + _db_name + " is dropped = " + isDropped);
      }
      setUp(_db_name);
   }

   public Long getCollectionSize(String type) {
      Long count = 0l;
      try {
         count = _userDB.collection(type).count().getCount();
      } catch (ArangoDBException e) {
         // ignore
      }
      return count;
   }

   public Long getTotalCount() {
      Long result = 0l;
      Collection<CollectionEntity> collections = _systemDB.db(_db_name).getCollections();
      for (CollectionEntity collectionEntity : collections) {
         result += _userDB.collection(collectionEntity.getName()).count().getCount();
      }

      return result;
   }
}
