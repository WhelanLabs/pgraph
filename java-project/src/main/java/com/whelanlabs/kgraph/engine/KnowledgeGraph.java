package com.whelanlabs.kgraph.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
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
   private ArangoDatabase _userDB;
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

   public void cleanup() throws InterruptedException, ExecutionException {
      // db.drop();
      _systemDB.shutdown();
      _systemDB = null;
   }

   public BaseDocument upsertNode(final ArangoCollection collection, final BaseDocument element) {
      logger.trace("upsertNode " + element.getKey());
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
            result = collection.updateDocument(element.getKey(), element).getNew();
            // result = collection.getDocument(element.getKey(), BaseEdgeDocument.class);
         }
      } catch (Exception e) {
         if (null != element) {
            logger.debug(element.toString());
         } else {
            logger.error("The element is null.");
         }
         if (null != collection) {
            logger.debug(collection.toString());
         } else {
            logger.error("The collection is null.");
         }
         throw e;
      }
      return result;
   }

   public ArangoCollection getEdgeCollection(String collectionName) {
      CollectionEntity collectionEntity = null;
      ArangoCollection collection = _userDB.collection(collectionName);
      if (!collection.exists()) {
         collectionEntity = _userDB.createCollection(collectionName, new CollectionCreateOptions().type(CollectionType.EDGES));
      } else {
         collectionEntity = collection.getInfo();
         logger.debug("createEdgeCollection - result.getType() = " + collectionEntity.getType());
         if (!"EDGES".equals(collectionEntity.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + collectionEntity.getType() + ")");
         }
      }
      ArangoCollection result = _userDB.collection(collectionName);
      return result;
   }

   public ArangoCollection getNodeCollection(String collectionName) {
      CollectionEntity collectionEntity = null;
      ArangoCollection collection = _userDB.collection(collectionName);
      if (!collection.exists()) {
         collectionEntity = _userDB.createCollection(collectionName);
      } else {
         collectionEntity = collection.getInfo();
         if (!"DOCUMENT".equals(collectionEntity.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + collectionEntity.getType() + ")");
         }
      }
      ArangoCollection result = _userDB.collection(collectionName);
      return result;
   }

   public BaseDocument getNodeByKey(String key, String type) {
      BaseDocument doc = _userDB.collection(type).getDocument(key, BaseDocument.class);
      return doc;
   }

   public void flush() throws InterruptedException, ExecutionException {
      if (_systemDB.db(_db_name).exists()) {

         Boolean isDropped = _userDB.drop();
         logger.debug("Is the database " + _db_name + " dropped : " + isDropped);
         logger.debug("database " + _db_name + " is dropped = " + isDropped);
      }
      setUp(_db_name);
   }

   public Long getTotalCount() {
      Long result = 0l;
      Collection<CollectionEntity> collections = _systemDB.db(_db_name).getCollections();
      for (CollectionEntity collectionEntity : collections) {
         result += _userDB.collection(collectionEntity.getName()).count().getCount();
      }

      return result;
   }

   public List<BaseDocument> queryElements(ArangoCollection collection, QueryClause... clauses) {
      Map<String, Object> bindVars = new HashMap<String, Object>();
      List<BaseDocument> results = new ArrayList<BaseDocument>();
      try {
         StringBuilder query = new StringBuilder("FOR t IN ");
         query.append(collection.name());
         query.append(" FILTER ");
         Boolean firstCollection = true;
         for (QueryClause clause : clauses) {
            if (firstCollection) {
               firstCollection = false;
            } else {
               query.append(" AND ");
            }
            query.append("t.");
            query.append(clause.toAQL());
            bindVars.put(clause.getName(), clause.getValue());
         }
         query.append(" RETURN t");
         
         logger.debug("query = '" + query.toString() + "'");
         logger.debug("bindVars = " + bindVars);

         // bindVars = Collections.singletonMap("name", "Homer");
         // query = new StringBuilder("FOR t IN testCollection FILTER t.xname == @name RETURN t");
         ArangoCursor<BaseDocument> cursor = _systemDB.db(_db_name).query(query.toString(), bindVars, BaseDocument.class);
         cursor.forEachRemaining(aDocument -> {
            results.add(aDocument);
         });
      } catch (ArangoDBException e) {
         logger.error("Failed to execute query. " + e.getMessage());
         throw e;
      }
      return results;
   }
}
