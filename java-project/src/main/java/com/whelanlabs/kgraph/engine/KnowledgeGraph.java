package com.whelanlabs.kgraph.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.entity.BaseEdgeDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.util.MapBuilder;

public class KnowledgeGraph {

   // private ArangoDB arangoDB;
   private ArangoDatabase _userDB;
   private DbName _db_name;
   private ArangoDB _systemDB = null;
   private String nodeTypesCollectionName = "node_types";
   private String edgeTypesCollectionName = "edge_types";
   private ArangoCollection nodeTypesCollection;
   private ArangoCollection edgeTypesCollection;

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
      
      // create node_types collection if not exists
      try {
         _userDB.createCollection(nodeTypesCollectionName);
      }
      catch(ArangoDBException e) {
         if(e.getMessage().contains("duplicate name")) {
            // do nothing - all is fine
         }
         else {
            throw e;
         }
      }      
      nodeTypesCollection = _userDB.collection(nodeTypesCollectionName);
      
      // create edge_types collection if not exists
      try {
         _userDB.createCollection(edgeTypesCollectionName);
      }
      catch(ArangoDBException e) {
         if(e.getMessage().contains("duplicate name")) {
            // do nothing - all is fine
         }
         else {
            throw e;
         }
      }   
      edgeTypesCollection = _userDB.collection(edgeTypesCollectionName);
      
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

   public Node upsertNode(final ArangoCollection collection, final Node element) {
      logger.trace("upsertNode " + element.getKey());
      try {
         if (!collection.documentExists(element.getKey())) {
            collection.insertDocument(element);
         } else {
            collection.updateDocument(element.getKey(), element);
         }
      } catch (Exception e) {
         logger.error(element.toString());
         throw e;
      }
      return element;
   }

   public ArrayList<Node> upsertNode(final ArangoCollection collection, final Node... elements) {
      // TODO: address ACID requirements
      ArrayList<Node> results = new ArrayList<Node>();
      for (Node element : elements) {
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

   public ArangoCollection createEdgeCollection(String collectionName, String leftCollection, String rightCollection) {

      // TODO: if neither collection or registration exist, create them
      
      // TODO: else if collection and registration exist, verify they match (if not exception)
      
      // TODO: else if collection or registration exist, then create the missing
      
      
      return null;
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
   
   public ArangoCollection createNodeCollection(String collectionName) {
      // create node collection
      _userDB.createCollection(collectionName);
      
      // TODO: register node collection
      Node nodeTypeNode = new Node(collectionName);
      upsertNode(nodeTypesCollection, nodeTypeNode);
      
      return _userDB.collection(collectionName);
      
   }

   public ArangoCollection getNodeCollection(String collectionName) {
      CollectionEntity collectionEntity = null;
      ArangoCollection result;
      ArangoCollection collection = _userDB.collection(collectionName);
      if (!collection.exists()) {
         result = createNodeCollection(collectionName);
      } else {
         collectionEntity = collection.getInfo();
         if (!"DOCUMENT".equals(collectionEntity.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + collectionEntity.getType() + ")");
         }
         result = _userDB.collection(collectionName);
      }
      
      return result;
   }

   public BaseEdgeDocument getEdgeByKey(String key, String type) { 
      BaseEdgeDocument doc = _userDB.collection(type).getDocument(key, BaseEdgeDocument.class);
      return doc;
   }
   
   public Node getNodeByKey(String key, String type) {
      Node doc = _userDB.collection(type).getDocument(key, Node.class);
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

   public List<Node> queryElements(ArangoCollection collection, QueryClause... clauses) {
      MapBuilder bindVars = new MapBuilder();
      List<Node> results = new ArrayList<Node>();
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

         // query = new StringBuilder("FOR t IN testCollection FILTER t.foo == @foo
         // RETURN t");

         logger.debug("query = '" + query.toString() + "'");
         logger.debug("bindVars = " + bindVars);

         ArangoCursor<Node> cursor = _systemDB.db(_db_name).query(query.toString(), bindVars.get(), Node.class);
         cursor.forEachRemaining(aDocument -> {
            results.add(aDocument);
         });
      } catch (Exception e) {
         logger.error("Failed to execute query. " + e.getMessage());
         throw e;
      }
      return results;
   }

   public List<Triple> expandElements(List<Node> startingNodes, String relname, List<QueryClause> relClauses, List<QueryClause> otherSideClauses) {
      List<Triple> results = new ArrayList<Triple>();
      
      return results;
   }

   public String generateKey() {
      return "KEY_" + UUID.randomUUID().toString();
   }
   
   public String generateName() {
      return "NAME_" + UUID.randomUUID().toString();
   }
}
