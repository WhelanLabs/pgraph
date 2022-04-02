package com.whelanlabs.kgraph.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.entity.BaseDocument;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.entity.PathEntity;
import com.arangodb.entity.TraversalEntity;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.TraversalOptions;
import com.arangodb.model.TraversalOptions.Direction;
import com.arangodb.util.MapBuilder;

public class KnowledgeGraph {

   // private ArangoDB arangoDB;
   private ArangoDatabase _userDB;
   private DbName _db_name;
   private ArangoDB _systemDB = null;
   private String nodeTypesCollectionName = "node_types";
   private String edgeTypesCollectionName = "edge_types";

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

   public BaseDocument upsertNode(final ArangoCollection collection, final BaseDocument element) {
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

   public Edge upsertEdge(final ArangoCollection collection, final Edge element) {
      Edge result = null;
      try {
         if (!collection.documentExists(element.getKey())) {
            collection.insertDocument(element);
            result = element;
         } else {
            logger.debug("Fetch already existing element. (key=" + element.getKey() + ")");
            result = collection.updateDocument(element.getKey(), element).getNew();
            // result = collection.getDocument(element.getKey(), Edge.class);
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
            throw new RuntimeException("Non-EDGES collection already exsists. (" + collectionEntity.getType() + ")");
         }
      }
      ArangoCollection result = _userDB.collection(collectionName);
      return result;
   }

   public ArangoCollection createNodeCollection(String collectionName) {
      _userDB.createCollection(collectionName);
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

   public Edge getEdgeByKey(String key, String type) {
      Edge doc = _userDB.collection(type).getDocument(key, Edge.class);
      return doc;
   }

   public Node getNodeByKey(String key, String collectionName) {
      Node doc = _userDB.collection(collectionName).getDocument(key, Node.class);
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

   public List<Node> queryNodes(ArangoCollection collection, QueryClause... clauses) {
      MapBuilder bindVars = new MapBuilder();
      List<Node> results = new ArrayList<Node>();
      try {
         StringBuilder query = generateQuery(collection, bindVars, clauses);

         ArangoCursor<Node> cursor = _systemDB.db(_db_name).query(query.toString(), bindVars.get(), Node.class);
         cursor.forEachRemaining(aDocument -> {
            System.out.println("cursor element!");
            results.add(aDocument);
         });
      } catch (Exception e) {
         logger.error("Failed to execute query. " + e.getMessage());
         throw e;
      }
      return results;
   }

   public List<Edge> queryEdges(ArangoCollection collection, QueryClause... clauses) {
      MapBuilder bindVars = new MapBuilder();
      List<Edge> results = new ArrayList<Edge>();
      try {
         StringBuilder query = generateQuery(collection, bindVars, clauses);

         ArangoCursor<Edge> cursor = _systemDB.db(_db_name).query(query.toString(), bindVars.get(), Edge.class);
         cursor.forEachRemaining(aDocument -> {
            results.add(aDocument);
         });
      } catch (Exception e) {
         logger.error("Failed to execute query. " + e.getMessage());
         throw e;
      }
      return results;
   }

   private StringBuilder generateQuery(ArangoCollection collection, MapBuilder bindVars, QueryClause... clauses) {
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
      return query;
   }

   public List<PathEntity<Node, Edge>> expandRight(Node leftNode, ArangoCollection edgeCollection, List<QueryClause> relClauses,
         List<QueryClause> otherSideClauses) {
      // TODO: see line 1260+ of:
      // https://github.com/arangodb/arangodb-java-driver/blob/4d39da8111bd36cec5207b193bbac2f11a68abfb/src/test/java/com/arangodb/ArangoDatabaseTest.java
      final TraversalOptions options = new TraversalOptions().edgeCollection(edgeCollection.name()).startVertex(leftNode.getId())
            .direction(Direction.outbound);
      final TraversalEntity<Node, Edge> traversal = _userDB.executeTraversal(Node.class, Edge.class, options);
      final Collection<Node> vertices = traversal.getVertices();

      Collection<PathEntity<Node, Edge>> paths = traversal.getPaths();
      Iterator<PathEntity<Node, Edge>> pathsItr = paths.iterator();
      // List<PathEntity<Node, Edge>>
      List<PathEntity<Node, Edge>> results = new ArrayList<PathEntity<Node, Edge>>();
      while (pathsItr.hasNext()) {
         PathEntity<Node, Edge> currentPath = pathsItr.next();
         if (currentPath.getEdges() != null) {
            results.add(pathsItr.next());
         }

      }

      return results;
   }

   public String generateKey() {
      return "KEY_" + UUID.randomUUID().toString();
   }

   public String generateName() {
      return "NAME_" + UUID.randomUUID().toString();
   }
}
