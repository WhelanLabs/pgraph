package com.whelanlabs.kgraph.engine;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.stream.Collectors;

import org.apache.commons.lang3.tuple.Triple;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.arangodb.ArangoCollection;
import com.arangodb.ArangoCursor;
import com.arangodb.ArangoDB;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.TraversalOptions.Direction;
import com.arangodb.util.MapBuilder;
import com.whelanlabs.kgraph.engine.QueryClause.Operator;
import com.whelanlabs.kgraph.serialization.MapperHelper;

public class KnowledgeGraph {

   private ArangoDatabase _userDB;
   private DbName _db_name;
   private ArangoDB _systemDB = null;
   private String nodeTypesCollectionName = "node_types";
   private String edgeTypesCollectionName = "edge_types";
   private Set<String> nodeTypesCache = new HashSet<>();
   private Set<String> edgeTypesCache = new HashSet<>();
   private static Long count = 0L;

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

         _systemDB = new ArangoDB.Builder().user("root").password("openSesame").serializer(new ArangoJack(MapperHelper.createDefaultMapper()))
               .build();
      }
      return _systemDB;
   }

   public void cleanup() throws InterruptedException, ExecutionException {
      // db.drop();
      _systemDB.shutdown();
      _systemDB = null;
   }

   public ElementList<Element> upsert(Element... elements) {
      ElementList<Element> results = new ElementList<>();
      for (Element element : elements) {
         if (element instanceof Node) {
            results.add(_upsert((Node) element));
         } else if (element instanceof Edge) {
            results.add(_upsert((Edge) element));
         } else {
            throw new RuntimeException("Unsupported type for insert");
         }
      }
      return results;
   }

   protected Node _upsert(final Node node) {
      ArangoCollection collection = null;
      try {
         addNodeType(node.getType());
         collection = getNodeCollection(node.getType());
         logger.trace("upsertNode " + node.getKey());
         if (!collection.documentExists(node.getKey())) {
            collection.insertDocument(node);
         } else {
            collection.updateDocument(node.getKey(), node);
         }
      } catch (Exception e) {
         logger.error(node.toString());
         throw e;
      }
      return node;
   }

   private void addNodeType(String type) {
      if (!nodeTypesCache.contains(type)) {
         ArangoCollection nodeTypesCollection = getNodeCollection(nodeTypesCollectionName);
         if (!nodeTypesCollection.documentExists(type)) {
            Map<String, Object> props = new HashMap<>();
            // props.put(typeAttrName, nodeTypesCollection)
            Node node = new Node(props);
            node.setKey(type);
            nodeTypesCollection.insertDocument(node);
            nodeTypesCache.add(type);
         }
      }
   }

   private void addEdgeType(Edge edge) {
      String edgeType = edge.getType();
      String leftType = edge.getAttribute(Edge.leftTypeAttrName).toString();
      String rightType = edge.getAttribute(Edge.rightTypeAttrName).toString();
      String edgeId = leftType + ":" + edgeType + ":" + rightType;

      if (!edgeTypesCache.contains(edgeId)) {
         ArangoCollection edgeTypesCollection = getNodeCollection(edgeTypesCollectionName);

         if (!edgeTypesCollection.documentExists(edgeId)) {
            Map<String, Object> props = new HashMap<>();
            props.put(Edge.leftTypeAttrName, leftType);
            props.put(Edge.rightTypeAttrName, rightType);
            props.put(Edge.edgeTypeAttrName, edgeType);
            Node node = new Node(props);
            node.setKey(edgeId);
            edgeTypesCollection.insertDocument(node);
            edgeTypesCache.add(edgeId);
         }
      }
   }

   protected Edge _upsert(final Edge edge) {
      ArangoCollection collection = null;
      Edge result = null;
      try {
         addEdgeType(edge);
         collection = getEdgeCollection(edge.getType());
         if (!collection.documentExists(edge.getKey())) {
            collection.insertDocument(edge);
            result = edge;
         } else {
            logger.debug("Fetch already existing element. (key=" + edge.getKey() + ")");
            result = collection.updateDocument(edge.getKey(), edge).getNew();
            result = collection.getDocument(edge.getKey(), Edge.class);
         }
      } catch (Exception e) {
         if (null != edge) {
            logger.debug(edge.toString());
         } else {
            logger.error("The edge is null.");
         }
         if (null != collection) {
            logger.debug("collection = '" + collection.toString() + "'");
         } else {
            logger.error("The collection is null.");
         }
         throw e;
      }
      return result;
   }

   protected ArangoCollection getEdgeCollection(String typeName) {
      CollectionEntity collectionEntity = null;
      ArangoCollection collection = _userDB.collection(typeName);
      if (!collection.exists()) {
         collectionEntity = _userDB.createCollection(typeName, new CollectionCreateOptions().type(CollectionType.EDGES));
      } else {
         collectionEntity = collection.getInfo();
         if (!"EDGES".equals(collectionEntity.getType().toString())) {
            throw new RuntimeException("Non-EDGES collection already exsists. (" + collectionEntity.getType() + ")");
         }
      }
      ArangoCollection result = _userDB.collection(typeName);
      return result;
   }

   public ArangoCollection createNodeType(String typeName) {
      _userDB.createCollection(typeName);
      return _userDB.collection(typeName);

   }

   protected ArangoCollection getNodeCollection(String typeName) {
      CollectionEntity collectionEntity = null;
      ArangoCollection result;
      ArangoCollection collection = _userDB.collection(typeName);
      if (!collection.exists()) {
         result = createNodeType(typeName);
      } else {
         collectionEntity = collection.getInfo();
         if (!"DOCUMENT".equals(collectionEntity.getType().toString())) {
            throw new RuntimeException("Non-DOCUMENT collection already exsists. (" + collectionEntity.getType() + ")");
         }
         result = _userDB.collection(typeName);
      }

      return result;
   }

   public Edge getEdgeByKey(String key, String type) {
      Edge doc = _userDB.collection(type).getDocument(key, Edge.class);
      return doc;
   }

   public Node getNodeByKey(String key, String typeName) {
      Node doc = _userDB.collection(typeName).getDocument(key, Node.class);
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

   public List<Node> queryNodes(String typeName, QueryClause... clauses) {
      ArangoCollection collection = _userDB.collection(typeName);
      MapBuilder bindVars = new MapBuilder();
      List<Node> results = new ArrayList<Node>();
      try {
         StringBuilder query = generateQuery(collection, bindVars, clauses);

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

   public List<Edge> queryEdges(String typeName, QueryClause... clauses) {
      MapBuilder bindVars = new MapBuilder();
      List<Edge> results = new ArrayList<Edge>();
      try {
         ArangoCollection collection = getEdgeCollection(typeName);
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
      if (clauses.length > 0) {
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
      }
      query.append(" RETURN t");

      logger.debug("query = '" + query.toString() + "'");
      logger.debug("bindVars = " + bindVars);
      return query;
   }

   public List<Triple<Node, Edge, Node>> expandRight(Node leftNode, String edgeCollectionName, List<QueryClause> relClauses,
         List<QueryClause> otherSideClauses) {
      return expand(leftNode, edgeCollectionName, relClauses, otherSideClauses, Direction.outbound);
   }

   public List<Triple<Node, Edge, Node>> expandLeft(Node rightNode, String edgeCollectionName, List<QueryClause> relClauses,
         List<QueryClause> otherSideClauses) {
      return expand(rightNode, edgeCollectionName, relClauses, otherSideClauses, Direction.inbound);
   }

   protected List<Triple<Node, Edge, Node>> expand(Node startingNode, String edgeCollectionName, List<QueryClause> relClauses,
         List<QueryClause> otherSideClauses, Direction direction) {
      List<Triple<Node, Edge, Node>> results = new ArrayList<>();
      // QueryClause edgeIDQueryClause = new QueryClause("_from", Operator.EQUALS,
      // startingNode.getId());
      QueryClause edgeIDQueryClause = new QueryClause(ElementFactory.getLeftAttrString(direction), Operator.EQUALS, startingNode.getId());
      List<QueryClause> augmentedRelClauses = new ArrayList<>();
      if (null != relClauses) {
         augmentedRelClauses.addAll(relClauses);
      }
      augmentedRelClauses.add(edgeIDQueryClause);
      List<Edge> edges = queryEdges(edgeCollectionName, augmentedRelClauses.toArray(new QueryClause[0]));

      for (Edge edge : edges) { // edge.getTo()
         QueryClause otherSideIDQueryClause = new QueryClause("_id", Operator.EQUALS, ElementFactory.getRightIdString(direction, edge));
         List<QueryClause> augmentedOtherSideClauses = new ArrayList<>();
         if (null != otherSideClauses) {
            augmentedOtherSideClauses.addAll(otherSideClauses);
         }
         augmentedOtherSideClauses.add(otherSideIDQueryClause);
         String typeName = ElementFactory.getTypeName(edge.getTo());

         List<Node> otherSides = queryNodes(typeName, augmentedOtherSideClauses.toArray(new QueryClause[0]));
         if (1 == otherSides.size()) {
            results.add(Triple.of(startingNode, edge, otherSides.get(0)));
         }
      }

      return results;
   }

   public static String generateKey() {
      return "KEY_" + System.currentTimeMillis() + count++;
   }

   public static String generateName() {
      return "NAME_" + System.currentTimeMillis() + count++;
   }

   public Long getCount(String typeName) {
      ArangoCollection collection = _userDB.collection(typeName);
      Long result = collection.count().getCount();
      return result;
   }

   public List<String> getNodeTypes() {
      List<Node> nodes = queryNodes(nodeTypesCollectionName);
      List<String> results = nodes.stream().map(object -> object.getKey()).collect(Collectors.toList());
      return results;
   }

   public List<Node> getEdgeTypes() {
      List<Node> edgeTypes = queryNodes(edgeTypesCollectionName);
      List<Node> results = new ArrayList<>();
      for(Node edgeType : edgeTypes) {
         results.add(edgeType);
      }
      return results;
   }

   public List<String> getEdgeTypesForLeftType(String leftType) {
      QueryClause queryClause = new QueryClause(Edge.leftTypeAttrName, QueryClause.Operator.EQUALS, leftType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> edgeTypes = edgeTypeNodes.stream()
            .map(object -> object.getAttribute(Edge.edgeTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> edgeTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(edgeTypes));
      return edgeTypesNoDups;
   }

   public List<String> getEdgeTypesForRightType(String rightType) {
      QueryClause queryClause = new QueryClause(Edge.rightTypeAttrName, QueryClause.Operator.EQUALS, rightType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> edgeTypes = edgeTypeNodes.stream()
            .map(object -> object.getAttribute(Edge.edgeTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> edgeTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(edgeTypes));
      return edgeTypesNoDups;
   }

   public List<String> getLeftTypesForEdgeType(String edgeType) {
      QueryClause queryClause = new QueryClause(Edge.edgeTypeAttrName, QueryClause.Operator.EQUALS, edgeType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> leftTypes = edgeTypeNodes.stream()
            .map(object -> object.getAttribute(Edge.leftTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> leftTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(leftTypes));
      return leftTypesNoDups;
   }

   public List<String> getRightTypesforEdgeType(String edgeType) {
      QueryClause queryClause = new QueryClause(Edge.edgeTypeAttrName, QueryClause.Operator.EQUALS, edgeType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> leftTypes = edgeTypeNodes.stream()
            .map(object -> object.getAttribute(Edge.rightTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> leftTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(leftTypes));
      return leftTypesNoDups;
   }
}
