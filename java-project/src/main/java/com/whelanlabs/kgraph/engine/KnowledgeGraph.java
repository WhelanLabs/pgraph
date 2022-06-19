package com.whelanlabs.kgraph.engine;

import java.io.IOException;
import java.io.InputStream;
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
import com.arangodb.ArangoDBException;
import com.arangodb.ArangoDatabase;
import com.arangodb.DbName;
import com.arangodb.entity.CollectionEntity;
import com.arangodb.entity.CollectionType;
import com.arangodb.mapping.ArangoJack;
import com.arangodb.model.CollectionCreateOptions;
import com.arangodb.model.TraversalOptions.Direction;
import com.arangodb.util.MapBuilder;
import com.whelanlabs.kgraph.engine.QueryClause.Operator;

/**
 * The Class KnowledgeGraph.  This class provides a generic implementation
 * of a persistent property graph.
 * 
 * This class is a bit of a God Class, and should be considered 
 * for refactoring, but it works for now, and there are more pressing
 * things to do for now.  (Possible future work - luckily, good
 * test coverage should make refactoring a real possibility.
 * see also: https://en.wikipedia.org/wiki/God_object
 */
public class KnowledgeGraph {

   /** The user DB. */
   // TODO: make this private again once Andrew's need is moved back into KGraph.
   public ArangoDatabase _userDB;

   /** The db name. */
   private DbName _db_name;

   /** The system DB. */
   private ArangoDB _systemDB = null;

   /** The node types collection name. This Collection 
    * contains schema information on Nodes*/
   protected static String nodeTypesCollectionName = "node_types";

   /** The edge types collection name. This Collection 
    * contains schema information on Edges*/
   protected static String edgeTypesCollectionName = "edge_types";

   /** The node types cache. */
   private Set<String> nodeTypesCache = new HashSet<>();

   /** The edge types cache. */
   private Set<String> edgeTypesCache = new HashSet<>();

   /** The logger. */
   private static Logger logger = LogManager.getLogger(KnowledgeGraph.class);

   /**
    * Instantiates a new knowledge graph.
    *
    * @param db_name the db name
    * @throws Exception the exception
    */
   public KnowledgeGraph(String db_name) throws Exception {
      _db_name = DbName.normalize(db_name);
      setupApplicationDatabase(_db_name);
   }

   /**
    * Sets up the application database.  If no database of the given name
    * is specified, a new database will be created.  Otherwise, the existing
    * database with the given name will be used.
    *
    * @param db_name the name of the database to be set up.
    * @throws InterruptedException the interrupted exception
    * @throws ExecutionException the execution exception
    * @throws IOException 
    */
   private void setupApplicationDatabase(DbName db_name) throws InterruptedException, ExecutionException, IOException {
      setupSystemDatabase();
      if (!_systemDB.db(db_name).exists()) {
         _systemDB.createDatabase(db_name);
      }
      _userDB = _systemDB.db(db_name);
   }

   /**
    * Sets up the system DB.
    * 
    * Loads the config properties from the "config.properties" resource in the classpath.
    * non-configured properties assume default values.
    * 
    * see also: https://www.arangodb.com/docs/stable/drivers/java-reference-setup.html
    * 
    * @return the ArangoDB system database
    * @throws IOException 
    */
   private synchronized ArangoDB setupSystemDatabase() throws IOException {
      if (null == _systemDB) {
         try (InputStream in = KnowledgeGraph.class.getClassLoader().getResourceAsStream("config.properties")) {
            _systemDB = new ArangoDB.Builder().loadProperties(in).serializer(new ArangoJack(MapperHelper.createKGraphMapper())).build();
         }
      }
      return _systemDB;
   }

   /**
    * Cleanup.
    *
    * @throws InterruptedException the interrupted exception
    * @throws ExecutionException the execution exception
    */
   public void cleanup() throws InterruptedException, ExecutionException {
      // db.drop();
      _systemDB.shutdown();
      _systemDB = null;
   }

   /**
    * Upsert.  Creates an Elements if they do not exist, and updates
    * Elements if they already exist.
    * 
    * For updates, all attributes from the input Element will be 
    * added to the Element if they do not yet exist, and overwritten 
    * in the existing Element if they do exist there.  Attributes
    * not specified in the input element will retain their previous
    * values.
    * 
    * Creation of edges requires that the end-point Nodes for the 
    * Edge have already been persisted.  The creation can be in the
    * same call as long as the end-point Nodes proceed the Edge 
    * in the input Array.
    *
    * @param elements the elements
    * @return the element list.  Order is the same as the input elements.
    */
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

   /**
    * Upsert.  Creates a Node if it does not exist, and updates
    * the Node if it already exists.
    * 
    * For updates, all attributes specified in the input Node will be 
    * added to the persisted Node if they do not yet exist, and overwritten 
    * in the persisted Node if they do exist there.
    *
    * @param node the node
    * @return the node
    */
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

   /**
    * Adds the node type.
    * 
    * As it currently stands, schema elements are generated by specific 
    * element creations, and do not reflect "possible" content that does
    * exist.  This is currently seen as a valid paradigm; however, this
    * may be augmented in the future to either allow schema creation 
    * without requiring the creation of instances.  Additionally, because
    * there are currently no methods to delete elements, no dangling
    * schema content should normally exist; this too may change in the
    * future.  Allowing existing schema in the future without associated
    * element is not assumed to be a breaking change for defining schema
    * content.
    * 
    * @param type the type
    */
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

   /**
    * Adds the edge type.
    *
    * As it currently stands, schema elements are generated by specific 
    * element creations, and do not reflect "possible" content that does
    * exist.  This is currently seen as a valid paradigm; however, this
    * may be augmented in the future to either allow schema creation 
    * without requiring the creation of instances.  Additionally, because
    * there are currently no methods to delete elements, no dangling
    * schema content should normally exist; this too may change in the
    * future.  Allowing existing schema in the future without associated
    * element is not assumed to be a breaking change for defining schema
    * content.
    * 
    * @param edge the edge
    */
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

   /**
    * Upsert.  Creates an Edge if it does not exist, and updates
    * the Edge if it already exists.
    * 
    * For updates, all attributes from the input Edge will be 
    * added to the Edge if they do not yet exist, and overwritten 
    * in the existing Edge if they do exist there.
    * 
    * Creation of edges requires that the end-point Nodes for the 
    * Edge have already been persisted.
    *
    * @param edge the edge
    * @return the edge
    */
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

   /**
    * Gets the edge collection.
    *
    * @param typeName the type name
    * @return the edge collection
    */
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

   /**
    * Creates the node type.
    *
    * @param typeName the type name
    * @return the ArangoDB collection
    */
   private ArangoCollection createNodeType(String typeName) {
      _userDB.createCollection(typeName);
      return _userDB.collection(typeName);

   }

   /**
    * Gets the node collection.
    *
    * @param typeName the type name
    * @return the node collection
    */
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

   /**
    * Gets the edge by key.
    *
    * @param key the key
    * @param type the type
    * @return the edge by key
    */
   public Edge getEdgeByKey(String key, String type) {
      Edge doc = _userDB.collection(type).getDocument(key, Edge.class);
      return doc;
   }

   /**
    * Gets the node by key.
    *
    * @param key the key
    * @param typeName the type name
    * @return the node by key
    */
   public Node getNodeByKey(String key, String typeName) {
      Node doc = _userDB.collection(typeName).getDocument(key, Node.class);
      if(null == doc) {
         throw new RuntimeException("Node does not exist. (key = " + key + ")");
      }
      return doc;
   }

   /**
    * Flush.  Empties the application database.
    *
    * @throws InterruptedException the interrupted exception
    * @throws ExecutionException the execution exception
    * @throws IOException 
    */
   public void flush() throws Exception {
      if (_systemDB.db(_db_name).exists()) {

         Boolean isDropped = _userDB.drop();
         logger.debug("Is the database " + _db_name + " dropped : " + isDropped);
         logger.debug("database " + _db_name + " is dropped = " + isDropped);
      }
      setupApplicationDatabase(_db_name);
   }

   /**
    * Gets the total count of elements in the application database.
    * 
    *
    * @return the total count
    */
   public Long getTotalCount() {
      Long result = 0l;
      Collection<CollectionEntity> collections = _systemDB.db(_db_name).getCollections();
      for (CollectionEntity collectionEntity : collections) {
         logger.debug("collectionEntity name: " + collectionEntity.getName());
         String collectionName = collectionEntity.getName();
         if (collectionName.startsWith("_")) {
            // skip - this is a ArangoDB framework Collection
         } else if (collectionName.equals(nodeTypesCollectionName)) {
            // skip - this is a kgraph schema collection
         } else if (collectionName.equals(edgeTypesCollectionName)) {
            // skip - this is a kgraph schema collection
         } else {
            result += _userDB.collection(collectionEntity.getName()).count().getCount();
         }
      }

      return result;
   }

   /**
    * Query nodes.
    *
    * @param typeName the type name
    * @param clauses the clauses
    * @return the list
    */
   public List<Node> queryNodes(String typeName, QueryClause... clauses) {
      ArangoCollection collection = _userDB.collection(typeName);
      MapBuilder bindVars = new MapBuilder();
      List<Node> results = new ArrayList<Node>();
      try {
         StringBuilder query = generateQuery(collection, bindVars, clauses);
         try {
            ArangoCursor<Node> cursor = _systemDB.db(_db_name).query(query.toString(), bindVars.get(), Node.class);
            cursor.forEachRemaining(aDocument -> {
               results.add(aDocument);
            });
         } catch (ArangoDBException e) {
            if (!e.getErrorMessage().contains("collection or view not found")) {
               throw e;
            }
         }
      } catch (Exception e) {
         logger.error("Failed to execute query. " + e.getMessage());
         throw e;
      }
      return results;
   }

   /**
    * Query edges.
    *
    * @param typeName the type name
    * @param clauses the clauses
    * @return the list
    */
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

   /**
    * Generate query.
    *
    * @param collection the collection
    * @param bindVars the bind vars
    * @param clauses the clauses
    * @return the string builder
    */
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

   /**
    * Expand right.
    * 
    * The returned triples are ordered in the context of the expansion direction.
    * This means that if expanding from left to right, then the triple order is
    * {left, edge, right}, and vice versa for expand from right to left.
    * (The otherside nodes are always last in the triple.)
    *
    * @param leftNode the left node
    * @param edgeCollectionName the edge collection name
    * @param relClauses the rel clauses
    * @param otherSideClauses the other side clauses
    * @return the list
    */
   public List<Triple<Node, Edge, Node>> expandRight(Node leftNode, String edgeCollectionName, List<QueryClause> relClauses,
         List<QueryClause> otherSideClauses) {
      return expand(leftNode, edgeCollectionName, relClauses, otherSideClauses, Direction.outbound);
   }

   /**
    * Expand left.
    *
    * The returned triples are ordered in the context of the expansion direction.
    * This means that if expanding from left to right, then the triple order is
    * {left, edge, right}, and vice versa for expand from right to left.
    * (The otherside nodes are always last in the triple.)
    * 
    * @param rightNode the right node
    * @param edgeCollectionName the edge collection name
    * @param relClauses the rel clauses
    * @param otherSideClauses the other side clauses
    * @return the list
    */
   public List<Triple<Node, Edge, Node>> expandLeft(Node rightNode, String edgeCollectionName, List<QueryClause> relClauses,
         List<QueryClause> otherSideClauses) {
      return expand(rightNode, edgeCollectionName, relClauses, otherSideClauses, Direction.inbound);
   }

   /**
    * Expand.
    *
    * The returned triples are ordered in the context of the expansion direction.
    * This means that if expanding from left to right, then the triple order is
    * {left, edge, right}, and vice versa for expand from right to left.
    * (The otherside nodes are always last in the triple.)
    * 
    * @param startingNode the starting node
    * @param edgeCollectionName the edge collection name
    * @param relClauses the rel clauses
    * @param otherSideClauses the other side clauses
    * @param direction the direction
    * @return the list
    */
   protected List<Triple<Node, Edge, Node>> expand(Node startingNode, String edgeCollectionName, List<QueryClause> relClauses,
         List<QueryClause> otherSideClauses, Direction direction) {
      List<Triple<Node, Edge, Node>> results = new ArrayList<>();
      QueryClause edgeIDQueryClause = new QueryClause(ElementHelper.getLeftAttrString(direction), Operator.EQUALS, startingNode.getId());
      List<QueryClause> augmentedRelClauses = new ArrayList<>();
      if (null != relClauses) {
         augmentedRelClauses.addAll(relClauses);
      }
      augmentedRelClauses.add(edgeIDQueryClause);
      List<Edge> edges = queryEdges(edgeCollectionName, augmentedRelClauses.toArray(new QueryClause[0]));

      for (Edge edge : edges) {
         QueryClause otherSideIDQueryClause = new QueryClause("_id", Operator.EQUALS, ElementHelper.getRightIdString(direction, edge));
         List<QueryClause> augmentedOtherSideClauses = new ArrayList<>();
         if (null != otherSideClauses) {
            augmentedOtherSideClauses.addAll(otherSideClauses);
         }
         augmentedOtherSideClauses.add(otherSideIDQueryClause);
         String typeName;
         if (direction == Direction.outbound) {
            typeName = edge.getRightType();
         } else { // if (direction == Direction.inbound) {
            /* The case of any response besides Direction.outbound or Direction.inbound is 
             * currently caught by the above "new QueryClause()" call, thus making
             * this code safe.  However, in the future code changes in the future might
             * make Direction.any a valid option. if so, this code will need to be improved.
             * 
             * Currently, protection against Direction.any is achieved by the unit test
             * "expandBoth_noFilters_exception".
             *  
             * Maybe some day when I am bored, I'll come back, add a real "elese" choice
             * here that throws an exception, and test it using a mocking framework.
             */
            typeName = edge.getLeftType();
         }

         List<Node> otherSides = queryNodes(typeName, augmentedOtherSideClauses.toArray(new QueryClause[0]));
         if (1 == otherSides.size()) {
            results.add(Triple.of(startingNode, edge, otherSides.get(0)));
         }
      }

      return results;
   }

   /**
    * Gets the count of elements of a given type that are currently persisted.
    *
    * @param typeName the type name
    * @return the count
    */
   public Long getCount(String typeName) {
      Long result = 0l;
      ArangoCollection collection = _userDB.collection(typeName);
      try {
         result = collection.count().getCount();
      }
      catch (Exception e) {
         if( e.getMessage()==null || !(e.getMessage().contains("collection or view not found"))) {
            throw e;
         }
      }
      // collection or view not found
      
      return result;
   }

   /**
    * Gets a list of the Types of Nodes in KGraph.
    *
    * @return the node types
    */
   public List<String> getNodeTypes() {
      List<Node> nodes = queryNodes(nodeTypesCollectionName);
      List<String> results = nodes.stream().map(object -> object.getKey()).collect(Collectors.toList());
      return results;
   }

   /**
    * Gets a list of Edge Type Schema Nodes in the current KGraph.
    *
    * @return the edge types
    */
   public List<Node> getEdgeTypes() {
      List<Node> edgeTypes = queryNodes(edgeTypesCollectionName);
      List<Node> results = new ArrayList<>();
      for (Node edgeType : edgeTypes) {
         results.add(edgeType);
      }
      return results;
   }

   /**
    * Gets a list of Edge Type Schema Nodes in the current KGraph
    * for which the input type is the defined left type.
    * 
    * Keep in mind that for a given Edge type, instances may have
    * different end types. each valid combination of left type,
    * edge type, and right type will correspond to a unique Edge 
    * Type Schema Node.
    *
    * @param leftType the left type
    * @return the edge types for left type
    */
   public List<String> getEdgeTypesForLeftType(String leftType) {
      QueryClause queryClause = new QueryClause(Edge.leftTypeAttrName, QueryClause.Operator.EQUALS, leftType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> edgeTypes = edgeTypeNodes.stream().map(object -> object.getAttribute(Edge.edgeTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> edgeTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(edgeTypes));
      return edgeTypesNoDups;
   }

   /**
    * Gets a list of Edge Type Schema Nodes in the current KGraph
    * for which the input type is the defined right type.
    * 
    * Keep in mind that for a given Edge type, instances may have
    * different end types. each valid combination of left type,
    * edge type, and right type will correspond to a unique Edge 
    * Type Schema Node.
    *
    * @param rightType the right type
    * @return the edge types for right type
    */
   public List<String> getEdgeTypesForRightType(String rightType) {
      QueryClause queryClause = new QueryClause(Edge.rightTypeAttrName, QueryClause.Operator.EQUALS, rightType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> edgeTypes = edgeTypeNodes.stream().map(object -> object.getAttribute(Edge.edgeTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> edgeTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(edgeTypes));
      return edgeTypesNoDups;
   }

   /**
    * Gets a list of all left-side Node Type Names in the current KGraph
    * for which the input type is the defined edge type.
    * 
    * (Put another way, this method shows all the types for
    * which the edge may an associated left Node.)
    *
    * @param edgeType the edge type
    * @return the left types for edge type
    */
   public List<String> getLeftTypesForEdgeType(String edgeType) {
      QueryClause queryClause = new QueryClause(Edge.edgeTypeAttrName, QueryClause.Operator.EQUALS, edgeType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> leftTypes = edgeTypeNodes.stream().map(object -> object.getAttribute(Edge.leftTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> leftTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(leftTypes));
      return leftTypesNoDups;
   }

   /**
    * Gets a list of all right-side Node Type Names in the current KGraph
    * for which the input type is the defined edge type.
    * 
    * (Put another way, this method shows all the types for
    * which the edge may an associated right Node.)
    *
    * @param edgeType the edge type
    * @return the right types for edge type
    */
   public List<String> getRightTypesforEdgeType(String edgeType) {
      QueryClause queryClause = new QueryClause(Edge.edgeTypeAttrName, QueryClause.Operator.EQUALS, edgeType);
      List<Node> edgeTypeNodes = queryNodes(edgeTypesCollectionName, queryClause);
      List<String> leftTypes = edgeTypeNodes.stream().map(object -> object.getAttribute(Edge.rightTypeAttrName).toString())
            .collect(Collectors.toList());
      List<String> leftTypesNoDups = new ArrayList<String>(new LinkedHashSet<>(leftTypes));
      return leftTypesNoDups;
   }
   
   public List<Node> query(String query, Map<String, Object> bindVars) {
      List<Node> resultNodes = new ArrayList<>();
      ArangoCursor<Node> cursor = _userDB.query(query, bindVars, null, Node.class);
      cursor.forEachRemaining(aDocument -> {
         logger.debug("result: " + aDocument);
         resultNodes.add(aDocument);
      });
      return resultNodes;
   }
   
   // TODO: augment delete to cascade delete edges when a node is deleted.
   public void delete(Element element) {
      try {
         _userDB.collection(element.getType()).deleteDocument(element.getKey());
       } catch (ArangoDBException e) {
         throw new RuntimeException("Failed to delete element.", e);
       }
   }
   
}
