package com.whelanlabs.kgraph;

public abstract class BaseGraphTest {

//   private static final String TEST_DB = "java_driver_graph_test_db";
//   private static ArangoDB arangoDB;
//   static ArangoDatabase db;
//   private static final String GRAPH_NAME = "traversalGraph";
//   private static final String EDGE_COLLECTION_NAME = "edges";
//   private static final String VERTEXT_COLLECTION_NAME = "circles";
//
//   @BeforeClass
//   public static void init() {
//      if (arangoDB == null) {
//         arangoDB = new ArangoDB.Builder().build();
//      }
//      if (arangoDB.db(TEST_DB).exists())
//         arangoDB.db(TEST_DB).drop();
//      arangoDB.createDatabase(TEST_DB);
//      BaseGraphTest.db = arangoDB.db(TEST_DB);
//
//      final Collection<EdgeDefinition> edgeDefinitions = new ArrayList<>();
//      final EdgeDefinition edgeDefinition = new EdgeDefinition().collection(EDGE_COLLECTION_NAME).from(VERTEXT_COLLECTION_NAME)
//            .to(VERTEXT_COLLECTION_NAME);
//      edgeDefinitions.add(edgeDefinition);
//      if (!db.graph(GRAPH_NAME).exists())
//         db.createGraph(GRAPH_NAME, edgeDefinitions, null);
//      addExampleElements();
//   }
//
//   @AfterClass
//   public static void shutdown() {
//      arangoDB.db(TEST_DB).drop();
//      arangoDB.shutdown();
//      arangoDB = null;
//   }
//
//   private static void addExampleElements() throws ArangoDBException {
//
//      // Add circle circles
//      final VertexEntity vA = createVertex(new Circle("A", "1"));
//      final VertexEntity vB = createVertex(new Circle("B", "2"));
//      final VertexEntity vC = createVertex(new Circle("C", "3"));
//      final VertexEntity vD = createVertex(new Circle("D", "4"));
//      final VertexEntity vE = createVertex(new Circle("E", "5"));
//      final VertexEntity vF = createVertex(new Circle("F", "6"));
//      final VertexEntity vG = createVertex(new Circle("G", "7"));
//      final VertexEntity vH = createVertex(new Circle("H", "8"));
//      final VertexEntity vI = createVertex(new Circle("I", "9"));
//      final VertexEntity vJ = createVertex(new Circle("J", "10"));
//      final VertexEntity vK = createVertex(new Circle("K", "11"));
//
//      // Add relevant edges - left branch:
//      saveEdge(new CircleEdge(vA.getId(), vB.getId(), false, true, "left_bar"));
//      saveEdge(new CircleEdge(vB.getId(), vC.getId(), false, true, "left_blarg"));
//      saveEdge(new CircleEdge(vC.getId(), vD.getId(), false, true, "left_blorg"));
//      saveEdge(new CircleEdge(vB.getId(), vE.getId(), false, true, "left_blub"));
//      saveEdge(new CircleEdge(vE.getId(), vF.getId(), false, true, "left_schubi"));
//
//      // Add relevant edges - right branch:
//      saveEdge(new CircleEdge(vA.getId(), vG.getId(), false, true, "right_foo"));
//      saveEdge(new CircleEdge(vG.getId(), vH.getId(), false, true, "right_blob"));
//      saveEdge(new CircleEdge(vH.getId(), vI.getId(), false, true, "right_blub"));
//      saveEdge(new CircleEdge(vG.getId(), vJ.getId(), false, true, "right_zip"));
//      saveEdge(new CircleEdge(vJ.getId(), vK.getId(), false, true, "right_zup"));
//   }
//
//   private static void saveEdge(final CircleEdge edge) throws ArangoDBException {
//      db.graph(GRAPH_NAME).edgeCollection(EDGE_COLLECTION_NAME).insertEdge(edge);
//   }
//
//   private static VertexEntity createVertex(final Circle vertex) throws ArangoDBException {
//      return db.graph(GRAPH_NAME).vertexCollection(VERTEXT_COLLECTION_NAME).insertVertex(vertex);
//   }

}