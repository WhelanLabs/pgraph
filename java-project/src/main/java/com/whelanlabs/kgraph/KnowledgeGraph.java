package com.whelanlabs.kgraph;

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

	// private static final String ANDREW_DB = "andrew_graph_db";
	private ArangoDB arangoDB;
	public ArangoDatabase db;

	private static Logger logger = LogManager.getLogger(KnowledgeGraph.class);

	public KnowledgeGraph(String db_name) throws Exception {
		setUp(db_name);
	}

	private void setUp(String db_name) throws InterruptedException, ExecutionException {
		arangoDB = getDB();

		DbName dbName = DbName.normalize(db_name);

		if (arangoDB.db(dbName).exists()) {
			// arangoDB.db(ANDREW_DB).drop();
		} else {
			arangoDB.createDatabase(dbName);
		}
		db = arangoDB.db(dbName);

	}

	private static ArangoDB getDB() {
		return new ArangoDB.Builder().user("root").password("openSesame").serializer(new ArangoJack()).build();
	}

	public void tearDown() throws InterruptedException, ExecutionException {
		// db.drop();
		arangoDB.shutdown();
		arangoDB = null;
	}

	public BaseDocument upsertNode(final ArangoCollection collection, final BaseDocument element) {
		BaseDocument result = null;
		System.out.println("upsertNode " + element.getKey());
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

	public CollectionEntity createEdgeCollection(String collectionName) {
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

	public CollectionEntity createNodeCollection(String collectionName) {
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

	public static void removeTablespace(String db_name) {
		ArangoDB arangodb_database = getDB();
		DbName dbName = DbName.normalize(db_name);
		ArangoDatabase tablespace = arangodb_database.db(dbName);
		try {
			tablespace.drop();
		} catch (ArangoDBException e) {
			logger.error(e.getMessage());
			if (!e.getMessage().contains("database not found")) {
				throw e;
			}
		}
	}

	public BaseDocument getNodeByKey(String key, String type) {
		BaseDocument doc = db.collection(type).getDocument(key, BaseDocument.class);
		return doc;
	}
}
