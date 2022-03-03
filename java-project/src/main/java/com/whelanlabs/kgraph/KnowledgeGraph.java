package com.whelanlabs.kgraph;

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

	//private ArangoDB arangoDB;
	public ArangoDatabase _userDB;
	private DbName _db_name;
	private static ArangoDB _systemDB = null;

	private static Logger logger = LogManager.getLogger(KnowledgeGraph.class);

	public KnowledgeGraph(String db_name) throws Exception {
		_db_name = DbName.normalize(db_name);
		setUp(_db_name);
	}

	private void setUp(DbName db_name) throws InterruptedException, ExecutionException {
		// arangoDB = getDB();
		if (!_systemDB.db(db_name).exists()) {
			_systemDB.createDatabase(db_name);
		}
		_userDB = _systemDB.db(db_name);

	}

	private static ArangoDB getDB() {
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
		BaseDocument result = null;
		logger.debug("upsertNode " + element.getKey());
		try {
			if (!collection.documentExists(element.getKey())) {
				collection.insertDocument(element);
				result = element;
			} else {
				logger.debug("Fetch already existing element. (key=" + element.getKey() + ")");
				collection.updateDocument(element.getKey(), element);
				result = element;
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
		return result;
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
		BaseDocument doc = _userDB.collection(type).getDocument(key, BaseDocument.class);
		return doc;
	}

	public Collection<CollectionEntity> getCollections() {
		return _systemDB.db(_db_name).getCollections();
	}

	public void flush() throws InterruptedException, ExecutionException {
		if (_systemDB.db(_db_name).exists()) {
			for( CollectionEntity collection : getCollections()) {
				collection.
			}
			
			Boolean isDropped = _userDB.drop();
			if (isDropped) {
				logger.info("database dropped (" + _db_name + ")");
			}
		}
		setUp(_db_name);
	}
}
