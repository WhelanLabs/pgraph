package com.whelanlabs.kgraph.engine;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import com.arangodb.entity.DocumentField;
import com.arangodb.internal.mapping.ArangoAnnotationIntrospector;
import com.arangodb.internal.mapping.VPackDeserializers;
import com.arangodb.internal.mapping.VPackSerializers;
import com.arangodb.jackson.dataformat.velocypack.VPackMapper;
import com.arangodb.velocypack.VPackSlice;
import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonDeserializer;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleModule;

/**
 * The Class MapperHelper.
 */
public class MapperHelper {

   /**
    * Instantiates a new mapper helper.
    */
   private MapperHelper() {
      // do nothing!
   }

   /**
    * Creates the default mapper.
    *
    * @return the v pack mapper
    */
   public static VPackMapper createKGraphMapper() {
      final VPackMapper mapper = new VPackMapper();
      mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
      mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
      mapper.setVisibility(PropertyAccessor.FIELD, JsonAutoDetect.Visibility.ANY);

      final SimpleModule module = new ArangoModule();
      module.addSerializer(VPackSlice.class, VPackSerializers.VPACK);
      module.addSerializer(java.util.Date.class, VPackSerializers.UTIL_DATE);
      module.addSerializer(java.sql.Date.class, VPackSerializers.SQL_DATE);
      module.addSerializer(java.sql.Timestamp.class, VPackSerializers.SQL_TIMESTAMP);
      // module.addSerializer(BaseDocument.class, VPackSerializers.BASE_DOCUMENT);
      // module.addSerializer(BaseEdgeDocument.class,
      // VPackSerializers.BASE_EDGE_DOCUMENT);

      module.addSerializer(Node.class, NODE_SERIALIZER);
      module.addSerializer(Edge.class, EDGE_SERIALIZER);

      module.addDeserializer(VPackSlice.class, VPackDeserializers.VPACK);
      module.addDeserializer(java.util.Date.class, VPackDeserializers.UTIL_DATE);
      module.addDeserializer(java.sql.Date.class, VPackDeserializers.SQL_DATE);
      module.addDeserializer(java.sql.Timestamp.class, VPackDeserializers.SQL_TIMESTAMP);
      // module.addDeserializer(BaseDocument.class, VPackDeserializers.BASE_DOCUMENT);
      // module.addDeserializer(BaseEdgeDocument.class,
      // VPackDeserializers.BASE_EDGE_DOCUMENT);

      module.addDeserializer(Node.class, NODE_DESERIALIZER);
      module.addDeserializer(Edge.class, EDGE_DESERIALIZER);

      mapper.registerModule(module);
      return mapper;
   }

   /** The Constant EDGE_DESERIALIZER. */
   public static final JsonDeserializer<Edge> EDGE_DESERIALIZER = new JsonDeserializer<Edge>() {
      @SuppressWarnings("unchecked")
      @Override
      public Edge deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
         return new Edge(p.readValueAs(Map.class));
      }
   };

   /** The Constant NODE_DESERIALIZER. */
   public static final JsonDeserializer<Node> NODE_DESERIALIZER = new JsonDeserializer<Node>() {
      @SuppressWarnings("unchecked")
      @Override
      public Node deserialize(final JsonParser p, final DeserializationContext ctxt) throws IOException, JsonProcessingException {
         return new Node(p.readValueAs(Map.class));
      }
   };

   /** The Constant NODE_SERIALIZER. */
   public static final JsonSerializer<Node> NODE_SERIALIZER = new JsonSerializer<Node>() {
      @Override
      public void serialize(final Node value, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
         final Map<String, Object> doc = new HashMap<>();
         doc.putAll(value.getProperties());
         doc.put(DocumentField.Type.ID.getSerializeName(), value.getId());
         doc.put(DocumentField.Type.KEY.getSerializeName(), value.getKey());
         doc.put(DocumentField.Type.REV.getSerializeName(), value.getRevision());
         gen.writeObject(doc);
      }
   };

   /** The Constant EDGE_SERIALIZER. */
   public static final JsonSerializer<Edge> EDGE_SERIALIZER = new JsonSerializer<Edge>() {
      @Override
      public void serialize(final Edge value, final JsonGenerator gen, final SerializerProvider serializers) throws IOException {
         final Map<String, Object> doc = new HashMap<>();
         doc.putAll(value.getProperties());
         doc.put(DocumentField.Type.ID.getSerializeName(), value.getId());
         doc.put(DocumentField.Type.KEY.getSerializeName(), value.getKey());
         doc.put(DocumentField.Type.REV.getSerializeName(), value.getRevision());
         doc.put(DocumentField.Type.FROM.getSerializeName(), value.getFrom());
         doc.put(DocumentField.Type.TO.getSerializeName(), value.getTo());
         gen.writeObject(doc);
      }
   };

   /**
    * The Class ArangoModule.
    */
   private static final class ArangoModule extends SimpleModule {

      /** The Constant serialVersionUID. */
      private static final long serialVersionUID = 4570097748991257899L;

      @Override
      public void setupModule(SetupContext context) {
         super.setupModule(context);
         context.insertAnnotationIntrospector(new ArangoAnnotationIntrospector());
      }
   }
}
