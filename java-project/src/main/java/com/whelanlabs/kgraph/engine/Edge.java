package com.whelanlabs.kgraph.engine;

import java.util.Map;
import java.util.Set;

import org.json.JSONObject;

import com.arangodb.entity.BaseEdgeDocument;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter; 

/**
 * The Class Edge.
 */
public class Edge extends BaseEdgeDocument implements Element {

   /** The Constant serialVersionUID. */
   private static final long serialVersionUID = -4801740400388403434L;

   /** The Constant leftTypeAttrName. Identifies the collection for the left
    *  end-point of an edge.*/
   public static final String leftTypeAttrName = "__left_collection";

   /** The Constant rightTypeAttrName. Identifies the collection for the right
    *  end-point of an edge.*/
   public static final String rightTypeAttrName = "__right_collection";

   /** The Constant edgeTypeAttrName. Identifies the collection for a
    *  given edge.  Also used in
    *  end-point of an edge.*/
   public static final String edgeTypeAttrName = "__edge_collection";

   /**
    * Instantiates a new edge.
    * 
    * Periodic errors may occur if using non-persisted nodes.
    *
    * @param edgeKey the edge key
    * @param left the left
    * @param right the right
    * @param type the type
    */
   public Edge(String edgeKey, Node left, Node right, String type) {
      super(edgeKey, left.getId(), right.getId());
      this.addAttribute(typeAttrName, type);
      String leftCollectionName = left.getType();
      String rightCollectionName = right.getType();
      this.addAttribute(leftTypeAttrName, leftCollectionName);
      this.addAttribute(rightTypeAttrName, rightCollectionName);
   }

   public Edge(String edgeKey, String leftKey, String rightKey, String leftType, String rightType, String type) {
      super(edgeKey, leftType + "/" + leftKey, rightType + "/" + rightKey);
      this.addAttribute(typeAttrName, type);
      this.addAttribute(leftTypeAttrName, leftType);
      this.addAttribute(rightTypeAttrName, rightType);
   }
   
   /**
    * Instantiates a new edge.
    * 
    * This constructor is largely intended for use in deserializing
    * objects, not for initial construction by calling applications.
    *
    * @param properties the properties for the created edge
    */
   protected Edge(final Map<String, Object> properties) {
      super(properties);
   }


   public String getLeftType() {
      return getAttribute(leftTypeAttrName).toString();
   }

   public String getRightType() {
      return getAttribute(rightTypeAttrName).toString();
   }

   @Override
   public String toJson() throws Exception {
      ObjectWriter ow = new ObjectMapper().writer().withDefaultPrettyPrinter();
      String json = ow.writeValueAsString(this);
      return json;
   }

   @Override
   public String toDot() {
// edgeId  [shape=oval label="one\ltwo three\lfour five six seven\l"]
// leftId -> edgeId -> rightId
      StringBuilder s = new StringBuilder();
      s.append(getId().replace("/", "_") + " [shape=oval label=\"");
      s.append("type = \\\"" + getType() + "\\\"\\l");
      s.append("id = \\\"" + getId() + "\\\"\\l");
      for(String key : this.getProperties().keySet()) {
         if("__type"!=key && "__left_collection"!=key && "__right_collection"!=key) {
            Object value = getAttribute(key);
            if(value instanceof String) {
               s.append(key + " = \\\"" + getAttribute(key) + "\\\"\\l");
            }
            else {
               s.append(key + " = " + getAttribute(key) + "\\l");
            }
         }
      }
      s.append("\"]\n");
      s.append(getFrom().replace("/", "_") + " -> " + getId().replace("/", "_") + " -> " +  getTo().replace("/", "_") + "\n");
      return s.toString();
   }

   public static Edge hydrate(JSONObject jsonObj) {
      
      String edgeKey = jsonObj.getString("_key");
      String leftId = jsonObj.getString("_from");
      String rightId = jsonObj.getString("_to");
      String leftType = jsonObj.getString("leftType");
      String rightType = jsonObj.getString("rightType");
      String type = jsonObj.getString("type");
      
      // TODO: this is a bit of a hack, but for now just trying to get the POC working.
      leftId = leftId.split("/")[1];
      rightId = rightId.split("/")[1];
      
      Edge result = new Edge(edgeKey, leftId, rightId, leftType, rightType, type);

      JSONObject props = jsonObj.getJSONObject("properties");
      Set<String> propsKeySet = props.keySet();
      for( String propKey : propsKeySet){
         Object propValue = props.get(propKey);
         result.addAttribute(propKey, propValue);
     }

      return result;
   }
   
}
