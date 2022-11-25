package com.whelanlabs.pgraph.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The Class QueryClause.
 */
public class QueryClause {

   /** The logger. */
   private static Logger logger = LogManager.getLogger(QueryClause.class);

   /**
    * The Enum Operator.  This contains AQL string values for operators.
    */
   public enum Operator {

      /** The less than. */
      LESS_THAN(" < "),
      /** The greater than. */
      GREATER_THAN(" > "),
      /** The equals. */
      EQUALS(" == "),
      /** The less than or equals. */
      LESS_THAN_OR_EQUALS(" <= "),
      /** The greater than or equals. */
      GREATER_THAN_OR_EQUALS(" >= "),
      /** The not equals. */
      NOT_EQUALS(" != ");

      /** The name. */
      private final String name;

      /**
       * Instantiates a new operator.
       *
       * @param s the s
       */
      private Operator(String s) {
         name = s;
      }

      /**
       * To AQL.
       *
       * @return the string
       */
      public String toAQL() {
         return this.name;
      }
   }

   /** The name of the property for the query clause. */
   private String _name;

   /** The op. */
   private Operator _op;

   /** The value of the property for the query clause. */
   private Object _value;

   /** The at symbol. */
   private String atSymbol = "@";

   /**
    * Instantiates a new query clause.
    *
    * @param name the name
    * @param op the op
    * @param value the value
    */
   public QueryClause(String name, QueryClause.Operator op, Object value) {
      _name = name;
      _op = op;
      _value = value;
      logger.debug("new QueryClause = '" + _name + _op.toAQL() + _value + "'");
   }

   /**
    * To AQL.  Generates a AQL string for the given query clause.
    *
    * @return the string
    */
   public String toAQL() {
      String result = _name + _op.toAQL() + atSymbol + _name;
      return result;
   }

   /**
    * Gets the value.
    *
    * @return the value
    */
   public Object getValue() {
      return _value;
   }

   /**
    * Gets the name.
    *
    * @return the name
    */
   public String getName() {
      return _name;
   }
}
