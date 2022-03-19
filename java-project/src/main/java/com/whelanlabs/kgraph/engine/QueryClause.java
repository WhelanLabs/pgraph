package com.whelanlabs.kgraph.engine;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueryClause {

   private static Logger logger = LogManager.getLogger(QueryClause.class);

   public enum Operator {
      LESS_THAN(" < "), GREATER_THAN(" > "), EQUALS(" == "), LESS_THAN_OR_EQUALS(" <= "), GREATER_THAN_OR_EQUALS(" >= "), NOT_EQUALS(" != ");

      private final String name;

      private Operator(String s) {
         name = s;
      }

      public String toAQL() {
         return this.name;
      }
   }

   private String _name;
   private Operator _op;
   private QueryValue _value;
   private String space = " ";

   public QueryClause(String name, QueryClause.Operator op, QueryValue value) {
      _name = name;
      _op = op;
      _value = value;
      logger.debug("new QueryClause = '" + toAQL() + "'");
   }

   public String toAQL() {
      String result = _name + space + _op.toAQL() + space + _value.toAQL();
      return result;
   }

   public Object getValue() {
      return _value;
   }

   public String getName() {
      return _name;
   }
}
