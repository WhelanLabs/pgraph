package com.whelanlabs.kgraph.engine;

public class StringQueryValue implements QueryValue {

   private Object _value;
   String quote = "\"";

   public StringQueryValue(Object value) {
      _value = value;
   }

   @Override
   public String toAQL() {
      return quote + _value + quote;
   }
}
