package com.whelanlabs.kgraph.test.test.engine;

import static org.junit.Assert.assertNotNull;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.whelanlabs.kgraph.engine.QueryClause;
import com.whelanlabs.kgraph.engine.StringQueryValue;

public class QueryClauseTest {

   @BeforeClass
   public static void setUpBeforeClass() throws Exception {
   }

   @AfterClass
   public static void tearDownAfterClass() throws Exception {
   }

   @Test
   public void constructor_validValues_ok() {
      QueryClause queryClause = new QueryClause("name", QueryClause.Operator.EQUALS, new StringQueryValue("Alice"));
      assertNotNull(queryClause);
      // fail("Not yet implemented");
   }

}
