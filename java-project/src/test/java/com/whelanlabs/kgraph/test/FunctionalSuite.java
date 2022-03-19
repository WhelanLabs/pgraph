package com.whelanlabs.kgraph.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.whelanlabs.kgraph.test.engine.KnowledgeGraphTest;
import com.whelanlabs.kgraph.test.test.engine.QueryClauseTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({ KnowledgeGraphTest.class, QueryClauseTest.class })

public class FunctionalSuite {
}