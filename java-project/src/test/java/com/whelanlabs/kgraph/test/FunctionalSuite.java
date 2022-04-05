package com.whelanlabs.kgraph.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.whelanlabs.kgraph.engine.ElementFactoryTest;
import com.whelanlabs.kgraph.engine.KnowledgeGraphTest2;
import com.whelanlabs.kgraph.test.engine.KnowledgeGraphTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({ KnowledgeGraphTest.class, KnowledgeGraphTest2.class, ElementFactoryTest.class})

public class FunctionalSuite {
}