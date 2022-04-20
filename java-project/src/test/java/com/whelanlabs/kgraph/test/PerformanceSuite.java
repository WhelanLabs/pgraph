package com.whelanlabs.kgraph.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.whelanlabs.kgraph.engine.ElementFactoryTest;
import com.whelanlabs.kgraph.engine.KnowledgeGraphTest2;
import com.whelanlabs.kgraph.test.engine.KnowledgeGraphTest;
import com.whelanlabs.kgraph.test.loader.StockDataLoaderTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({ StockDataLoaderTest.class })

public class PerformanceSuite {
}