package com.whelanlabs.kgraph.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.whelanlabs.kgraph.test.engine.KnowledgeGraphTest;
import com.whelanlabs.kgraph.test.loader.StockDataLoaderTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({ KnowledgeGraphTest.class, StockDataLoaderTest.class })

public class AllSuite {
}