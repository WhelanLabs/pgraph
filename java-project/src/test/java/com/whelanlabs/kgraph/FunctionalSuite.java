package com.whelanlabs.kgraph;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.whelanlabs.kgraph.engine.KnowledgeGraphTest;
import com.whelanlabs.kgraph.loader.StockDataLoaderTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({ KnowledgeGraphTest.class })

public class FunctionalSuite {
}