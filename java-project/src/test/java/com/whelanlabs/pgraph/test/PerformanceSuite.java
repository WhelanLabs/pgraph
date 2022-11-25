package com.whelanlabs.pgraph.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.whelanlabs.pgraph.test.loader.StockDataLoaderTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({ StockDataLoaderTest.class })

public class PerformanceSuite {
}