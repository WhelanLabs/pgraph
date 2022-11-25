package com.whelanlabs.pgraph.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.whelanlabs.pgraph.engine.ElementFactoryTest;
import com.whelanlabs.pgraph.engine.PropertyGraphTest2;
import com.whelanlabs.pgraph.test.engine.PropertyGraphTest;

@RunWith(Suite.class)

@Suite.SuiteClasses({ PropertyGraphTest.class, PropertyGraphTest2.class, ElementFactoryTest.class })

public class FunctionalSuite {
}