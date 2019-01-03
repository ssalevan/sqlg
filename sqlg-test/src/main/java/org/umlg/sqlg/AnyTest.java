package org.umlg.sqlg;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.umlg.sqlg.test.sharding.TestSharding;

/**
 * Date: 2014/07/16
 * Time: 12:10 PM
 */
@RunWith(Suite.class)
@Suite.SuiteClasses({
        TestSharding.class,
//        TestReducing.class,
//        TestReducingVertexStep.class,
//        TestReducingUserSuppliedIds.class,
//        TestSimpleJoinGremlin.class,
//        TestMultipleIDQuery.class,
//        TestSimpleVertexEdgeGremlin.class,
//        TestUserSuppliedPKBulkMode.class,
//        TestUserSuppliedPKTopology.class
//        TestVertexStepOrderBy.class
})
public class AnyTest {
}
