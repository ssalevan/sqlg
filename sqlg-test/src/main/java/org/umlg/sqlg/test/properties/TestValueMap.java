package org.umlg.sqlg.test.properties;

import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.test.BaseTest;

import java.util.List;
import java.util.Map;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/12/30
 */
public class TestValueMap extends BaseTest {

    @Test
    public void g_V_valueMapXname_ageX() {
        loadModern();
        final Traversal<Vertex, Map<String, List>> traversal = this.sqlgGraph.traversal().V().valueMap("name", "age");

        printTraversalForm(traversal);
        int counter = 0;
        while (traversal.hasNext()) {
            counter++;
            final Map<String, List> values = traversal.next();
            final String name = (String) values.get("name").get(0);
            if (name.equals("marko")) {
                Assert.assertEquals(29, values.get("age").get(0));
                Assert.assertEquals(2, values.size());
            } else if (name.equals("josh")) {
                Assert.assertEquals(32, values.get("age").get(0));
                Assert.assertEquals(2, values.size());
            } else if (name.equals("peter")) {
                Assert.assertEquals(35, values.get("age").get(0));
                Assert.assertEquals(2, values.size());
            } else if (name.equals("vadas")) {
                Assert.assertEquals(27, values.get("age").get(0));
                Assert.assertEquals(2, values.size());
            } else if (name.equals("lop")) {
                Assert.assertNull(values.get("lang"));
                Assert.assertEquals(1, values.size());
            } else if (name.equals("ripple")) {
                Assert.assertNull(values.get("lang"));
                Assert.assertEquals(1, values.size());
            } else {
                throw new IllegalStateException("It is not possible to reach here: " + values);
            }
        }
        Assert.assertEquals(6, counter);
    }
}
