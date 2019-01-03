package org.umlg.sqlg.test.reducing;

import org.apache.commons.collections4.set.ListOrderedSet;
import org.apache.tinkerpop.gremlin.process.traversal.Traversal;
import org.apache.tinkerpop.gremlin.process.traversal.dsl.graph.__;
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.umlg.sqlg.structure.PropertyType;
import org.umlg.sqlg.structure.topology.VertexLabel;
import org.umlg.sqlg.test.BaseTest;

import java.util.*;

/**
 * @author Pieter Martin (https://github.com/pietermartin)
 * Date: 2018/12/31
 */
public class TestReducingVertexStepUserSuppliedIds extends BaseTest {

    @Test
    public void testGroupOverOnePropertyMax() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("B", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        this.sqlgGraph.tx().commit();
        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b1", "age", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b1", "age", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 4);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 1);
        Vertex b21 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 5);
        Vertex b22 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 6);
        Vertex b23 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 7);
        Vertex b24 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 8);

        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("ab", b4);

        a2.addEdge("ab", b21);
        a2.addEdge("ab", b22);
        a2.addEdge("ab", b23);
        a2.addEdge("ab", b24);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = this.sqlgGraph.traversal()
                .V().has("name", __.or(__.is("a1"), __.is("a2"))).out()
                .<String, Integer>group().by("name").by(__.values("age").max());

        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("b1"));
        Assert.assertTrue(result.containsKey("b2"));
        Assert.assertEquals(2, result.get("b1"), 0);
        Assert.assertEquals(8, result.get("b2"), 0);
    }

    @Test
    public void testGroupOverOnePropertyLocalMax() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        VertexLabel bVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("B", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        aVertexLabel.ensureEdgeLabelExist("ab", bVertexLabel);
        this.sqlgGraph.tx().commit();


        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex b1 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b1", "age", 1);
        Vertex b2 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b1", "age", 2);
        Vertex b3 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 3);
        Vertex b4 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b2", "age", 4);

        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 1);
        Vertex b21 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b21", "age", 1);
        Vertex b22 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b21", "age", 2);
        Vertex b23 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b22", "age", 3);
        Vertex b24 = this.sqlgGraph.addVertex(T.label, "B", "uid", UUID.randomUUID().toString(), "name", "b22", "age", 4);

        a1.addEdge("ab", b1);
        a1.addEdge("ab", b2);
        a1.addEdge("ab", b3);
        a1.addEdge("ab", b4);

        a2.addEdge("ab", b21);
        a2.addEdge("ab", b22);
        a2.addEdge("ab", b23);
        a2.addEdge("ab", b24);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("A")
                .local(
                        __.out().<String, Integer>group().by("name").by(__.values("age").max())
                );

        printTraversalForm(traversal);
        List<Map<String, Integer>> result = traversal.toList();
        Assert.assertEquals(2, result.size());
        for (int i = 0; i < 2; i++) {
            if (result.get(i).containsKey("b1") && result.get(i).containsKey("b2")) {
                Assert.assertEquals(2, result.get(i).get("b1"), 0);
                Assert.assertEquals(4, result.get(i).get("b2"), 0);
            } else if (result.get(i).containsKey("b21") && result.get(i).containsKey("b22")) {
                Assert.assertEquals(2, result.get(i).get("b21"), 0);
                Assert.assertEquals(4, result.get(i).get("b22"), 0);
            } else {
                Assert.fail();
            }
        }

    }

    @Test
    public void g_VX1_2X_unionXoutE_count__inE_count__outE_weight_sumX() {
        loadModernUserSuppliedIds();
        @SuppressWarnings("unchecked") final Traversal<Vertex, Number> traversal = this.sqlgGraph.traversal()
                .V(convertToVertexId("marko"), convertToVertexId("vadas"))
                .union(
                        __.outE().count(),
                        __.inE().count(),
                        (Traversal) __.outE().values("weight").sum()
                );
        printTraversalForm(traversal);
        checkResults(Arrays.asList(3L, 1.9D, 1L), traversal);
    }

}
