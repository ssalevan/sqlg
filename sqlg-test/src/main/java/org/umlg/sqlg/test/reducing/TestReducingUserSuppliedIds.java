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
 * Date: 2018/11/17
 */
@SuppressWarnings("Duplicates")
public class TestReducingUserSuppliedIds extends BaseTest {

    @SuppressWarnings("Duplicates")
    @Test
    public void testMax() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p2", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p3", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p4", "age", 0);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().hasLabel("Person").values("age").max();
        printTraversalForm(traversal);
        Assert.assertEquals(3, traversal.next(), 0);
    }

    @SuppressWarnings("Duplicates")
    @Test
    public void testMin() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p2", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p3", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p4", "age", 0);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().hasLabel("Person").values("age").min();
        printTraversalForm(traversal);
        Assert.assertEquals(0, traversal.next(), 0);
    }

    @Test
    public void testSum() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p2", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p3", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p4", "age", 0);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Long> traversal = this.sqlgGraph.traversal().V().hasLabel("Person").values("age").sum();
        printTraversalForm(traversal);
        Assert.assertEquals(6, traversal.next(), 0);
    }

    @Test
    public void testMean() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p2", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p3", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "p4", "age", 0);
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Double> traversal = this.sqlgGraph.traversal().V().hasLabel("Person").values("age").mean();
        printTraversalForm(traversal);
        Assert.assertEquals(1.5, traversal.next(), 0D);
    }

    @Test
    public void testGroupOverOnePropertyMax() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").max());
        printTraversalForm(traversal);
        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(3, result.get("A"), 0);
        Assert.assertEquals(4, result.get("B"), 0);
    }

    @Test
    public void testGroupOverOnePropertyMin() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").min());
        printTraversalForm(traversal);
        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(1, result.get("A"), 0);
        Assert.assertEquals(2, result.get("B"), 0);
    }

    @Test
    public void testGroupOverOnePropertySum() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Integer>group().by("name").by(__.values("age").sum());
        printTraversalForm(traversal);
        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(4.0, result.get("A"), 0L);
        Assert.assertEquals(6.0, result.get("B"), 0L);
    }

    @Test
    public void testGroupOverOnePropertyMean() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 4);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Double>> traversal = sqlgGraph.traversal()
                .V().hasLabel("Person")
                .<String, Double>group().by("name").by(__.values("age").mean());
        printTraversalForm(traversal);
        Map<String, Double> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("B"));
        Assert.assertEquals(2.0, result.get("A"), 0D);
        Assert.assertEquals(3.0, result.get("B"), 0D);
    }

    @Test
    public void testGroupOverTwoPropertiesWithValues() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("surname", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<List<String>, Integer>> traversal = this.sqlgGraph.traversal().V().hasLabel("Person")
                .<List<String>, Integer>group()
                .by(__.values("name", "surname").fold())
                .by(__.values("age").max());

        printTraversalForm(traversal);

        Map<List<String>, Integer> result = traversal.next();
        System.out.println(result);

        Assert.assertTrue(result.containsKey(Arrays.asList("A", "C")) || result.containsKey(Arrays.asList("C", "A")));
        Assert.assertTrue(result.containsKey(Arrays.asList("B", "D")) || result.containsKey(Arrays.asList("D", "B")));
        Assert.assertTrue(result.containsKey(Arrays.asList("B", "E")) || result.containsKey(Arrays.asList("E", "B")));
        Assert.assertTrue(result.containsKey(Arrays.asList("C", "E")) || result.containsKey(Arrays.asList("E", "C")));
        Assert.assertEquals(4, result.size());
        Assert.assertFalse(traversal.hasNext());

        if (result.containsKey(Arrays.asList("A", "C"))) {
            Assert.assertEquals(3, result.get(Arrays.asList("A", "C")), 0);
        } else {
            Assert.assertEquals(3, result.get(Arrays.asList("C", "A")), 0);
        }
        if (result.containsKey(Arrays.asList("B", "D"))) {
            Assert.assertEquals(2, result.get(Arrays.asList("B", "D")), 0);
        } else {
            Assert.assertEquals(2, result.get(Arrays.asList("D", "B")), 0);
        }
        if (result.containsKey(Arrays.asList("B", "E"))) {
            Assert.assertEquals(4, result.get(Arrays.asList("B", "E")), 0);
        } else {
            Assert.assertEquals(4, result.get(Arrays.asList("E", "B")), 0);
        }
        if (result.containsKey(Arrays.asList("C", "E"))) {
            Assert.assertEquals(5, result.get(Arrays.asList("C", "E")), 0);
        } else {
            Assert.assertEquals(5, result.get(Arrays.asList("E", "C")), 0);
        }
    }

    @SuppressWarnings("ArraysAsListWithZeroOrOneArgument")
    @Test
    public void testGroupOverTwoPropertiesWithValueMap() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("surname", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "surname", "D", "age", 2);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "surname", "C", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "surname", "E", "age", 4);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "C", "surname", "E", "age", 5);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<Map<String, List<String>>, Integer>> traversal = this.sqlgGraph.traversal().V().hasLabel("Person")
                .<Map<String, List<String>>, Integer>group()
                .by(__.valueMap("name", "surname"))
                .by(__.values("age").max());

        printTraversalForm(traversal);

        Map<Map<String, List<String>>, Integer> result = traversal.next();
        System.out.println(result);

        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("C"));
            put("name", Arrays.asList("A"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("D"));
            put("name", Arrays.asList("B"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("B"));
        }}));
        Assert.assertTrue(result.containsKey(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("C"));
        }}));

        Assert.assertEquals(3, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("C"));
            put("name", Arrays.asList("A"));
        }}), 0);
        Assert.assertEquals(2, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("D"));
            put("name", Arrays.asList("B"));
        }}), 0);
        Assert.assertEquals(4, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("B"));
        }}), 0);
        Assert.assertEquals(5, result.get(new HashMap<String, List<String>>() {{
            put("surname", Arrays.asList("E"));
            put("name", Arrays.asList("C"));
        }}), 0);
    }

    @Test
    public void testGroupOverOnePropertyWithJoin() {
        VertexLabel personVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        VertexLabel addressVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("Address", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("year", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        personVertexLabel.ensureEdgeLabelExist("livesAt", addressVertexLabel, new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));

        this.sqlgGraph.tx().commit();
        Vertex person = this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A");
        Vertex address1 = this.sqlgGraph.addVertex(T.label, "Address", "uid", UUID.randomUUID().toString(), "name", "A", "year", 2);
        Vertex address2 = this.sqlgGraph.addVertex(T.label, "Address", "uid", UUID.randomUUID().toString(), "name", "A", "year", 4);
        Vertex address3 = this.sqlgGraph.addVertex(T.label, "Address", "uid", UUID.randomUUID().toString(), "name", "C", "year", 6);
        Vertex address4 = this.sqlgGraph.addVertex(T.label, "Address", "uid", UUID.randomUUID().toString(), "name", "D", "year", 8);
        Vertex address5 = this.sqlgGraph.addVertex(T.label, "Address", "uid", UUID.randomUUID().toString(), "name", "D", "year", 7);
        Vertex address6 = this.sqlgGraph.addVertex(T.label, "Address", "uid", UUID.randomUUID().toString(), "name", "D", "year", 6);
        person.addEdge("livesAt", address1, "uid", UUID.randomUUID().toString());
        person.addEdge("livesAt", address2, "uid", UUID.randomUUID().toString());
        person.addEdge("livesAt", address3, "uid", UUID.randomUUID().toString());
        person.addEdge("livesAt", address4, "uid", UUID.randomUUID().toString());
        person.addEdge("livesAt", address5, "uid", UUID.randomUUID().toString());
        person.addEdge("livesAt", address6, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = this.sqlgGraph.traversal()
                .V().hasLabel("Person")
                .out("livesAt")
                .<String, Integer>group()
                .by("name")
                .by(__.values("year").max());

        printTraversalForm(traversal);

        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(3, result.size());
        Assert.assertTrue(result.containsKey("A"));
        Assert.assertTrue(result.containsKey("C"));
        Assert.assertTrue(result.containsKey("D"));
        Assert.assertEquals(4, result.get("A"), 0);
        Assert.assertEquals(6, result.get("C"), 0);
        Assert.assertEquals(8, result.get("D"), 0);
    }

    @Test
    public void testGroupByLabel() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Dog", new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "A", "age", 10);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "B", "age", 20);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "C", "age", 100);
        this.sqlgGraph.addVertex(T.label, "Person", "uid", UUID.randomUUID().toString(), "name", "D", "age", 40);

        this.sqlgGraph.addVertex(T.label, "Dog", "uid", UUID.randomUUID().toString(), "name", "A", "age", 10);
        this.sqlgGraph.addVertex(T.label, "Dog", "uid", UUID.randomUUID().toString(), "name", "B", "age", 200);
        this.sqlgGraph.addVertex(T.label, "Dog", "uid", UUID.randomUUID().toString(), "name", "C", "age", 30);
        this.sqlgGraph.addVertex(T.label, "Dog", "uid", UUID.randomUUID().toString(), "name", "D", "age", 40);

        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Map<String, Integer>> traversal = this.sqlgGraph.traversal().V().<String, Integer>group().by(T.label).by(__.values("age").max());
        printTraversalForm(traversal);

        Map<String, Integer> result = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(2, result.size());
        Assert.assertTrue(result.containsKey("Person"));
        Assert.assertTrue(result.containsKey("Dog"));
        Assert.assertEquals(100, result.get("Person"), 0);
        Assert.assertEquals(200, result.get("Dog"), 0);
    }

    @Test
    public void testDuplicatePathQuery() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel, new HashMap<String, PropertyType>() {{
            put("uid", PropertyType.STRING);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("uid")));
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a2", "age", 5);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a3", "age", 7);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "uid", UUID.randomUUID().toString(), "name", "a4", "age", 5);
        a1.addEdge("aa", a2, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a3, "uid", UUID.randomUUID().toString());
        a1.addEdge("aa", a4, "uid", UUID.randomUUID().toString());
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V(a1).out("aa").values("age").max();
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Integer max = traversal.next();
        Assert.assertEquals(7, max, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testDuplicatePathQuery2() {
        VertexLabel aVertexLabel = this.sqlgGraph.getTopology().ensureVertexLabelExist("A", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));

        aVertexLabel.ensureEdgeLabelExist("aa", aVertexLabel);
        this.sqlgGraph.tx().commit();

        Vertex a1 = this.sqlgGraph.addVertex(T.label, "A", "name", "a1", "age", 1);
        Vertex a2 = this.sqlgGraph.addVertex(T.label, "A", "name", "a2", "age", 5);
        Vertex a3 = this.sqlgGraph.addVertex(T.label, "A", "name", "a3", "age", 7);
        Vertex a4 = this.sqlgGraph.addVertex(T.label, "A", "name", "a4", "age", 5);
        a1.addEdge("aa", a2);
        a1.addEdge("aa", a3);
        a1.addEdge("aa", a4);
        a2.addEdge("aa", a1);
        a2.addEdge("aa", a3);
        this.sqlgGraph.tx().commit();

        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V(a1).out("aa").out("aa").values("age").max();
        printTraversalForm(traversal);
        Assert.assertTrue(traversal.hasNext());
        Integer max = traversal.next();
        Assert.assertEquals(7, max, 0);
        Assert.assertFalse(traversal.hasNext());
    }

    @Test
    public void testMaxAgain() {
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Person", new HashMap<String, PropertyType>() {{
            put("name", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("name")));
        this.sqlgGraph.getTopology().ensureVertexLabelExist("Dog", new HashMap<String, PropertyType>() {{
            put("dog", PropertyType.STRING);
            put("age", PropertyType.INTEGER);
        }}, ListOrderedSet.listOrderedSet(Collections.singletonList("dog")));
        this.sqlgGraph.tx().commit();

        this.sqlgGraph.addVertex(T.label, "Person", "name", "a1", "age", 1);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a2", "age", 3);
        this.sqlgGraph.addVertex(T.label, "Person", "name", "a3", "age", 5);
        this.sqlgGraph.addVertex(T.label, "Dog", "dog", "d1", "age", 7);
        this.sqlgGraph.tx().commit();
        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().values("age").max();
        printTraversalForm(traversal);
        Integer max = traversal.next();
        Assert.assertFalse(traversal.hasNext());
        Assert.assertEquals(7, max, 0);
    }

    @Test
    public void testMax2() {
        loadModernUserSuppliedIds();
        Traversal<Vertex, Integer> traversal = this.sqlgGraph.traversal().V().values("age").max();
        printTraversalForm(traversal);
        checkResults(Arrays.asList(35), traversal);
    }

}
