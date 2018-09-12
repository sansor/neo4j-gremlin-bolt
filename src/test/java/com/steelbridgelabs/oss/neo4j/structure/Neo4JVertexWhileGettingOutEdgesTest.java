/*
 *  Copyright 2016 SteelBridge Laboratories, LLC.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *  http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 *  For more information: http://steelbridgelabs.com
 */

package com.steelbridgelabs.oss.neo4j.structure;

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.types.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.stream.Stream;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JVertexWhileGettingOutEdgesTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Transaction transaction;

    @Mock
    private Neo4JSession session;

    @Mock
    private Neo4JReadPartition partition;

    @Mock
    private Node node;

    @Mock
    private Neo4JElementIdProvider vertexIdProvider;

    @Mock
    private Neo4JElementIdProvider edgeIdProvider;

    @Mock
    private Graph.Features.VertexFeatures vertexFeatures;

    @Mock
    private Graph.Features features;

    @Mock
    private Neo4JVertex otherVertex;

    @Mock
    private Neo4JEdge edge1;

    @Mock
    private Neo4JEdge edge2;

    @Mock
    private StatementResult statementResult;

    @Mock
    private ResultSummary resultSummary;

    @Test
    public void givenNoLabelsShouldGetVertices() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`l1`)-[r]->(m) WHERE n.id = {id} RETURN n, r, m", Collections.singletonMap("id", 1L))))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenNoLabelsShouldGetDatabaseAndTransientVertices() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`l1`)-[r]->(m) WHERE n.id = {id} AND NOT r.id IN {ids} RETURN n, r, m", parameters)))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertTrue("Edges iterator does not contain two elements", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenLabelShouldGetDatabaseAndTransientVertices() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL");
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`l1`)-[r:`EL`]->(m) WHERE n.id = {id} AND NOT r.id IN {ids} RETURN n, r, m", parameters)))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertTrue("Edges iterator does not contain two elements", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenLabelsShouldGetDatabaseAndTransientVertices() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`l1`)-[r:`EL2`|:`EL1`]->(m) WHERE n.id = {id} AND NOT r.id IN {ids} RETURN n, r, m", parameters)))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL1", "EL2");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertTrue("Edges iterator does not contain two elements", edges.hasNext());
    }

    @Test
    public void givenLabelShouldGetDatabaseVertices() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`l1`)-[r:`EL1`]->(m) WHERE n.id = {id} AND NOT r.id IN {ids} RETURN n, r, m", parameters)))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL1");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertFalse("Edges iterator cannot not contain two elements", edges.hasNext());
    }

    @Test
    public void givenLabelShouldGetTransientVertices() {
        // arrange
        Map<String, Object> parameters = new HashMap<>();
        parameters.put("id", 1L);
        parameters.put("ids", Collections.singletonList(200L));
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge1.id()).thenAnswer(invocation -> 100L);
        Mockito.when(edge1.label()).thenAnswer(invocation -> "EL1");
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`l1`)-[r:`EL2`]->(m) WHERE n.id = {id} AND NOT r.id IN {ids} RETURN n, r, m", parameters)))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Stream.empty());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL2");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertFalse("Edges iterator cannot not contain two elements", edges.hasNext());
    }

    @Test
    public void givenNoLabelsAndPartitionMatchPatternShouldGetVertices() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(partition.vertexMatchPatternLabels()).thenAnswer(invocation -> new HashSet<>(Arrays.asList("P1", "P2")));
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Arrays.asList("l1", "P1", "P2"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`P1`:`P2`:`l1`)-[r]->(m:`P1`:`P2`) WHERE n.id = {id} RETURN n, r, m", Collections.singletonMap("id", 1L))))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenNoLabelsAndPartitionMatchPredicateShouldGetVertices() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(partition.vertexMatchPatternLabels()).thenAnswer(invocation -> Collections.emptySet());
        Mockito.when(partition.vertexMatchPredicate(Mockito.eq("m"))).thenAnswer(invocation -> "(m:`P1` OR m:`P2`)");
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Arrays.asList("l1", "P1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(session.executeStatement(Mockito.eq(new Statement("MATCH (n:`P1`:`l1`)-[r]->(m) WHERE n.id = {id} AND (m:`P1` OR m:`P2`) RETURN n, r, m", Collections.singletonMap("id", 1L))))).thenAnswer(invocation -> statementResult);
        Mockito.when(session.edges(Mockito.eq(statementResult))).thenAnswer(invocation -> Collections.singleton(edge1).stream());
        Mockito.when(statementResult.consume()).thenAnswer(invocation -> resultSummary);
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
    }

    @Test
    public void givenTransientVertexAndNoLabelsShouldGetTransientVertices() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, Collections.singletonList("l1"));
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT);
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertFalse("Edges iterator cannot not contain two elements", edges.hasNext());
    }

    @Test
    public void givenTransientVertexAndLabelsShouldGetTransientVertices() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, Collections.singletonList("l1"));
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL2");
        // assert
        Assert.assertNotNull("Failed to get edge iterator", edges);
        Assert.assertTrue("Edges iterator is empty", edges.hasNext());
        Assert.assertNotNull("Failed to get edge", edges.next());
        Assert.assertFalse("Edges iterator cannot not contain two elements", edges.hasNext());
    }

    @Test
    public void givenTransientVertexAndLabelsShouldGetEmptyIterator() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(edge2.id()).thenAnswer(invocation -> 200L);
        Mockito.when(edge2.label()).thenAnswer(invocation -> "EL2");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, Collections.singletonList("l1"));
        vertex.addOutEdge(edge2);
        // act
        Iterator<Edge> edges = vertex.edges(Direction.OUT, "EL1");
        // assert
        Assert.assertFalse("Edges iterator should be empty", edges.hasNext());
    }
}
