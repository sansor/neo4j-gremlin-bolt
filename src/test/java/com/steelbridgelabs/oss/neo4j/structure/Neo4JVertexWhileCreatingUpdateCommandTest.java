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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Node;

import java.util.Arrays;
import java.util.Collections;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JVertexWhileCreatingUpdateCommandTest {

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
    private Neo4JVertex vertex1;

    @Mock
    private Neo4JVertex vertex2;

    @Mock
    private Neo4JEdge edge2;

    @Mock
    private StatementResult statementResult;

    @Mock
    private Record record;

    @Mock
    private Entity entity;

    @Mock
    private Value value;

    @Mock
    private ResultSummary resultSummary;

    @Test
    public void givenNotDirtyNodeShouldReturnNull() {
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
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        // act
        Neo4JDatabaseCommand command = vertex.updateCommand();
        // assert
        Assert.assertNull("Failed to create update command", command);
    }

    @Test
    public void givenNewPropertyShouldCreateUpdateCommand() {
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
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.property("key2", "value2");
        // act
        Neo4JDatabaseCommand command = vertex.updateCommand();
        // assert
        Assert.assertNotNull("Failed to create update command", command);
        Assert.assertNotNull("Failed to create update command statement", command.getStatement());
        Assert.assertEquals("Invalid update command statement", command.getStatement().text(), "MATCH (v:`l1`) WHERE n.id = {id} SET v = {vp}");
        Assert.assertEquals("Invalid update command statement", command.getStatement().parameters(), Values.parameters("id", 1L, "vp", Values.parameters("key1", "value1", "key2", "value2", "id", 1L)));
        Assert.assertNotNull("Failed to create update command callback", command.getCallback());
        // invoke callback
        command.getCallback().accept(statementResult);
    }

    @Test
    public void givenNewLabelShouldCreateUpdateCommand() {
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
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.addLabel("Test");
        // act
        Neo4JDatabaseCommand command = vertex.updateCommand();
        // assert
        Assert.assertNotNull("Failed to create update command", command);
        Assert.assertNotNull("Failed to create update command statement", command.getStatement());
        Assert.assertEquals("Invalid update command statement", command.getStatement().text(), "MATCH (v:`l1`) WHERE n.id = {id} SET v:`Test`");
        Assert.assertEquals("Invalid update command statement", command.getStatement().parameters(), Values.parameters("id", 1L));
        Assert.assertNotNull("Failed to create update command callback", command.getCallback());
        // invoke callback
        command.getCallback().accept(statementResult);
    }

    @Test
    public void givenRemoveLabelShouldCreateUpdateCommand() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.anyString())).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Arrays.asList("l1", "l2"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.removeLabel("l2");
        // act
        Neo4JDatabaseCommand command = vertex.updateCommand();
        // assert
        Assert.assertNotNull("Failed to create update command", command);
        Assert.assertNotNull("Failed to create update command statement", command.getStatement());
        Assert.assertEquals("Invalid update command statement", command.getStatement().text(), "MATCH (v:`l1`:`l2`) WHERE n.id = {id} REMOVE v:`l2`");
        Assert.assertEquals("Invalid update command statement", command.getStatement().parameters(), Values.parameters("id", 1L));
        Assert.assertNotNull("Failed to create update command callback", command.getCallback());
        // invoke callback
        command.getCallback().accept(statementResult);
    }

    @Test
    public void givenRemovePropertyShouldCreateUpdateCommand() {
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
        Mockito.when(vertexIdProvider.get(Mockito.any())).thenAnswer(invocation -> 1L);
        Mockito.when(vertexIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(vertexIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "n.id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, vertexIdProvider, edgeIdProvider, node);
        vertex.property("key1").remove();
        // act
        Neo4JDatabaseCommand command = vertex.updateCommand();
        // assert
        Assert.assertNotNull("Failed to create update command", command);
        Assert.assertNotNull("Failed to create update command statement", command.getStatement());
        Assert.assertEquals("Invalid update command statement", command.getStatement().text(), "MATCH (v:`l1`) WHERE n.id = {id} SET v = {vp}");
        Assert.assertEquals("Invalid update command statement", command.getStatement().parameters(), Values.parameters("id", 1L, "vp", Values.parameters("key1", null, "id", 1L)));
        Assert.assertNotNull("Failed to create update command callback", command.getCallback());
        // invoke callback
        command.getCallback().accept(statementResult);
    }
}
