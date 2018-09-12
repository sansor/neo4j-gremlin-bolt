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
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.util.Collections;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JVertexWhileAddingEdgeTest {

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
    private Neo4JElementIdProvider provider;

    @Mock
    private Graph.Features.VertexFeatures vertexFeatures;

    @Mock
    private Graph.Features features;

    @Mock
    private Neo4JVertex otherVertex;

    @Mock
    private Neo4JEdge edge;

    @Test
    public void givenLabelAndVertexShouldCreateEdge() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generate()).thenAnswer(invocation -> 2L);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, provider, node);
        Mockito.when(session.addEdge(Mockito.eq("A"), Mockito.eq(vertex), Mockito.eq(otherVertex))).thenAnswer(invocation -> edge);
        // act
        Edge edge = vertex.addEdge("A", otherVertex);
        // assert
        Assert.assertNotNull("Failed to add edge to vertex", edge);
    }

    @Test
    public void givenKeyValuePairShouldCreateEdge() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generate()).thenAnswer(invocation -> 2L);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, provider, node);
        Mockito.when(session.addEdge(Mockito.eq("A"), Mockito.eq(vertex), Mockito.eq(otherVertex), Mockito.eq("k1"), Mockito.eq(10L))).thenAnswer(invocation -> edge);
        // act
        Edge edge = vertex.addEdge("A", otherVertex, "k1", 10L);
        // assert
        Assert.assertNotNull("Failed to add edge to vertex", edge);
    }

    @Test(expected = IllegalArgumentException.class)
    public void givenNullVertexShouldThrowException() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("l1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generate()).thenAnswer(invocation -> 2L);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, provider, node);
        Mockito.when(session.addEdge(Mockito.eq("A"), Mockito.eq(vertex), Mockito.eq(otherVertex), Mockito.eq("k1"), Mockito.eq(10L))).thenAnswer(invocation -> edge);
        // act
        vertex.addEdge("A", null, "k1", 10L);
        // assert
        Assert.fail("Failed to detect null vertex");
    }
}
