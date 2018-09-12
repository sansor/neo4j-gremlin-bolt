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
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JVertexWhileRemovingLabelTest {

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

    @Test
    public void givenExistingLabelShouldRemoveItFromVertex() {
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
        Mockito.when(provider.generate()).thenAnswer(invocation -> 2L);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, provider, node);
        // act
        boolean result = vertex.removeLabel("l1");
        // assert
        Assert.assertTrue("Failed to remove label from vertex", result);
        Assert.assertArrayEquals("Invalid vertex labels", vertex.labels(), new String[]{});
        Assert.assertTrue("Failed to mark vertex as dirty", vertex.isDirty());
    }

    @Test
    public void givenNotExistingLabelShouldNotModifyVertex() {
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
        Mockito.when(provider.generate()).thenAnswer(invocation -> 2L);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, provider, node);
        // act
        boolean result = vertex.removeLabel("l2");
        // assert
        Assert.assertFalse("Failed to detect label was not present in vertex", result);
        Assert.assertArrayEquals("Invalid vertex labels", vertex.labels(), new String[]{"l1"});
        Assert.assertFalse("Vertex is dirty", vertex.isDirty());
    }

    @Test(expected = IllegalArgumentException.class)
    public void givenLabelInReadPartitionShouldThrowException() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.eq("label"))).thenAnswer(invocation -> false);
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
        // act
        vertex.removeLabel("label");
        // assert
        Assert.fail("Failed to detect label in partition");
    }

    @Test(expected = IllegalArgumentException.class)
    public void givenLabelInWritePartitionShouldThrowException() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(partition.validateLabel(Mockito.eq("label1"))).thenAnswer(invocation -> true);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(graph.vertexLabels()).thenAnswer(invocation -> new HashSet<>(Arrays.asList("label1", "label2")));
        Mockito.when(node.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(node.labels()).thenAnswer(invocation -> Collections.singletonList("label1"));
        Mockito.when(node.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(node.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(provider.generate()).thenAnswer(invocation -> 2L);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, provider, node);
        // act
        vertex.removeLabel("label1");
        // assert
        Assert.fail("Failed to detect label in partition");
    }

    @Test
    public void givenLabelThatWasPreviouslyAddedShouldRemoveItFromVertex() {
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
        Mockito.when(provider.generate()).thenAnswer(invocation -> 2L);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Neo4JVertex vertex = new Neo4JVertex(graph, session, provider, provider, node);
        vertex.addLabel("l3");
        Assert.assertTrue(vertex.isDirty());
        // act
        boolean result = vertex.removeLabel("l3");
        // assert
        Assert.assertTrue("Failed to remove label from vertex", result);
        Assert.assertArrayEquals("Invalid vertex labels", vertex.labels(), new String[]{"l1"});
        Assert.assertFalse("Failed to reset vertex dirty property", vertex.isDirty());
    }
}
