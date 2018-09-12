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
import org.apache.tinkerpop.gremlin.structure.T;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.summary.ResultSummary;

import java.util.Iterator;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JSessionWhileAddVertexTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Transaction transaction;

    @Mock
    private Neo4JElementIdProvider provider;

    @Mock
    private Graph.Features.VertexFeatures vertexFeatures;

    @Mock
    private Graph.Features features;

    @Mock
    private Neo4JReadPartition partition;

    @Mock
    private Session session;

    @Mock
    private org.neo4j.driver.v1.Transaction neo4jTransaction;

    @Mock
    private StatementResult statementResult;

    @Mock
    private ResultSummary resultSummary;

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVertexWithDefaultLabel() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // act
            Vertex vertex = session.addVertex();
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
        }
    }

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVertexWithId() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.anyInt())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // act
            Vertex vertex = session.addVertex();
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex id", 1L, vertex.id());
        }
    }

    @Test
    public void givenLabelShouldCreateVertex() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // act
            Vertex vertex = session.addVertex(T.label, "label1");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", "label1", vertex.label());
        }
    }

    @Test
    public void givenLabelsShouldCreateVertex() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // act
            Neo4JVertex vertex = session.addVertex(T.label, "label1::label2::label3");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", "label1::label2::label3", vertex.label());
            Assert.assertArrayEquals("Failed to assign vertex labels", new String[]{"label1", "label2", "label3"}, vertex.labels());
        }
    }

    @Test
    public void givenKeyValuePairsShouldCreateVertexWithProperties() {
        // arrange
        Mockito.when(vertexFeatures.getCardinality(Mockito.anyString())).thenAnswer(invocation -> VertexProperty.Cardinality.single);
        Mockito.when(features.vertex()).thenAnswer(invocation -> vertexFeatures);
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(graph.features()).thenAnswer(invocation -> features);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // act
            Neo4JVertex vertex = session.addVertex("k1", "v1", "k2", 2L, "k3", true);
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k1"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k1").value(), "v1");
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k2"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k2").value(), 2L);
            Assert.assertNotNull("Failed to assign vertex property", vertex.property("k3"));
            Assert.assertEquals("Failed to assign vertex label", vertex.property("k3").value(), true);
        }
    }

    @Test
    public void givenNewVertexWithIdShouldBeAvailableOnIdQueries() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // add vertex
            session.addVertex();
            // act
            Iterator<Vertex> vertices = session.vertices(new Object[]{1L});
            // assert
            Assert.assertNotNull("Failed to find vertex", vertices.hasNext());
            Vertex vertex = vertices.next();
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
        }
    }

    @Test
    public void givenNewVertexWithIdShouldBeAvailableOnAllIdsQueries() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(session.beginTransaction()).then(invocation -> neo4jTransaction);
        Mockito.when(neo4jTransaction.run(Mockito.any(Statement.class))).then(invocation -> statementResult);
        Mockito.when(statementResult.hasNext()).then(invocation -> false);
        Mockito.when(statementResult.consume()).then(invocation -> resultSummary);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // transaction
            try (org.neo4j.driver.v1.Transaction tx = session.beginTransaction()) {
                // add vertex
                session.addVertex();
                // act
                Iterator<Vertex> vertices = session.vertices(new Object[0]);
                // assert
                Assert.assertNotNull("Failed to find vertex", vertices.hasNext());
                Vertex vertex = vertices.next();
                Assert.assertNotNull("Failed to create vertex", vertex);
                Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
                // commit
                tx.success();
            }
        }
    }

    @Test
    public void givenNewVertexWithoutIdShouldBeAvailableOnAllIdsQueries() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(session.beginTransaction()).then(invocation -> neo4jTransaction);
        Mockito.when(neo4jTransaction.run(Mockito.any(Statement.class))).then(invocation -> statementResult);
        Mockito.when(statementResult.hasNext()).then(invocation -> false);
        Mockito.when(statementResult.consume()).then(invocation -> resultSummary);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> null);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // transaction
            try (org.neo4j.driver.v1.Transaction tx = session.beginTransaction()) {
                // add vertex
                session.addVertex();
                // act
                Iterator<Vertex> vertices = session.vertices(new Object[0]);
                // assert
                Assert.assertNotNull("Failed to find vertex", vertices.hasNext());
                Vertex vertex = vertices.next();
                Assert.assertNotNull("Failed to create vertex", vertex);
                Assert.assertEquals("Failed to assign vertex label", Vertex.DEFAULT_LABEL, vertex.label());
                // commit
                tx.success();
            }
        }
    }
}
