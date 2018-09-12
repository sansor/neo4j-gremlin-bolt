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
import org.apache.tinkerpop.gremlin.structure.Transaction;
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
public class Neo4JSessionWhileAddEdgeTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Neo4JVertex outVertex;

    @Mock
    private Neo4JVertex inVertex;

    @Mock
    private Neo4JElementIdProvider provider;

    @Mock
    private Neo4JReadPartition partition;

    @Mock
    private Session session;

    @Mock
    private Transaction transaction;

    @Mock
    private org.neo4j.driver.v1.Transaction neo4jTransaction;

    @Mock
    private StatementResult statementResult;

    @Mock
    private ResultSummary resultSummary;

    @Test
    public void givenEmptyKeyValuePairsShouldCreateVEdgeWithLabel() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // act
            Neo4JEdge edge = session.addEdge("label1", outVertex, inVertex);
            // assert
            Assert.assertNotNull("Failed to create edge", edge);
            Assert.assertEquals("Failed to assign edge label", "label1", edge.label());
            Assert.assertTrue("Failed to mark edge as transient", edge.isTransient());
        }
    }

    @Test
    public void givenEmptyKeyValuePairsShouldCreateEdgeWithId() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, Mockito.mock(Session.class), provider, provider, false)) {
            // act
            Neo4JEdge edge = session.addEdge("label1", outVertex, inVertex);
            // assert
            Assert.assertNotNull("Failed to create edge", edge);
            Assert.assertEquals("Failed to assign edge id", 1L, edge.id());
            Assert.assertTrue("Failed to mark edge as transient", edge.isTransient());
        }
    }

    @Test
    public void givenKeyValuePairsShouldCreateEdgeWithProperties() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // act
            Neo4JEdge edge = session.addEdge("label1", outVertex, inVertex, "k1", "v1", "k2", 2L, "k3", true);
            // assert
            Assert.assertNotNull("Failed to create edge", edge);
            Assert.assertTrue("Failed to mark edge as transient", edge.isTransient());
            Assert.assertNotNull("Failed to assign edge property", edge.property("k1"));
            Assert.assertEquals("Failed to assign edge label", edge.property("k1").value(), "v1");
            Assert.assertNotNull("Failed to assign edge property", edge.property("k2"));
            Assert.assertEquals("Failed to assign edge label", edge.property("k2").value(), 2L);
            Assert.assertNotNull("Failed to assign edge property", edge.property("k3"));
            Assert.assertEquals("Failed to assign edge label", edge.property("k3").value(), true);
        }
    }

    @Test
    public void givenNewEdgeWithIdShouldBeAvailableOnIdQueries() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(graph.getPartition()).thenAnswer(invocation -> partition);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        ArgumentCaptor<Long> argument = ArgumentCaptor.forClass(Long.class);
        Mockito.when(provider.processIdentifier(argument.capture())).thenAnswer(invocation -> argument.getValue());
        try (Neo4JSession session = new Neo4JSession(graph, this.session, provider, provider, false)) {
            // add edge
            session.addEdge("label1", outVertex, inVertex);
            // act
            Iterator<Edge> edges = session.edges(new Object[]{1L});
            // assert
            Assert.assertNotNull("Failed to find edge", edges.hasNext());
            Edge edge = edges.next();
            Assert.assertNotNull("Failed to create edge", edge);
            Assert.assertEquals("Failed to assign edge label", "label1", edge.label());
        }
    }

    @Test
    public void givenNewEdgeWithIdShouldBeAvailableOnAllIdsQueries() {
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
                // add edge
                session.addEdge("label1", outVertex, inVertex);
                // act
                Iterator<Edge> edges = session.edges(new Object[0]);
                // assert
                Assert.assertTrue("Failed to create edge", edges.hasNext());
                Edge edge = edges.next();
                Assert.assertNotNull("Failed to create edge", edge);
                Assert.assertEquals("Failed to assign edge label", "label1", edge.label());
                // commit
                tx.success();
            }
        }
    }

    @Test
    public void givenNewEdgeWithoutIdShouldBeAvailableOnAllIdsQueries() {
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
                // add edge
                session.addEdge("label1", outVertex, inVertex);
                // act
                Iterator<Edge> edges = session.edges(new Object[0]);
                // assert
                Assert.assertNotNull("Failed to create edge", edges.hasNext());
                Edge edge = edges.next();
                Assert.assertNotNull("Failed to create edge", edge);
                Assert.assertEquals("Failed to assign edge label", "label1", edge.label());
                // commit
                tx.success();
            }
        }
    }
}
