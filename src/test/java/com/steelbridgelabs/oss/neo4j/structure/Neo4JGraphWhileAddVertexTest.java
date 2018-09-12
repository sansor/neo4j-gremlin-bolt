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

import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JGraphWhileAddVertexTest {

    @Mock
    private Driver driver;

    @Mock
    private Session session;

    @Mock
    private Transaction transaction;

    @Mock
    private Neo4JElementIdProvider provider;

    @Mock
    private StatementResult statementResult;

    @Test
    @SuppressWarnings("unchecked")
    public void givenLabelShouldAddVertex() {
        // arrange
        Mockito.when(driver.session(Mockito.any(AccessMode.class), Mockito.any(Iterable.class))).thenReturn(session);
        Mockito.when(session.beginTransaction()).thenAnswer(invocation -> transaction);
        Mockito.when(transaction.run(Mockito.any(Statement.class))).thenAnswer(invocation -> statementResult);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JGraph graph = new Neo4JGraph(driver, provider, provider)) {
            // act
            Neo4JVertex vertex = (Neo4JVertex)graph.addVertex("L1");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Invalid vertex label", vertex.label(), "L1");
            Assert.assertTrue("Failed to initialize vertex as transient", vertex.isTransient());
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void givenKeyValuePairShouldAddVertex() {
        // arrange
        Mockito.when(driver.session(Mockito.any(AccessMode.class), Mockito.any(Iterable.class))).thenReturn(session);
        Mockito.when(session.beginTransaction()).thenAnswer(invocation -> transaction);
        Mockito.when(transaction.run(Mockito.any(Statement.class))).thenAnswer(invocation -> statementResult);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(provider.generate()).thenAnswer(invocation -> 1L);
        Mockito.when(provider.processIdentifier(Mockito.any())).thenAnswer(invocation -> 1L);
        try (Neo4JGraph graph = new Neo4JGraph(driver, provider, provider)) {
            // act
            Vertex vertex = graph.addVertex("key", "value");
            // assert
            Assert.assertNotNull("Failed to create vertex", vertex);
            Assert.assertEquals("Invalid vertex property value", vertex.property("key").orElse(null), "value");
        }
    }
}
