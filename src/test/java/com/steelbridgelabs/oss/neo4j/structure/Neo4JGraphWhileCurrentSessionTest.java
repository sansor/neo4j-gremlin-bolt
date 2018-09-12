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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JGraphWhileCurrentSessionTest {

    @Mock
    private Driver driver;

    @Mock
    private Session session;

    @Mock
    private Neo4JElementIdProvider provider;

    @Test
    @SuppressWarnings("unchecked")
    public void givenNewGraphShouldCreateNewSession() {
        // arrange
        Mockito.when(driver.session(Mockito.any(AccessMode.class), Mockito.any(Iterable.class))).thenReturn(session);
        Mockito.when(driver.session()).thenAnswer(invocation -> session);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        try (Neo4JGraph graph = new Neo4JGraph(driver, provider, provider)) {
            // act
            try (Neo4JSession neo4JSession = graph.currentSession()) {
                // assert
                Assert.assertNotNull("Failed to create Neo4JSession instance", neo4JSession);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void givenGraphWithSessionShouldReturnSameSession() {
        // arrange
        Mockito.when(driver.session(Mockito.any(AccessMode.class), Mockito.any(Iterable.class))).thenReturn(session);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        try (Neo4JGraph graph = new Neo4JGraph(driver, provider, provider)) {
            try (Neo4JSession neo4JSession1 = graph.currentSession()) {
                Assert.assertNotNull("Failed to create Neo4JSession instance", neo4JSession1);
                // act
                Neo4JSession neo4JSession2 = graph.currentSession();
                // assert
                Assert.assertNotNull("Failed to return Neo4JSession instance", neo4JSession2);
                Assert.assertEquals("Failed to return same session", neo4JSession1, neo4JSession2);
            }
        }
    }

    @Test
    @SuppressWarnings("unchecked")
    public void givenGraphWithSessionShouldReturnAnotherSessionFromADifferentThread() throws InterruptedException {
        // arrange
        Mockito.when(driver.session(Mockito.any(AccessMode.class), Mockito.any(Iterable.class))).thenReturn(session);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        try (Neo4JGraph graph = new Neo4JGraph(driver, provider, provider)) {
            try (final Neo4JSession neo4JSession1 = graph.currentSession()) {
                Assert.assertNotNull("Failed to create Neo4JSession instance", neo4JSession1);
                // act
                Thread thread = new Thread(() -> {
                    try (Neo4JSession neo4JSession2 = graph.currentSession()) {
                        // assert
                        Assert.assertNotNull("Failed to return Neo4JSession instance", neo4JSession2);
                        Assert.assertNotEquals("Using the same session from a different thread", neo4JSession1, neo4JSession2);
                    }
                });
                thread.start();
                thread.join();
            }
        }
    }
}
