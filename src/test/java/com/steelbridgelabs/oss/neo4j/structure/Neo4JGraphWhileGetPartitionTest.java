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
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Session;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JGraphWhileGetPartitionTest {

    @Mock
    private Driver driver;

    @Mock
    private Session session;

    @Mock
    private Neo4JElementIdProvider provider;

    @Mock
    private Neo4JReadPartition partition;

    @Test
    public void givenNewGraphShouldCreatePartitionWithAllLabels() {
        // arrange
        Mockito.when(driver.session()).thenAnswer(invocation -> session);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        try (Neo4JGraph graph = new Neo4JGraph(driver, provider, provider)) {
            // act
            Neo4JReadPartition partition = graph.getPartition();
            // assert
            Assert.assertNotNull("Failed to create partition", partition);
            Assert.assertTrue("Partition cannot exclude labels", partition.validateLabel("l1"));
        }
    }

    @Test
    public void givenPartitionShouldUsePartitionForAllOperations() {
        // arrange
        Mockito.when(driver.session()).thenAnswer(invocation -> session);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(partition.containsVertex(Mockito.any())).thenAnswer(invocation -> true);
        try (Neo4JGraph graph = new Neo4JGraph(partition, new String[0], driver, provider, provider)) {
            // act
            Neo4JReadPartition partition = graph.getPartition();
            // assert
            Assert.assertNotNull("Failed to create partition", partition);
            Assert.assertEquals("Invalid partition instance", partition, this.partition);
        }
    }

    @Test(expected = IllegalArgumentException.class)
    public void givenVertexLabelsOutsidePartitionShouldThrowException() {
        // arrange
        Mockito.when(driver.session()).thenAnswer(invocation -> session);
        Mockito.when(provider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(partition.containsVertex(Mockito.any())).thenAnswer(invocation -> false);
        // act
        new Neo4JGraph(partition, new String[0], driver, provider, provider);
        // assert
        Assert.fail("Failed to detect labels outside partition");
    }
}
