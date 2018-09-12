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

package com.steelbridgelabs.oss.neo4j.structure.partitions;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JReadPartition;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.Set;

/**
 * @author Rogelio J. Baucells
 */
public class AllLabelReadPartitionWhileVertexMatchPatternLabelsTest {

    @Test
    public void givenPartitionShouldReturnVertexMatchPatternLabels() {
        // arrange
        Neo4JReadPartition partition = new AllLabelReadPartition("l1", "l2", "l3");
        // act
        Set<String> result = partition.vertexMatchPatternLabels();
        // assert
        Assert.assertNotNull("Invalid vertex match pattern labels", result);
        Assert.assertTrue("Failed to generate vertex match pattern labels", result.containsAll(Arrays.asList("l3", "l1", "l2")));
    }
}
