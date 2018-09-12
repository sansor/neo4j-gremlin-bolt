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

import java.util.Set;

/**
 * @author Rogelio J. Baucells
 */
public interface Neo4JReadPartition {

    /**
     * Checks the given label can be added/removed to/from a vertex.
     *
     * @param label The label to validate.
     * @return <code>true</code> if the label can be assigned to a vertex, otherwise <code>false</code>.
     */
    boolean validateLabel(String label);

    /**
     * Checks if the partition has the given vertex (labels in vertex).
     *
     * @param labels The label to check in the partition.
     * @return <code>true</code> if the vertex is in the partition, otherwise <code>false</code>.
     */
    boolean containsVertex(Set<String> labels);

    /**
     * Checks if the partition uses MATCH patterns (see {@link #vertexMatchPatternLabels()}).
     *
     * @return <code>true</code> if the partition uses MATCH patterns, otherwise <code>false</code>.
     */
    boolean usesMatchPattern();

    /**
     * Checks if the partition uses MATCH predicate (see {@link #vertexMatchPredicate(String)}).
     *
     * @return <code>true</code> if the partition uses MATCH predicate, otherwise <code>false</code>.
     */
    boolean usesMatchPredicate();

    /**
     * Gets the set of labels required at the time of matching a vertex in a Cypher MATCH pattern.
     *
     * @return The set of labels.
     */
    Set<String> vertexMatchPatternLabels();

    /**
     * Generates a {@link org.apache.tinkerpop.gremlin.structure.Vertex} Cypher MATCH predicate, example:
     * <p>
     * (alias:Label1 OR alias:Label2)
     * </p>
     *
     * @param alias The vertex alias in the MATCH Cypher statement.
     * @return The Cypher MATCH predicate if required by the vertex, otherwise <code>null</code>.
     */
    String vertexMatchPredicate(String alias);
}
