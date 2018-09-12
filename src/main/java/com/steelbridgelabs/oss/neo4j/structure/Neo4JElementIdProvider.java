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

import org.neo4j.driver.v1.types.Entity;

/**
 * @author Rogelio J. Baucells
 */
public interface Neo4JElementIdProvider<T> {

    /**
     * Generates a new identifier value for a {@link Neo4JElement}.
     *
     * @return the new identifier value or <code>null</code> if provider does not support identifier generation.
     */
    T generate();

    /**
     * Gets the field name used for {@link Entity} identifier.
     *
     * @return The field name used for {@link Entity} identifier or <code>null</code> if not using field for identifier.
     */
    String fieldName();

    /**
     * Gets the identifier value from a neo4j {@link Entity}.
     *
     * @param entity The neo4j {@link Entity}.
     * @return The neo4j {@link Entity} identifier.
     */
    T get(Entity entity);

    /**
     * Process the given identifier converting it to the correct type if necessary.
     *
     * @param id The {@link Neo4JElement} identifier.
     * @return The {@link Neo4JElement} identifier converted to the correct type if necessary.
     */
    T processIdentifier(Object id);

    /**
     * Gets the MATCH WHERE predicate operand.
     *
     * @param alias The neo4j {@link Entity} alias in a MATCH statement.
     * @return The MATCH WHERE predicate operand.
     */
    String matchPredicateOperand(String alias);
}
