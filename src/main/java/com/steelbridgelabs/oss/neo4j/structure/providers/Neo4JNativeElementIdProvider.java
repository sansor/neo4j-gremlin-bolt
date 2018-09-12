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

package com.steelbridgelabs.oss.neo4j.structure.providers;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import org.neo4j.driver.v1.types.Entity;

import java.util.Objects;

/**
 * {@link com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider} implementation based on a Neo4J native id support.
 */
public class Neo4JNativeElementIdProvider implements Neo4JElementIdProvider<Long> {

    /**
     * Gets the field name used for {@link Entity} identifier.
     *
     * @return The field name used for {@link Entity} identifier or <code>null</code> if not using field for identifier.
     */
    @Override
    public String fieldName() {
        return null;
    }

    /**
     * Gets the identifier value from a neo4j {@link Entity}.
     *
     * @param entity The neo4j {@link Entity}.
     * @return The neo4j {@link Entity} identifier.
     */
    @Override
    public Long get(Entity entity) {
        Objects.requireNonNull(entity, "entity cannot be null");
        // return id()
        return entity.id();
    }

    /**
     * Generates a new identifier value. This {@link com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider} will fetch a pool of identifiers
     * from a Neo4J database Node.
     *
     * @return A unique identifier within the database sequence generator.
     */
    @Override
    public Long generate() {
        // no id generation
        return null;
    }

    /**
     * Process the given identifier converting it to the correct type if necessary.
     *
     * @param id The {@link org.apache.tinkerpop.gremlin.structure.Element} identifier.
     * @return The {@link org.apache.tinkerpop.gremlin.structure.Element} identifier converted to the correct type if necessary.
     */
    @Override
    public Long processIdentifier(Object id) {
        Objects.requireNonNull(id, "Element identifier cannot be null");
        // check for Long
        if (id instanceof Long)
            return (Long)id;
        // check for numeric types
        if (id instanceof Number)
            return ((Number)id).longValue();
        // check for string
        if (id instanceof String)
            return Long.valueOf((String)id);
        // error
        throw new IllegalArgumentException(String.format("Expected an id that is convertible to Long but received %s", id.getClass()));
    }

    /**
     * Gets the MATCH WHERE predicate operand.
     *
     * @param alias The neo4j {@link Entity} alias in a MATCH statement.
     * @return The MATCH WHERE predicate operand.
     */
    @Override
    public String matchPredicateOperand(String alias) {
        Objects.requireNonNull(alias, "alias cannot be null");
        // id(alias)
        return "ID(" + alias + ")";
    }
}
