/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package com.steelbridgelabs.oss.neo4j.structure;

import com.steelbridgelabs.oss.neo4j.structure.Neo4JElementIdProvider;
import org.neo4j.driver.v1.types.Entity;

import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * @author Rogelio J. Baucells
 */
public class ElementIdProvider implements Neo4JElementIdProvider<Long> {

    public final static String IdFieldName = "id";

    private final AtomicLong atomicLong = new AtomicLong();

    @Override
    public String fieldName() {
        return IdFieldName;
    }

    @Override
    public Long generate() {
        return atomicLong.incrementAndGet();
    }

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

    @Override
    public Long get(Entity entity) {
        Objects.requireNonNull(entity, "entity cannot be null");
        // return property value
        return entity.get(IdFieldName).asLong();
    }

    @Override
    public String matchPredicateOperand(String alias) {
        return alias + "." + IdFieldName;
    }
}
