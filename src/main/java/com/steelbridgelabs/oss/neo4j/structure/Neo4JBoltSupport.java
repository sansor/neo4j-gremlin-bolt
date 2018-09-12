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

import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.Date;
import java.util.Objects;

/**
 * @author Rogelio J. Baucells
 */
final class Neo4JBoltSupport {

    private Neo4JBoltSupport() {
    }

    static void checkPropertyValue(Object value) {
        Objects.requireNonNull(value, "value cannot be null");
        // check for supported types, TODO: Bolt supports List and Map values
        if (value instanceof Boolean || value instanceof Long || value instanceof Double || value instanceof String || value instanceof Integer || value instanceof Date)
            return;
        // throw exception
        throw Property.Exceptions.dataTypeOfPropertyValueNotSupported(value);
    }
}
