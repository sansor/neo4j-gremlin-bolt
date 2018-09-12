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
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.types.Entity;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;

/**
 * {@link Neo4JElementIdProvider} implementation based on a sequence generator stored in a Neo4J database Node.
 */
public class DatabaseSequenceElementIdProvider implements Neo4JElementIdProvider<Long> {

    private static final Logger logger = LoggerFactory.getLogger(DatabaseSequenceElementIdProvider.class);

    public static final String DefaultIdFieldName = "id";
    public static final String DefaultSequenceNodeLabel = "UniqueIdentifierGenerator";
    public static final long DefaultPoolSize = 1000;

    private final Driver driver;
    private final String idFieldName;
    private final String sequenceNodeLabel;
    private final long poolSize;
    private final AtomicLong atomicLong = new AtomicLong(0L);
    private final Object monitor = new Object();

    private AtomicLong maximum = new AtomicLong(0L);

    public DatabaseSequenceElementIdProvider(Driver driver) {
        Objects.requireNonNull(driver, "driver cannot be null");
        // initialize fields
        this.driver = driver;
        this.poolSize = DefaultPoolSize;
        this.idFieldName = DefaultIdFieldName;
        this.sequenceNodeLabel = DefaultSequenceNodeLabel;
    }

    public DatabaseSequenceElementIdProvider(Driver driver, long poolSize, String idFieldName, String sequenceNodeLabel) {
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(idFieldName, "idFieldName cannot be null");
        Objects.requireNonNull(sequenceNodeLabel, "sequenceNodeLabel cannot be null");
        // initialize fields
        this.driver = driver;
        this.poolSize = poolSize;
        this.idFieldName = idFieldName;
        this.sequenceNodeLabel = sequenceNodeLabel;
    }

    /**
     * Gets the field name used for {@link Entity} identifier.
     *
     * @return The field name used for {@link Entity} identifier or <code>null</code> if not using field for identifier.
     */
    @Override
    public String fieldName() {
        return idFieldName;
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
        // return property value
        return entity.get(idFieldName).asLong();
    }

    /**
     * Generates a new identifier value. This {@link Neo4JElementIdProvider} will fetch a pool of identifiers
     * from a Neo4J database Node.
     *
     * @return A unique identifier within the database sequence generator.
     */
    @Override
    public Long generate() {
        // get maximum identifier we can use (before obtaining new identifier to make sure it is in the current pool)
        long max = maximum.get();
        // generate new identifier
        long identifier = atomicLong.incrementAndGet();
        // check we need to obtain new identifier pool (identifier is out of range for current pool)
        if (identifier > max) {
            // loop until we get an identifier value
            do {
                // log information
                if (logger.isDebugEnabled())
                    logger.debug("About to request a pool of identifiers from database, maximum id: {}", max);
                // make sure only one thread gets a new range of identifiers
                synchronized (monitor) {
                    // update maximum number in pool, do not switch the next two statements (in case another thread was executing the synchronized block while the current thread was waiting)
                    max = maximum.get();
                    identifier = atomicLong.incrementAndGet();
                    // verify a new identifier is needed (compare it with current maximum)
                    if (identifier >= max) {
                        // create database session
                        try (Session session = driver.session()) {
                            // create transaction
                            try (Transaction transaction = session.beginTransaction()) {
                                // create cypher command, reserve poolSize identifiers
                                Statement statement = new Statement("MERGE (g:`" + sequenceNodeLabel + "`) ON CREATE SET g.nextId = 1 ON MATCH SET g.nextId = g.nextId + {poolSize} RETURN g.nextId", Collections.singletonMap("poolSize", poolSize));
                                // execute statement
                                StatementResult result = transaction.run(statement);
                                // process result
                                if (result.hasNext()) {
                                    // get record
                                    Record record = result.next();
                                    // get nextId value
                                    long nextId = record.get(0).asLong();
                                    // set value for next identifier (do not switch the next two statements!)
                                    atomicLong.set(nextId - poolSize);
                                    maximum.set(nextId);
                                }
                                // commit
                                transaction.success();
                            }
                        }
                        // update maximum number in pool
                        max = maximum.get();
                        // get a new identifier
                        identifier = atomicLong.incrementAndGet();
                        // log information
                        if (logger.isDebugEnabled())
                            logger.debug("Requested new pool of identifiers from database, current id: {}, maximum id: {}", identifier, max);
                    }
                    else if (logger.isDebugEnabled())
                        logger.debug("No need to request pool of identifiers, current id: {}, maximum id: {}", identifier, max);
                }
            }
            while (identifier > max);
        }
        else if (logger.isDebugEnabled())
            logger.debug("Current identifier: {}", identifier);
        // return identifier
        return identifier;
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
        // alias.identifier
        return alias + "." + idFieldName;
    }
}