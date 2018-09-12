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

import com.steelbridgelabs.oss.neo4j.structure.partitions.NoReadPartition;
import com.steelbridgelabs.oss.neo4j.structure.summary.ResultSummaryLogger;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.AbstractThreadLocalTransaction;
import org.apache.tinkerpop.gremlin.structure.util.GraphFactoryClass;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.apache.tinkerpop.gremlin.structure.util.TransactionException;
import org.neo4j.driver.v1.AccessMode;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;
import java.util.stream.Collectors;

/**
 * @author Rogelio J. Baucells
 */
@Graph.OptIn(Graph.OptIn.SUITE_STRUCTURE_STANDARD)
@Graph.OptIn(Graph.OptIn.SUITE_PROCESS_STANDARD)
@GraphFactoryClass(Neo4JGraphFactory.class)
public class Neo4JGraph implements Graph {

    private class Neo4JTransaction extends AbstractThreadLocalTransaction {

        Neo4JTransaction() {
            super(Neo4JGraph.this);
        }

        @Override
        protected void doOpen() {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // open database transaction
            session.beginTransaction();
        }

        @Override
        protected void doCommit() throws TransactionException {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // commit transaction
            session.commit();
        }

        @Override
        protected void doRollback() throws TransactionException {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // rollback transaction
            session.rollback();
        }

        @Override
        public boolean isOpen() {
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // check transaction is open
            return session.isTransactionOpen();
        }

        @Override
        protected void doClose() {
            // close base
            super.doClose();
            // current session
            Neo4JSession session = Neo4JGraph.this.currentSession();
            // close transaction
            session.closeTransaction();
        }
    }

    private final Neo4JReadPartition partition;
    private final Set<String> vertexLabels;
    private final Driver driver;
    private final Neo4JElementIdProvider<?> vertexIdProvider;
    private final Neo4JElementIdProvider<?> edgeIdProvider;
    private final ThreadLocal<Neo4JSession> session = ThreadLocal.withInitial(() -> null);
    private final Neo4JTransaction transaction = new Neo4JTransaction();
    private final Configuration configuration;
    private final boolean readonly;
    private final Iterable<String> bookmarks;

    private final Set<Consumer<Neo4JGraph>> closeListeners = new HashSet<>();

    /**
     * Creates a {@link Neo4JGraph} instance.
     *
     * @param driver           The {@link Driver} instance with the database connection information.
     * @param vertexIdProvider The {@link Neo4JElementIdProvider} for the {@link Vertex} id generation.
     * @param edgeIdProvider   The {@link Neo4JElementIdProvider} for the {@link Edge} id generation.
     */
    public Neo4JGraph(Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider) {
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        // no label partition
        this.partition = new NoReadPartition();
        this.vertexLabels = Collections.emptySet();
        // store driver instance
        this.driver = driver;
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        // graph factory configuration (required for tinkerpop test suite)
        this.configuration = null;
        // general purpose graph
        this.readonly = false;
        this.bookmarks = null;
    }

    /**
     * Creates a {@link Neo4JGraph} instance.
     *
     * @param driver           The {@link Driver} instance with the database connection information.
     * @param vertexIdProvider The {@link Neo4JElementIdProvider} for the {@link Vertex} id generation.
     * @param edgeIdProvider   The {@link Neo4JElementIdProvider} for the {@link Edge} id generation.
     * @param readonly         {@code true} indicates the Graph instance will be used to read from the Neo4J database.
     * @param bookmarks        The initial references to some previous transactions. Both null value and empty iterable are permitted, and indicate that the bookmarks do not exist or are unknown.
     */
    public Neo4JGraph(Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, boolean readonly, String... bookmarks) {
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        // no label partition
        this.partition = new NoReadPartition();
        this.vertexLabels = Collections.emptySet();
        // store driver instance
        this.driver = driver;
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        // graph factory configuration (required for tinkerpop test suite)
        this.configuration = null;
        // readonly & bookmarks
        this.readonly = readonly;
        this.bookmarks = Arrays.asList(bookmarks);
    }

    /**
     * Creates a {@link Neo4JGraph} instance with the given partition within the neo4j database.
     *
     * @param partition        The {@link Neo4JReadPartition} within the neo4j database.
     * @param vertexLabels     The set of labels to append to vertices created by the {@link Neo4JGraph} session.
     * @param driver           The {@link Driver} instance with the database connection information.
     * @param vertexIdProvider The {@link Neo4JElementIdProvider} for the {@link Vertex} id generation.
     * @param edgeIdProvider   The {@link Neo4JElementIdProvider} for the {@link Edge} id generation.
     */
    public Neo4JGraph(Neo4JReadPartition partition, String[] vertexLabels, Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider) {
        Objects.requireNonNull(partition, "partition cannot be null");
        Objects.requireNonNull(vertexLabels, "vertexLabels cannot be null");
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        // initialize fields
        this.partition = partition;
        this.vertexLabels = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(vertexLabels)));
        this.driver = driver;
        // validate partition & additional labels
        if (!partition.containsVertex(this.vertexLabels))
            throw new IllegalArgumentException("Invalid vertexLabels, vertices created by the graph will not be part of the given partition");
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        // graph factory configuration (required for tinkerpop test suite)
        this.configuration = null;
        // general purpose graph
        this.readonly = false;
        this.bookmarks = null;
    }

    /**
     * Creates a {@link Neo4JGraph} instance with the given partition within the neo4j database.
     *
     * @param partition        The {@link Neo4JReadPartition} within the neo4j database.
     * @param vertexLabels     The set of labels to append to vertices created by the {@link Neo4JGraph} session.
     * @param driver           The {@link Driver} instance with the database connection information.
     * @param vertexIdProvider The {@link Neo4JElementIdProvider} for the {@link Vertex} id generation.
     * @param edgeIdProvider   The {@link Neo4JElementIdProvider} for the {@link Edge} id generation.
     * @param readonly         {@code true} indicates the Graph instance will be used to read from the Neo4J database.
     * @param bookmarks        The initial references to some previous transactions. Both null value and empty iterable are permitted, and indicate that the bookmarks do not exist or are unknown.
     */
    public Neo4JGraph(Neo4JReadPartition partition, String[] vertexLabels, Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, boolean readonly, String... bookmarks) {
        Objects.requireNonNull(partition, "partition cannot be null");
        Objects.requireNonNull(vertexLabels, "vertexLabels cannot be null");
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        // initialize fields
        this.partition = partition;
        this.vertexLabels = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(vertexLabels)));
        this.driver = driver;
        // validate partition & additional labels
        if (!partition.containsVertex(this.vertexLabels))
            throw new IllegalArgumentException("Invalid vertexLabels, vertices created by the graph will not be part of the given partition");
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        // graph factory configuration (required for tinkerpop test suite)
        this.configuration = null;
        // readonly & bookmarks
        this.readonly = readonly;
        this.bookmarks = Arrays.asList(bookmarks);
    }

    /**
     * Creates a {@link Neo4JGraph} instance with the given partition within the neo4j database.
     *
     * @param partition        The {@link Neo4JReadPartition} within the neo4j database.
     * @param vertexLabels     The set of labels to append to vertices created by the {@link Neo4JGraph} session.
     * @param driver           The {@link Driver} instance with the database connection information.
     * @param vertexIdProvider The {@link Neo4JElementIdProvider} for the {@link Vertex} id generation.
     * @param edgeIdProvider   The {@link Neo4JElementIdProvider} for the {@link Edge} id generation.
     * @param configuration    The {@link Configuration} used to create the {@link Graph} instance.
     */
    Neo4JGraph(Neo4JReadPartition partition, String[] vertexLabels, Driver driver, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, Configuration configuration, boolean readonly, String... bookmarks) {
        Objects.requireNonNull(partition, "partition cannot be null");
        Objects.requireNonNull(vertexLabels, "vertexLabels cannot be null");
        Objects.requireNonNull(driver, "driver cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(configuration, "configuration cannot be null");
        // initialize fields
        this.partition = partition;
        this.vertexLabels = Collections.unmodifiableSet(new HashSet<>(Arrays.asList(vertexLabels)));
        this.driver = driver;
        // validate partition & additional labels
        if (!partition.containsVertex(this.vertexLabels))
            throw new IllegalArgumentException("Invalid vertexLabels, vertices created by the graph will not be part of the given partition");
        // store providers
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        // graph factory configuration (required for tinkerpop test suite)
        this.configuration = configuration;
        // general purpose graph
        this.readonly = readonly;
        this.bookmarks = Arrays.asList(bookmarks);
    }

    Neo4JSession currentSession() {
        // get current session
        Neo4JSession session = this.session.get();
        if (session == null) {
            // create new session
            session = new Neo4JSession(this, driver.session(readonly ? AccessMode.READ : AccessMode.WRITE, bookmarks), vertexIdProvider, edgeIdProvider, readonly);
            // attach it to current thread
            this.session.set(session);
        }
        return session;
    }

    /**
     * Gets the {@link Neo4JReadPartition} that has been applied to current {@link Neo4JGraph}.
     *
     * @return The partition labels.
     */
    public Neo4JReadPartition getPartition() {
        return partition;
    }

    /**
     * Gets the {@link Neo4JElementIdProvider} used for vertex id generation.
     *
     * @return The {@link Neo4JElementIdProvider} instance.
     */
    public Neo4JElementIdProvider<?> getVertexIdProvider() {
        return vertexIdProvider;
    }

    /**
     * Gets the {@link Neo4JElementIdProvider} used for edge id generation.
     *
     * @return The {@link Neo4JElementIdProvider} instance.
     */
    public Neo4JElementIdProvider<?> getEdgeIdProvider() {
        return edgeIdProvider;
    }

    /**
     * Gets the labels that will be applied to vertices created by the current {@link Neo4JGraph}.
     *
     * @return The additional labels for new vertices.
     */
    public Set<String> vertexLabels() {
        return vertexLabels;
    }

    /**
     * Return the bookmark received following the last completed
     * {@linkplain Transaction transaction}. If no bookmark was received
     * or if this transaction was rolled back, the bookmark value will
     * be null.
     *
     * @return a reference to a previous transaction
     */
    public String lastBookmark() {
        // get current session
        Neo4JSession session = currentSession();
        // return bookmark in session
        return session.lastBookmark();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Vertex addVertex(Object... keyValues) {
        // check graph is readonly
        if (readonly)
            throw Graph.Exceptions.vertexAdditionsNotSupported();
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // add vertex
        return session.addVertex(keyValues);
    }

    /**
     * Creates an index in the neo4j database.
     *
     * @param label        The label associated with the Index.
     * @param propertyName The property name associated with the Index.
     */
    public void createIndex(String label, String propertyName) {
        Objects.requireNonNull(label, "label cannot be null");
        Objects.requireNonNull(propertyName, "propertyName cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // execute statement
        session.executeStatement(new Statement("CREATE INDEX ON :`" + label + "`(" + propertyName + ")"));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <C extends GraphComputer> C compute(Class<C> implementation) throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        throw Graph.Exceptions.graphComputerNotSupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(Object... ids) {
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find vertices
        return session.vertices(ids);
    }

    public Iterator<Vertex> vertices(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // execute statement
        StatementResult result = session.executeStatement(statement);
        // find vertices
        Iterator<Vertex> iterator = session.vertices(result)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
        // process summary (query has been already consumed by collect)
        ResultSummaryLogger.log(result.consume());
        // return iterator
        return iterator;
    }

    public Iterator<Vertex> vertices(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // use overloaded method
        return vertices(new Statement(statement));
    }

    public Iterator<Vertex> vertices(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // use overloaded method
        return vertices(new Statement(statement, parameters));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Edge> edges(Object... ids) {
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find edges
        return session.edges(ids);
    }

    public Iterator<Edge> edges(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // execute statement
        StatementResult result = session.executeStatement(statement);
        // find edges
        Iterator<Edge> iterator = session.edges(result)
            .collect(Collectors.toCollection(LinkedList::new))
            .iterator();
        // process summary (query has been already consumed by collect)
        ResultSummaryLogger.log(result.consume());
        // return iterator
        return iterator;
    }

    public Iterator<Edge> edges(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // use overloaded method
        return edges(new Statement(statement));
    }

    public Iterator<Edge> edges(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // use overloaded method
        return edges(new Statement(statement, parameters));
    }

    /**
     * Executes the given statement on the current {@link Graph} instance. WARNING: There is no
     * guarantee that the results are confined within the current {@link Neo4JReadPartition}.
     *
     * @param statement The CYPHER statement.
     * @return The {@link StatementResult} with the CYPHER statement execution results.
     */
    public StatementResult execute(Statement statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // get current session
        Neo4JSession session = currentSession();
        // transaction should be ready for io operations
        transaction.readWrite();
        // find execute statement
        return session.executeStatement(statement);
    }

    /**
     * Executes the given statement on the current {@link Graph} instance. WARNING: There is no
     * guarantee that the results are confined within the current {@link Neo4JReadPartition}.
     *
     * @param statement The CYPHER statement.
     * @return The {@link StatementResult} with the CYPHER statement execution results.
     */
    public StatementResult execute(String statement) {
        Objects.requireNonNull(statement, "statement cannot be null");
        // use overloaded method
        return execute(new Statement(statement));
    }

    /**
     * Executes the given statement on the current {@link Graph} instance. WARNING: There is no
     * guarantee that the results are confined within the current {@link Neo4JReadPartition}.
     *
     * @param statement  The CYPHER statement.
     * @param parameters The CYPHER statement parameters.
     * @return The {@link StatementResult} with the CYPHER statement execution results.
     */
    public StatementResult execute(String statement, Map<String, Object> parameters) {
        Objects.requireNonNull(statement, "statement cannot be null");
        Objects.requireNonNull(parameters, "parameters cannot be null");
        // use overloaded method
        return execute(new Statement(statement, parameters));
    }

    public boolean isProfilerEnabled() {
        // get current session
        Neo4JSession session = currentSession();
        // get from session
        return session.isProfilerEnabled();
    }

    public void setProfilerEnabled(boolean value) {
        // get current session
        Neo4JSession session = currentSession();
        // enable/disable profiler
        session.setProfilerEnabled(value);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Transaction tx() {
        // return transaction, do not open transaction here!
        return transaction;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Variables variables() {
        throw Graph.Exceptions.variablesNotSupported();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Configuration configuration() {
        return configuration;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() {
        // get current session
        Neo4JSession session = this.session.get();
        if (session != null) {
            // close session
            session.close();
            // remove session
            this.session.remove();
        }
        // synchronize access to set
        synchronized (closeListeners) {
            // notify consumers
            closeListeners.forEach(consumer -> consumer.accept(this));
        }
    }

    public void addCloseListener(Consumer<Neo4JGraph> consumer) {
        Objects.requireNonNull(consumer, "consumer cannot be null");
        // synchronize access to set
        synchronized (closeListeners) {
            // add consumer
            closeListeners.add(consumer);
        }
    }

    public void removeCloseListener(Consumer<Neo4JGraph> consumer) {
        Objects.requireNonNull(consumer, "consumer cannot be null");
        // synchronize access to set
        synchronized (closeListeners) {
            // remove consumer
            closeListeners.remove(consumer);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return StringFactory.graphString(this, "");
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Features features() {
        return new Neo4JGraphFeatures(readonly);
    }
}
