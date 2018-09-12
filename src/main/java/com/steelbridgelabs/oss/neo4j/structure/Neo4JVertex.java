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

import com.steelbridgelabs.oss.neo4j.structure.summary.ResultSummaryLogger;
import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.internal.types.TypeRepresentation;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Node;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collector;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Rogelio J. Baucells
 */
public class Neo4JVertex extends Neo4JElement implements Vertex {

    private static class Neo4JVertexProperty<T> implements VertexProperty<T> {

        private final Neo4JVertex vertex;
        private final Object id;
        private final String name;
        private final T value;

        public Neo4JVertexProperty(Neo4JVertex vertex, Object id, String name, T value) {
            Objects.requireNonNull(vertex, "vertex cannot be null");
            Objects.requireNonNull(id, "id cannot be null");
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(value, "value cannot be null");
            // store fields
            this.vertex = vertex;
            this.id = id;
            this.name = name;
            this.value = value;
        }

        @Override
        public Vertex element() {
            return vertex;
        }

        @Override
        public <U> Iterator<Property<U>> properties(String... propertyKeys) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        @Override
        public Object id() {
            return id;
        }

        @Override
        public <V> Property<V> property(String key, V value) {
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        }

        @Override
        public String key() {
            return name;
        }

        @Override
        public T value() throws NoSuchElementException {
            return value;
        }

        @Override
        public boolean isPresent() {
            return true;
        }

        @Override
        public void remove() {
            // check cardinality
            Cardinality cardinality = vertex.cardinalities.get(name);
            if (cardinality != null) {
                // check it is single value
                if (cardinality != Cardinality.single) {
                    // get list of properties in vertex
                    Collection<?> vertexProperties = vertex.properties.get(name);
                    if (vertexProperties != null) {
                        // remove this instance from list
                        vertexProperties.remove(this);
                        // check properties are empty, remove key from vertex properties
                        if (vertexProperties.isEmpty()) {
                            // remove property
                            vertex.properties.remove(name);
                            // remove cardinality
                            vertex.cardinalities.remove(name);
                            // mark property as removed
                            vertex.removedProperties.add(name);
                            // mark vertex as dirty
                            vertex.dirty = true;
                            // notify session
                            vertex.session.dirtyVertex(vertex);
                        }
                    }
                }
                else {
                    // remove property
                    vertex.properties.remove(name);
                    // remove cardinality
                    vertex.cardinalities.remove(name);
                    // mark property as removed
                    vertex.removedProperties.add(name);
                    // mark vertex as dirty
                    vertex.dirty = true;
                    // notify session
                    vertex.session.dirtyVertex(vertex);
                }
            }
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof VertexProperty && ElementHelper.areEqual(this, object);
        }

        @Override
        public int hashCode() {
            return ElementHelper.hashCode((Element)this);
        }

        @Override
        public String toString() {
            return StringFactory.propertyString(this);
        }
    }

    public static final String LabelDelimiter = "::";

    private static final AtomicLong propertyIdProvider = new AtomicLong(0L);

    private final Object id;
    private final Neo4JGraph graph;
    private final Neo4JReadPartition partition;
    private final Neo4JSession session;
    private final Neo4JElementIdProvider<?> vertexIdProvider;
    private final Neo4JElementIdProvider<?> edgeIdProvider;
    private final Map<String, Collection<VertexProperty>> properties = new HashMap<>();
    private final Map<String, VertexProperty.Cardinality> cardinalities = new HashMap<>();
    private final Set<Neo4JEdge> outEdges = new HashSet<>();
    private final Set<Neo4JEdge> inEdges = new HashSet<>();
    private final Set<String> outEdgeLabels = new HashSet<>();
    private final Set<String> inEdgeLabels = new HashSet<>();
    private final SortedSet<String> labelsAdded = new TreeSet<>();
    private final SortedSet<String> labelsRemoved = new TreeSet<>();
    private final SortedSet<String> labels;
    private final Set<String> additionalLabels;

    private Object generatedId = null;
    private boolean outEdgesLoaded = false;
    private boolean inEdgesLoaded = false;
    private boolean dirty = false;
    private SortedSet<String> matchLabels;
    private SortedSet<String> originalLabels;
    private Set<String> graphLabels;
    private Set<String> removedProperties = new HashSet<>();
    private Map<String, Collection<VertexProperty>> originalProperties;
    private Map<String, VertexProperty.Cardinality> originalCardinalities;

    Neo4JVertex(Neo4JGraph graph, Neo4JSession session, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, Collection<String> labels) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(labels, "labels cannot be null");
        // store fields
        this.graph = graph;
        this.partition = graph.getPartition();
        this.additionalLabels = graph.vertexLabels();
        this.session = session;
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        this.labels = new TreeSet<>(labels);
        // this is the original set of labels
        this.originalLabels = Collections.emptySortedSet();
        // labels used to match vertex in database
        this.matchLabels = Collections.emptySortedSet();
        // graph labels
        this.graphLabels = additionalLabels;
        // initialize original properties and cardinalities
        this.originalProperties = new HashMap<>();
        this.originalCardinalities = new HashMap<>();
        // generate id
        this.id = vertexIdProvider.generate();
        // this is a new vertex, everything is in memory
        outEdgesLoaded = true;
        inEdgesLoaded = true;
    }

    Neo4JVertex(Neo4JGraph graph, Neo4JSession session, Neo4JElementIdProvider<?> vertexIdProvider, Neo4JElementIdProvider<?> edgeIdProvider, Node node) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(vertexIdProvider, "vertexIdProvider cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(node, "node cannot be null");
        // store fields
        this.graph = graph;
        this.partition = graph.getPartition();
        this.additionalLabels = graph.vertexLabels();
        this.session = session;
        this.vertexIdProvider = vertexIdProvider;
        this.edgeIdProvider = edgeIdProvider;
        // from node
        this.id = vertexIdProvider.get(node);
        // graph labels (additional & partition labels in original node)
        this.graphLabels = StreamSupport.stream(node.labels().spliterator(), false).filter(label -> additionalLabels.contains(label) && !partition.validateLabel(label)).collect(Collectors.toSet());
        // labels, do not store additional && partition labels
        this.labels = StreamSupport.stream(node.labels().spliterator(), false).filter(label -> !graphLabels.contains(label)).collect(Collectors.toCollection(TreeSet::new));
        // this is the original set of labels
        this.originalLabels = new TreeSet<>(this.labels);
        // labels used to match the vertex in the database
        this.matchLabels = StreamSupport.stream(node.labels().spliterator(), false).collect(Collectors.toCollection(TreeSet::new));
        // id field name (if any)
        String idFieldName = vertexIdProvider.fieldName();
        // copy properties from node, exclude identifier
        StreamSupport.stream(node.keys().spliterator(), false).filter(key -> !key.equals(idFieldName)).forEach(key -> {
            // value
            Value value = node.get(key);
            TypeRepresentation type = (TypeRepresentation)value.type();
            // process value type
            switch (type.constructor()) {
                case LIST:
                    // process values
                    properties.put(key, value.asList().stream().map(item -> new Neo4JVertexProperty<>(this, propertyIdProvider.incrementAndGet(), key, item)).collect(Collectors.toList()));
                    // cardinality
                    cardinalities.put(key, VertexProperty.Cardinality.list);
                    break;
                case MAP:
                    throw new RuntimeException("TODO: implement maps");
                default:
                    // add property
                    properties.put(key, Collections.singletonList(new Neo4JVertexProperty<>(this, propertyIdProvider.incrementAndGet(), key, value.asObject())));
                    // cardinality
                    cardinalities.put(key, VertexProperty.Cardinality.single);
                    break;
            }
        });
        // initialize original properties and cardinalities
        this.originalProperties = new HashMap<>(properties);
        this.originalCardinalities = new HashMap<>(cardinalities);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Object id() {
        return id != null ? id : generatedId;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String label() {
        // labels separated by "::"
        return labels.stream().collect(Collectors.joining(LabelDelimiter));
    }

    public String[] labels() {
        return labels.toArray(new String[labels.size()]);
    }

    public boolean addLabel(String label) {
        Objects.requireNonNull(label, "label cannot be null");
        // exclude partition
        if (!partition.validateLabel(label))
            throw new IllegalArgumentException("Invalid label, label name cannot be the same as Graph partition labels");
        // add label to set
        if (labels.add(label)) {
            // notify session
            session.dirtyVertex(this);
            // we need to update labels
            labelsAdded.add(label);
            // indicate label was added
            return true;
        }
        return false;
    }

    public boolean removeLabel(String label) {
        Objects.requireNonNull(label, "label cannot be null");
        // exclude partition
        if (!partition.validateLabel(label))
            throw new IllegalArgumentException("Invalid label, label name cannot be removed since it is part of the Graph partition");
        // prevent additional labels from being removed
        if (additionalLabels.contains(label))
            throw new IllegalArgumentException("Invalid label, label name cannot be removed since it is part of additional labels for vertices");
        // remove label from set
        if (labels.remove(label)) {
            // check this label was previously added in this session
            if (!labelsAdded.remove(label)) {
                // notify session
                session.dirtyVertex(this);
                // we need to update labels
                labelsRemoved.add(label);
            }
            // indicate label was removed
            return true;
        }
        return false;
    }

    /**
     * Generates a Cypher MATCH pattern for the vertex, example:
     * <p>
     * (alias:Label1:Label2)
     * </p>
     *
     * @param alias The node alias, <code>null</code> if not required.
     * @return the Cypher MATCH clause.
     */
    public String matchPattern(String alias) {
        // generate match pattern
        if (alias != null)
            return "(" + alias + processLabels(matchLabels, false) + ")";
        // pattern without alias
        return "(" + processLabels(matchLabels, false) + ")";
    }

    /**
     * Generates a Cypher MATCH predicate for the vertex, example:
     * <p>
     * alias.id = {id} AND (alias:Label1 OR alias:Label2)
     * </p>
     *
     * @param alias           The node alias.
     * @param idParameterName The name of the parameter that contains the vertex id.
     * @return the Cypher MATCH predicate or <code>null</code> if not required to MATCH the vertex.
     */
    public String matchPredicate(String alias, String idParameterName) {
        Objects.requireNonNull(alias, "alias cannot be null");
        Objects.requireNonNull(idParameterName, "idParameterName cannot be null");
        // get partition
        Neo4JReadPartition partition = graph.getPartition();
        // create match predicate
        return vertexIdProvider.matchPredicateOperand(alias) + " = {" + idParameterName + "}" + (partition.usesMatchPredicate() ? " AND (" + partition.vertexMatchPredicate(alias) + ")" : "");
    }

    /**
     * Generates a Cypher MATCH statement for the vertex, example:
     * <p>
     * MATCH (alias) WHERE alias.id = {id} AND (alias:Label1 OR alias:Label2)
     * </p>
     *
     * @param alias           The node alias.
     * @param idParameterName The name of the parameter that contains the vertex id.
     * @return the Cypher MATCH predicate or <code>null</code> if not required to MATCH the vertex.
     */
    public String matchStatement(String alias, String idParameterName) {
        Objects.requireNonNull(alias, "alias cannot be null");
        Objects.requireNonNull(idParameterName, "idParameterName cannot be null");
        // create statement
        return "MATCH " + matchPattern(alias) + " WHERE " + matchPredicate(alias, idParameterName);
    }

    @Override
    public boolean isDirty() {
        return dirty || !labelsAdded.isEmpty() || !labelsRemoved.isEmpty();
    }

    @Override
    public boolean isTransient() {
        return originalLabels.isEmpty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Edge addEdge(String label, Vertex vertex, Object... keyValues) {
        // validate label
        ElementHelper.validateLabel(label);
        // vertex must exist
        if (vertex == null)
            throw Graph.Exceptions.argumentCanNotBeNull("vertex");
        // validate properties
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // add edge
        return session.addEdge(label, this, (Neo4JVertex)vertex, keyValues);
    }

    void removeEdge(Neo4JEdge edge) {
        // remove edge from internal references
        outEdges.remove(edge);
        inEdges.remove(edge);
    }

    private void processEdgesWhereClause(String vertexAlias, List<Object> identifiers, String alias, StringBuilder builder, Map<String, Object> parameters) {
        // generate match predicate
        String predicate = partition.vertexMatchPredicate(vertexAlias);
        // check identifiers are empty
        if (!identifiers.isEmpty()) {
            // filter edges
            builder.append(" AND NOT ").append(edgeIdProvider.matchPredicateOperand(alias)).append(" IN {ids}");
            // ids parameters
            parameters.put("ids", identifiers);
            // check we need to add in predicate
            if (predicate != null) {
                // append predicate
                builder.append(" AND ").append(predicate);
            }
        }
        else if (predicate != null) {
            // append WHERE
            builder.append(" AND ").append(predicate);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Edge> edges(Direction direction, String... labels) {
        Objects.requireNonNull(direction, "direction cannot be null");
        Objects.requireNonNull(labels, "labels cannot be null");
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // load labels in hash set (remove duplicates)
        Set<String> set = new HashSet<>(Arrays.asList(labels));
        // parameters
        Map<String, Object> parameters = new HashMap<>();
        // vertex id
        parameters.put("id", id());
        // out edges
        if (direction == Direction.OUT) {
            // check we have all edges in memory
            if (!outEdgesLoaded) {
                // labels we need to query for
                Set<String> relationshipLabels = set.stream().filter(item -> !outEdgeLabels.contains(item)).collect(Collectors.toSet());
                // check query is required for labels
                if (set.isEmpty() || !relationshipLabels.isEmpty()) {
                    // create string builder
                    StringBuilder builder = new StringBuilder();
                    // match clause
                    builder.append("MATCH ").append(matchPattern("n")).append("-[r").append(relationshipLabels.stream().map(label -> ":`" + label + "`").collect(Collectors.joining("|"))).append("]->(m").append(processLabels(Collections.emptySet(), true)).append(")").append(" WHERE ").append(vertexIdProvider.matchPredicateOperand("n")).append(" = {id}");
                    // edge ids already in memory
                    List<Object> identifiers = outEdges.stream().map(Neo4JEdge::id).filter(Objects::nonNull).collect(Collectors.toList());
                    // process where clause
                    processEdgesWhereClause("m", identifiers, "r", builder, parameters);
                    // return
                    builder.append(" RETURN n, r, m");
                    // create statement
                    Statement statement = new Statement(builder.toString(), parameters);
                    // execute statement
                    StatementResult result = session.executeStatement(statement);
                    // execute command
                    Stream<Edge> query = session.edges(result);
                    // edges in memory plus the ones in database (return copy since edges can be deleted in the middle of the loop)
                    Iterator<Edge> iterator = Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(edge -> (Edge)edge), query)
                        .collect(Collectors.toList())
                        .iterator();
                    // process summary (query has been already consumed by combine)
                    ResultSummaryLogger.log(result.consume());
                    // after this line it is safe to update loaded flag and labels in memory
                    outEdgesLoaded = labels.length == 0;
                    outEdgeLabels.addAll(set);
                    // return iterator
                    return iterator;
                }
            }
            // edges in memory (return copy since edges can be deleted in the middle of the loop)
            return outEdges.stream().filter(edge -> labels.length == 0 || set.contains(edge.label()))
                .map(edge -> (Edge)edge)
                .collect(Collectors.toList())
                .iterator();
        }
        // in edges
        if (direction == Direction.IN) {
            // check we have all edges in memory
            if (!inEdgesLoaded) {
                // labels we need to query for
                Set<String> relationshipLabels = set.stream().filter(item -> !inEdgeLabels.contains(item)).collect(Collectors.toSet());
                // check query is required for labels
                if (set.isEmpty() || !relationshipLabels.isEmpty()) {
                    // create string builder
                    StringBuilder builder = new StringBuilder();
                    // match clause
                    builder.append("MATCH ").append(matchPattern("n")).append("<-[r").append(relationshipLabels.stream().map(label -> ":`" + label + "`").collect(Collectors.joining("|"))).append("]-(m").append(processLabels(Collections.emptySet(), true)).append(")").append(" WHERE ").append(vertexIdProvider.matchPredicateOperand("n")).append(" = {id}");
                    // edge ids already in memory
                    List<Object> identifiers = inEdges.stream().map(Neo4JEdge::id).filter(Objects::nonNull).collect(Collectors.toList());
                    // process where clause
                    processEdgesWhereClause("m", identifiers, "r", builder, parameters);
                    // return
                    builder.append(" RETURN n, r, m");
                    // create statement
                    Statement statement = new Statement(builder.toString(), parameters);
                    // execute statement
                    StatementResult result = session.executeStatement(statement);
                    // execute command
                    Stream<Edge> query = session.edges(result);
                    // edges in memory plus the ones in database (return copy since edges can be deleted in the middle of the loop)
                    Iterator<Edge> iterator = Stream.concat((labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(edge -> (Edge)edge), query)
                        .collect(Collectors.toList())
                        .iterator();
                    // process summary (query has been already consumed by combine)
                    ResultSummaryLogger.log(result.consume());
                    // after this line it is safe to update loaded flag and labels in memory
                    inEdgesLoaded = labels.length == 0;
                    inEdgeLabels.addAll(set);
                    // return iterator
                    return iterator;
                }
            }
            // edges in memory (return copy since edges can be deleted in the middle of the loop)
            return inEdges.stream().filter(edge -> labels.length == 0 || set.contains(edge.label()))
                .map(edge -> (Edge)edge)
                .collect(Collectors.toList())
                .iterator();
        }
        // check we have all edges in memory
        if (!outEdgesLoaded || !inEdgesLoaded) {
            // check we have labels already in memory
            if (set.isEmpty() || !outEdgeLabels.containsAll(set) || !inEdgeLabels.containsAll(set)) {
                // create string builder
                StringBuilder builder = new StringBuilder();
                // match clause
                builder.append("MATCH ").append(matchPattern("n")).append("-[r").append(set.stream().map(label -> ":`" + label + "`").collect(Collectors.joining("|"))).append("]-(m").append(processLabels(Collections.emptySet(), true)).append(")").append(" WHERE ").append(vertexIdProvider.matchPredicateOperand("n")).append(" = {id}");
                // edge ids already in memory
                List<Object> identifiers = Stream.concat(outEdges.stream(), inEdges.stream()).map(Neo4JEdge::id).filter(Objects::nonNull).collect(Collectors.toList());
                // process where clause
                processEdgesWhereClause("m", identifiers, "r", builder, parameters);
                // return
                builder.append(" RETURN n, r, m");
                // create statement
                Statement statement = new Statement(builder.toString(), parameters);
                // execute statement
                StatementResult result = session.executeStatement(statement);
                // execute command
                Stream<Edge> query = session.edges(result);
                // edges in memory plus the ones in database (return copy since edges can be deleted in the middle of the loop)
                Iterator<Edge> iterator = Stream.concat(Stream.concat(labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream(), labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(edge -> (Edge)edge), query)
                    .collect(Collectors.toList())
                    .iterator();
                // process summary (query has been already consumed by combine)
                ResultSummaryLogger.log(result.consume());
                // after this line it is safe to update loaded flags
                outEdgesLoaded = outEdgesLoaded || labels.length == 0;
                inEdgesLoaded = inEdgesLoaded || labels.length == 0;
                // update labels in memory
                outEdgeLabels.addAll(set);
                inEdgeLabels.addAll(set);
                // return iterator
                return iterator;
            }
        }
        // edges in memory (return copy since edges can be deleted in the middle of the loop)
        return Stream.concat(labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream(), labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream())
            .map(edge -> (Edge)edge)
            .collect(Collectors.toList())
            .iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction, String... labels) {
        Objects.requireNonNull(direction, "direction cannot be null");
        Objects.requireNonNull(labels, "labels cannot be null");
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // load labels in hash set (remove duplicates)
        Set<String> set = new HashSet<>(Arrays.asList(labels));
        // parameters
        Map<String, Object> parameters = new HashMap<>();
        // vertex id
        parameters.put("id", id());
        // out edges
        if (direction == Direction.OUT) {
            // check we have all edges in memory
            if (!outEdgesLoaded) {
                // labels we need to query for
                Set<String> relationshipLabels = set.stream().filter(item -> !outEdgeLabels.contains(item)).collect(Collectors.toSet());
                // check query is required for labels
                if (set.isEmpty() || !relationshipLabels.isEmpty()) {
                    // create string builder
                    StringBuilder builder = new StringBuilder();
                    // match clause
                    builder.append("MATCH ").append(matchPattern("n")).append("-[r").append(relationshipLabels.stream().map(label -> ":`" + label + "`").collect(Collectors.joining("|"))).append("]->(m").append(processLabels(Collections.emptySet(), true)).append(")").append(" WHERE ").append(vertexIdProvider.matchPredicateOperand("n")).append(" = {id}");
                    // edge ids already in memory
                    List<Object> identifiers = outEdges.stream().map(Neo4JEdge::id).filter(Objects::nonNull).collect(Collectors.toList());
                    // process where clause
                    processEdgesWhereClause("m", identifiers, "r", builder, parameters);
                    // return
                    builder.append(" RETURN m");
                    // create statement
                    Statement statement = new Statement(builder.toString(), parameters);
                    // execute statement
                    StatementResult result = session.executeStatement(statement);
                    // execute command
                    Stream<Vertex> query = session.vertices(result);
                    // return copy since elements can be deleted in the middle of the loop
                    Iterator<Vertex> iterator = Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex), query)
                        .collect(Collectors.toList())
                        .iterator();
                    // process summary (query has been already consumed by collector)
                    ResultSummaryLogger.log(result.consume());
                    // return iterator
                    return iterator;
                }
            }
            // edges in memory (return copy since elements can be deleted in the middle of the loop)
            return (labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex)
                .collect(Collectors.toList())
                .iterator();
        }
        // in edges
        if (direction == Direction.IN) {
            // check we have all edges in memory
            if (!inEdgesLoaded) {
                // labels we need to query for
                Set<String> relationshipLabels = set.stream().filter(item -> !inEdgeLabels.contains(item)).collect(Collectors.toSet());
                // check query is required for labels
                if (set.isEmpty() || !relationshipLabels.isEmpty()) {
                    // create string builder
                    StringBuilder builder = new StringBuilder();
                    // match clause
                    builder.append("MATCH ").append(matchPattern("n")).append("<-[r").append(relationshipLabels.stream().map(label -> ":`" + label + "`").collect(Collectors.joining("|"))).append("]-(m").append(processLabels(Collections.emptySet(), true)).append(")").append(" WHERE ").append(vertexIdProvider.matchPredicateOperand("n")).append(" = {id}");
                    // edge ids already in memory
                    List<Object> identifiers = inEdges.stream().map(Neo4JEdge::id).filter(Objects::nonNull).collect(Collectors.toList());
                    // process where clause
                    processEdgesWhereClause("m", identifiers, "r", builder, parameters);
                    // return
                    builder.append(" RETURN m");
                    // create statement
                    Statement statement = new Statement(builder.toString(), parameters);
                    // execute statement
                    StatementResult result = session.executeStatement(statement);
                    // execute command
                    Stream<Vertex> query = session.vertices(result);
                    // return copy since elements can be deleted in the middle of the loop
                    Iterator<Vertex> iterator = Stream.concat((labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex), query)
                        .collect(Collectors.toList())
                        .iterator();
                    // process summary (query has been already consumed by collector)
                    ResultSummaryLogger.log(result.consume());
                    // return iterator
                    return iterator;
                }
            }
            // edges in memory (return copy since elements can be deleted in the middle of the loop
            return (labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex)
                .collect(Collectors.toList())
                .iterator();
        }
        // check we have all edges in memory
        if (!outEdgesLoaded || !inEdgesLoaded) {
            // check we have labels already in memory
            if (set.isEmpty() || !outEdgeLabels.containsAll(set) || !inEdgeLabels.containsAll(set)) {
                // create string builder
                StringBuilder builder = new StringBuilder();
                // match clause
                builder.append("MATCH ").append(matchPattern("n")).append("-[r").append(set.stream().map(label -> ":`" + label + "`").collect(Collectors.joining("|"))).append("]-(m").append(processLabels(Collections.emptySet(), true)).append(")").append(" WHERE ").append(vertexIdProvider.matchPredicateOperand("n")).append(" = {id}");
                // edge ids already in memory
                List<Object> identifiers = Stream.concat(outEdges.stream(), inEdges.stream()).map(Neo4JEdge::id).filter(Objects::nonNull).collect(Collectors.toList());
                // process where clause
                processEdgesWhereClause("m", identifiers, "r", builder, parameters);
                // return
                builder.append(" RETURN m");
                // create statement
                Statement statement = new Statement(builder.toString(), parameters);
                // execute statement
                StatementResult result = session.executeStatement(statement);
                // execute command
                Stream<Vertex> query = session.vertices(result);
                // return copy since elements can be deleted in the middle of the loop
                Iterator<Vertex> iterator = Stream.concat(Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex), (labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex)), query)
                    .collect(Collectors.toList())
                    .iterator();
                // process summary (query has been already consumed by collector)
                ResultSummaryLogger.log(result.consume());
                // return iterator
                return iterator;
            }
        }
        // edges in memory (return copy since edges can be deleted in the middle of the loop)
        return Stream.concat((labels.length != 0 ? outEdges.stream().filter(edge -> set.contains(edge.label())) : outEdges.stream()).map(Edge::inVertex), (labels.length != 0 ? inEdges.stream().filter(edge -> set.contains(edge.label())) : inEdges.stream()).map(Edge::outVertex))
            .collect(Collectors.toList())
            .iterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String name, V value, Object... keyValues) {
        ElementHelper.validateProperty(name, value);
        ElementHelper.legalPropertyKeyValueArray(keyValues);
        // check key values
        if (keyValues.length != 0)
            throw VertexProperty.Exceptions.metaPropertiesNotSupported();
        // validate bolt support
        Neo4JBoltSupport.checkPropertyValue(value);
        // check cardinality
        VertexProperty.Cardinality existingCardinality = cardinalities.get(name);
        if (existingCardinality != null && existingCardinality != cardinality)
            throw new IllegalArgumentException(String.format(Locale.getDefault(), "Property %s has been defined with %s cardinality", name, existingCardinality));
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // vertex property
        Neo4JVertexProperty<V> property = new Neo4JVertexProperty<>(this, propertyIdProvider.incrementAndGet(), name, value);
        // check cardinality
        switch (cardinality) {
            case list:
                // get existing list for key
                Collection<VertexProperty> list = properties.get(name);
                if (list == null) {
                    // initialize list
                    list = new ArrayList<>();
                    // use list
                    properties.put(name, list);
                    // cardinality
                    cardinalities.put(name, VertexProperty.Cardinality.list);
                }
                // add value to list, this will always call dirty method in session
                if (list.add(property)) {
                    // notify session
                    session.dirtyVertex(this);
                    // update flag
                    dirty = true;
                }
                break;
            case set:
                // get existing set for key
                Collection<VertexProperty> set = properties.get(name);
                if (set == null) {
                    // initialize set
                    set = new HashSet<>();
                    // use set
                    properties.put(name, set);
                    // cardinality
                    cardinalities.put(name, VertexProperty.Cardinality.set);
                }
                // check value does not exist in collection, TODO: optimize this search
                if (set.stream().noneMatch(item -> item.value().equals(value))) {
                    // add property to set
                    set.add(property);
                    // notify session
                    session.dirtyVertex(this);
                    // update flag
                    dirty = true;
                }
                break;
            default:
                // use value (single)
                properties.put(name, Collections.singletonList(property));
                // cardinality
                cardinalities.put(name, VertexProperty.Cardinality.single);
                // notify session
                session.dirtyVertex(this);
                // update flag
                dirty = true;
                break;
        }
        // return property
        return property;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> VertexProperty<V> property(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        // check we have a property with the given key
        Collection<?> collection = properties.get(key);
        if (collection != null) {
            // check size
            if (collection.size() == 1) {
                // iterator
                Iterator<?> iterator = collection.iterator();
                // advance iterator to first element
                if (iterator.hasNext()) {
                    // first element
                    return (VertexProperty<V>)iterator.next();
                }
                return VertexProperty.empty();
            }
            // exception
            throw Vertex.Exceptions.multiplePropertiesExistForProvidedKey(key);
        }
        return VertexProperty.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> Iterator<VertexProperty<V>> properties(String... propertyKeys) {
        Objects.requireNonNull(propertyKeys, "propertyKeys cannot be null");
        // check we have properties with key
        if (!properties.isEmpty()) {
            // no properties in filter
            if (propertyKeys.length == 0) {
                // all properties (return a copy since properties iterator can be modified by calling remove())
                return properties.entrySet().stream()
                    .flatMap(entry -> entry.getValue().stream())
                    .map(item -> (VertexProperty<V>)item)
                    .collect(Collectors.toList())
                    .iterator();
            }
            // one property in filter
            if (propertyKeys.length == 1) {
                // get list for key
                Collection<?> list = properties.get(propertyKeys[0]);
                if (list != null) {
                    // all properties (return a copy since properties iterator can be modified by calling remove())
                    return list.stream()
                        .map(item -> (VertexProperty<V>)item)
                        .collect(Collectors.toList())
                        .iterator();
                }
                // nothing on key
                return Collections.emptyIterator();
            }
            // loop property keys (return a copy since properties iterator can be modified by calling remove())
            return Arrays.stream(propertyKeys)
                .flatMap(key -> ((Collection<?>)properties.getOrDefault(key, Collections.EMPTY_LIST)).stream())
                .map(item -> (VertexProperty<V>)item)
                .collect(Collectors.toList())
                .iterator();
        }
        // nothing
        return Collections.emptyIterator();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Graph graph() {
        return graph;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // remove all edges
        outEdges.forEach(edge -> session.removeEdge(edge, false));
        // remove vertex on session
        session.removeVertex(this);
    }

    void addInEdge(Neo4JEdge edge) {
        Objects.requireNonNull(edge, "edge cannot be null");
        // add to set
        inEdges.add(edge);
    }

    void addOutEdge(Neo4JEdge edge) {
        Objects.requireNonNull(edge, "edge cannot be null");
        // add to set
        outEdges.add(edge);
    }

    private Map<String, Object> statementParameters() {
        // define collector
        Collector<Map.Entry<String, Collection<VertexProperty>>, Map<String, Object>, Map<String, Object>> collector = Collector.of(
            HashMap::new,
            (map, entry) -> {
                // key & value
                String key = entry.getKey();
                Collection<VertexProperty> list = entry.getValue();
                // check cardinality
                if (cardinalities.get(key) == VertexProperty.Cardinality.single) {
                    // iterator
                    Iterator<VertexProperty> iterator = list.iterator();
                    // add single value to map
                    if (iterator.hasNext())
                        map.put(key, iterator.next().value());
                }
                else {
                    // add list of values
                    map.put(key, list.stream().map(Property::value).collect(Collectors.toList()));
                }
            },
            (map1, map2) -> map1,
            (map) -> map
        );
        // process properties
        Map<String, Object> parameters = properties.entrySet().stream().collect(collector);
        // removed properties
        removedProperties.forEach(name -> parameters.put(name, null));
        // append id field if required
        String idFieldName = vertexIdProvider.fieldName();
        if (id != null && idFieldName != null)
            parameters.put(idFieldName, id);
        // return parameters
        return parameters;
    }

    @Override
    public Neo4JDatabaseCommand insertCommand() {
        // concat labels with additional labels on insertion
        SortedSet<String> labels = Stream.concat(this.labels.stream(), additionalLabels.stream()).collect(Collectors.toCollection(TreeSet::new));
        try {
            // parameters
            Value parameters = Values.parameters("vp", statementParameters());
            // check database side id generation is required
            if (id == null) {
                // create statement
                String statement = "CREATE (n" + processLabels(labels, false) + "{vp}) RETURN " + vertexIdProvider.matchPredicateOperand("n");
                // command statement
                return new Neo4JDatabaseCommand(new Statement(statement, parameters), result -> {
                    // check we received data
                    if (result.hasNext()) {
                        // record
                        Record record = result.next();
                        // process node identifier
                        generatedId = vertexIdProvider.processIdentifier(record.get(0).asObject());
                    }
                });
            }
            // command statement
            return new Neo4JDatabaseCommand(new Statement("CREATE (" + processLabels(labels, false) + "{vp})", parameters));
        }
        finally {
            // to find vertex in database (labels + additional labels)
            matchLabels = labels;
        }
    }

    @Override
    public Neo4JDatabaseCommand updateCommand() {
        // check we need to issue statement (adding a label and then removing it will set the vertex as dirty in session but nothing to do)
        if (dirty || !labelsAdded.isEmpty() || !labelsRemoved.isEmpty()) {
            // create builder
            StringBuilder builder = new StringBuilder();
            // parameters
            Map<String, Object> parameters = new HashMap<>();
            // match statement
            builder.append("MATCH ").append(matchPattern("v")).append(" WHERE ").append(matchPredicate("v", "id"));
            // id parameter
            parameters.put("id", id());
            // check vertex is dirty
            if (dirty) {
                // set properties
                builder.append(" SET v = {vp}");
                // update parameters
                parameters.put("vp", statementParameters());
            }
            // check labels were added
            if (!labelsAdded.isEmpty()) {
                // add labels
                builder.append(!dirty ? " SET v" : ", v").append(processLabels(labelsAdded, false));
            }
            // check labels were removed
            if (!labelsRemoved.isEmpty()) {
                // remove labels
                builder.append(" REMOVE v").append(processLabels(labelsRemoved, false));
            }
            // command statement
            return new Neo4JDatabaseCommand(new Statement(builder.toString(), parameters));
        }
        return null;
    }

    @Override
    public Neo4JDatabaseCommand deleteCommand() {
        // create statement
        String statement = "MATCH " + matchPattern("v") + " WHERE " + matchPredicate("v", "id") + " DETACH DELETE v";
        // parameters
        Value parameters = Values.parameters("id", id());
        // command statement
        return new Neo4JDatabaseCommand(new Statement(statement, parameters));
    }

    void commit() {
        // commit labels
        labelsAdded.clear();
        labelsRemoved.clear();
        originalLabels = new TreeSet<>(labels);
        matchLabels = Stream.concat(originalLabels.stream(), graphLabels.stream()).collect(Collectors.toCollection(TreeSet::new));
        // update property values
        originalProperties = new HashMap<>(properties);
        originalCardinalities = new HashMap<>(cardinalities);
        // reset removed properties
        removedProperties.clear();
        // reset flags
        dirty = false;
    }

    void rollback() {
        // restore labels
        labelsAdded.clear();
        labelsRemoved.clear();
        labels.clear();
        labels.addAll(originalLabels);
        matchLabels = Stream.concat(originalLabels.stream(), graphLabels.stream()).collect(Collectors.toCollection(TreeSet::new));
        // restore property values
        properties.clear();
        cardinalities.clear();
        properties.putAll(originalProperties);
        cardinalities.putAll(originalCardinalities);
        // reset removed properties
        removedProperties.clear();
        // reset flags
        outEdgesLoaded = false;
        inEdgesLoaded = false;
        dirty = false;
    }

    private String processLabels(Set<String> labels, boolean addPartition) {
        Objects.requireNonNull(labels, "labels cannot be null");
        // check we need to include partition in match
        if (addPartition) {
            // get labels from read partition to be applied in vertex patterns
            Set<String> partitionLabels = partition.vertexMatchPatternLabels();
            if (!partitionLabels.isEmpty()) {
                // make sure partition is in match pattern
                return Stream.concat(partitionLabels.stream(), labels.stream()).map(label -> ":`" + label + "`").collect(Collectors.joining(""));
            }
        }
        // labels
        return labels.stream().map(label -> ":`" + label + "`").collect(Collectors.joining(""));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object object) {
        // ElementHelper.areEqual is implemented on this.id(), handle the case of generated ids
        return object instanceof Vertex && (id != null ? ElementHelper.areEqual(this, object) : super.equals(object));
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int hashCode() {
        // ElementHelper.hashCode() is implemented on this.id(), handle the case of generated ids
        return id != null ? ElementHelper.hashCode(this) : super.hashCode();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String toString() {
        return StringFactory.vertexString(this);
    }
}
