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

import org.apache.tinkerpop.gremlin.structure.Direction;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.util.ElementHelper;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.types.Relationship;

import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

/**
 * @author Rogelio J. Baucells
 */
public class Neo4JEdge extends Neo4JElement implements Edge {

    private static class Neo4JEdgeProperty<T> implements Property<T> {

        private final Neo4JEdge edge;
        private final String name;
        private final T value;

        Neo4JEdgeProperty(Neo4JEdge edge, String name, T value) {
            Objects.requireNonNull(edge, "edge cannot be null");
            Objects.requireNonNull(name, "name cannot be null");
            Objects.requireNonNull(value, "value cannot be null");
            // store fields
            this.edge = edge;
            this.name = name;
            this.value = value;
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
        public Element element() {
            return edge;
        }

        @Override
        public void remove() {
            // remove from edge
            edge.properties.remove(name);
            // mark property as removed
            edge.removedProperties.add(name);
            // mark edge as dirty
            edge.dirty = true;
            // notify session
            edge.session.dirtyEdge(edge);
        }

        @Override
        public boolean equals(final Object object) {
            return object instanceof Property && ElementHelper.areEqual(this, object);
        }

        @Override
        public int hashCode() {
            return ElementHelper.hashCode(this);
        }

        @Override
        public String toString() {
            return StringFactory.propertyString(this);
        }
    }

    private final Object id;
    private final Neo4JGraph graph;
    private final Neo4JSession session;
    private final Neo4JElementIdProvider<?> edgeIdProvider;
    private final Map<String, Neo4JEdgeProperty> properties = new HashMap<>();
    private final String label;
    private final Neo4JVertex out;
    private final Neo4JVertex in;

    private Object generatedId = null;
    private boolean dirty = false;
    private boolean newEdge;
    private Set<String> removedProperties = new HashSet<>();
    private Map<String, Neo4JEdgeProperty> originalProperties;

    Neo4JEdge(Neo4JGraph graph, Neo4JSession session, Neo4JElementIdProvider<?> edgeIdProvider, String label, Neo4JVertex out, Neo4JVertex in) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(label, "label cannot be null");
        Objects.requireNonNull(properties, "properties cannot be null");
        Objects.requireNonNull(out, "out cannot be null");
        Objects.requireNonNull(in, "in cannot be null");
        // store fields
        this.graph = graph;
        this.session = session;
        this.edgeIdProvider = edgeIdProvider;
        this.label = label;
        this.out = out;
        this.in = in;
        // generate id
        this.id = edgeIdProvider.generate();
        // initialize original properties
        originalProperties = new HashMap<>();
        // this is a new edge (transient)
        newEdge = true;
    }

    Neo4JEdge(Neo4JGraph graph, Neo4JSession session, Neo4JElementIdProvider<?> edgeIdProvider, Neo4JVertex out, Relationship relationship, Neo4JVertex in) {
        Objects.requireNonNull(graph, "graph cannot be null");
        Objects.requireNonNull(session, "session cannot be null");
        Objects.requireNonNull(edgeIdProvider, "edgeIdProvider cannot be null");
        Objects.requireNonNull(out, "out cannot be null");
        Objects.requireNonNull(relationship, "relationship cannot be null");
        Objects.requireNonNull(in, "in cannot be null");
        // store fields
        this.graph = graph;
        this.session = session;
        this.edgeIdProvider = edgeIdProvider;
        // from relationship
        this.id = edgeIdProvider.get(relationship);
        this.label = relationship.type();
        // id field name (if any)
        String idFieldName = edgeIdProvider.fieldName();
        // copy properties from relationship, remove idFieldName from map
        StreamSupport.stream(relationship.keys().spliterator(), false).filter(key -> !key.equals(idFieldName)).forEach(key -> {
            // value
            Value value = relationship.get(key);
            // add property value
            properties.put(key, new Neo4JEdgeProperty<>(this, key, value.asObject()));
        });
        // vertices
        this.out = out;
        this.in = in;
        // initialize original properties
        originalProperties = new HashMap<>(properties);
        // this is a persisted edge
        newEdge = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // out direction
        if (direction == Direction.OUT)
            return Stream.of((Vertex)out).iterator();
        // in direction
        if (direction == Direction.IN)
            return Stream.of((Vertex)in).iterator();
        // both
        return Stream.of((Vertex)out, in).iterator();
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
        return label;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public boolean isDirty() {
        return dirty;
    }

    @Override
    public boolean isTransient() {
        return newEdge;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public <V> Property<V> property(String name, V value) {
        ElementHelper.validateProperty(name, value);
        // validate bolt support
        Neo4JBoltSupport.checkPropertyValue(value);
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // property value for key
        Neo4JEdgeProperty<V> propertyValue = new Neo4JEdgeProperty<>(this, name, value);
        // update map
        properties.put(name, propertyValue);
        // set edge as dirty
        session.dirtyEdge(this);
        // update flag
        dirty = true;
        // return property
        return propertyValue;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> Property<V> property(String key) {
        Objects.requireNonNull(key, "key cannot be null");
        // property value
        Neo4JEdgeProperty propertyValue = properties.get(key);
        if (propertyValue != null)
            return (Property<V>)propertyValue;
        // empty property
        return Property.empty();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void remove() {
        // transaction should be ready for io operations
        graph.tx().readWrite();
        // remove edge on session
        session.removeEdge(this, true);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    @SuppressWarnings("unchecked")
    public <V> Iterator<Property<V>> properties(String... propertyKeys) {
        Objects.requireNonNull(propertyKeys, "propertyKeys cannot be null");
        // check filter is a single property
        if (propertyKeys.length == 1) {
            // property value
            Property<V> propertyValue = properties.get(propertyKeys[0]);
            if (propertyValue != null) {
                // return iterator
                return Collections.singleton(propertyValue).iterator();
            }
            return Collections.emptyIterator();
        }
        // no properties in filter
        if (propertyKeys.length == 0) {
            // all properties (return a copy since properties iterator can be modified by calling remove())
            return properties.values().stream()
                .map(value -> (Property<V>)value)
                .collect(Collectors.toList())
                .iterator();
        }
        // filter properties (return a copy since properties iterator can be modified by calling remove())
        return Arrays.stream(propertyKeys)
            .map(key -> (Property<V>)properties.get(key))
            .filter(Objects::nonNull)
            .collect(Collectors.toList())
            .iterator();
    }

    private Map<String, Object> statementParameters() {
        // process properties
        Map<String, Object> parameters = properties.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, entry -> entry.getValue().value()));
        // removed properties
        removedProperties.forEach(name -> parameters.put(name, null));
        // append id field if required
        String idFieldName = edgeIdProvider.fieldName();
        if (id != null && idFieldName != null)
            parameters.put(idFieldName, id);
        // return parameters
        return parameters;
    }

    @Override
    public Neo4JDatabaseCommand insertCommand() {
        // parameters
        Value parameters = Values.parameters("oid", out.id(), "iid", in.id(), "ep", statementParameters());
        // check database side id generation is required
        if (id == null) {
            // create statement
            String statement = out.matchStatement("o", "oid") + " " + in.matchStatement("i", "iid") + " CREATE (o)-[r:`" + label + "`{ep}]->(i) RETURN " + edgeIdProvider.matchPredicateOperand("r");
            // command statement
            return new Neo4JDatabaseCommand(new Statement(statement, parameters), result -> {
                // check we received data
                if (result.hasNext()) {
                    // record
                    Record record = result.next();
                    // process node identifier
                    generatedId = edgeIdProvider.processIdentifier(record.get(0).asObject());
                }
            });
        }
        // create statement
        String statement = out.matchStatement("o", "oid") + " " + in.matchStatement("i", "iid") + " CREATE (o)-[:`" + label + "`{ep}]->(i)";
        // command statement
        return new Neo4JDatabaseCommand(new Statement(statement, parameters));
    }

    @Override
    public Neo4JDatabaseCommand updateCommand() {
        // check edge is dirty
        if (dirty) {
            // update statement
            String statement = out.matchStatement("o", "oid") + " " + in.matchStatement("i", "iid") + " MATCH (o)-[r:`" + label + "`]->(i)" + " WHERE " + edgeIdProvider.matchPredicateOperand("r") + " = {id} SET r = {rp}";
            // parameters
            Value parameters = Values.parameters("oid", out.id(), "iid", in.id(), "id", id(), "rp", statementParameters());
            // command statement
            return new Neo4JDatabaseCommand(new Statement(statement, parameters), result -> {
            });
        }
        return null;
    }

    @Override
    public Neo4JDatabaseCommand deleteCommand() {
        // delete statement
        String statement = out.matchStatement("o", "oid") + " " + in.matchStatement("i", "iid") + " MATCH (o)-[r:`" + label + "`]->(i)" + " WHERE " + edgeIdProvider.matchPredicateOperand("r") + " = {id} DELETE r";
        // parameters
        Value parameters = Values.parameters("oid", out.id(), "iid", in.id(), "id", id());
        // command statement
        return new Neo4JDatabaseCommand(new Statement(statement, parameters), result -> {
        });
    }

    void commit() {
        // commit property values
        originalProperties = new HashMap<>(properties);
        // reset removed properties
        removedProperties.clear();
        // reset flags
        dirty = false;
        // this is no longer a transient edge
        newEdge = false;
    }

    void rollback() {
        // restore edge references
        out.addOutEdge(this);
        in.addInEdge(this);
        // restore property values
        properties.clear();
        properties.putAll(originalProperties);
        // reset removed properties
        removedProperties.clear();
        // reset flags
        dirty = false;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public boolean equals(final Object object) {
        // ElementHelper.areEqual is implemented on this.id(), handle the case of generated ids
        return object instanceof Edge && (id != null ? ElementHelper.areEqual(this, object) : super.equals(object));
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
        return StringFactory.edgeString(this);
    }
}
