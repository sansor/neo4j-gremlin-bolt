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

import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Value;
import org.neo4j.driver.v1.Values;
import org.neo4j.driver.v1.summary.ResultSummary;
import org.neo4j.driver.v1.types.Entity;
import org.neo4j.driver.v1.types.Relationship;

import java.util.Collections;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class Neo4JEdgeWhileCreatingUpdateDeleteTest {

    @Mock
    private Neo4JGraph graph;

    @Mock
    private Neo4JSession session;

    @Mock
    private Neo4JVertex outVertex;

    @Mock
    private Neo4JVertex inVertex;

    @Mock
    private Relationship relationship;

    @Mock
    private Neo4JElementIdProvider edgeIdProvider;

    @Mock
    private Transaction transaction;

    @Mock
    private StatementResult statementResult;

    @Mock
    private Record record;

    @Mock
    private Entity entity;

    @Mock
    private Value value;

    @Mock
    private ResultSummary resultSummary;

    @Test
    public void givenNotDirtyRelationShouldReturnNull() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(outVertex.matchPattern(Mockito.any())).thenAnswer(invocation -> "(o)");
        Mockito.when(outVertex.matchPredicate(Mockito.any(), Mockito.any())).thenAnswer(invocation -> "ID(o) = {oid}");
        Mockito.when(outVertex.id()).thenAnswer(invocation -> 1L);
        Mockito.when(inVertex.matchPattern(Mockito.any())).thenAnswer(invocation -> "(i)");
        Mockito.when(inVertex.matchPredicate(Mockito.any(), Mockito.any())).thenAnswer(invocation -> "ID(i) = {iid}");
        Mockito.when(inVertex.id()).thenAnswer(invocation -> 2L);
        Mockito.when(relationship.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(relationship.type()).thenAnswer(invocation -> "label");
        Mockito.when(relationship.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(relationship.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 3L);
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> "id");
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "r.id");
        Mockito.when(statementResult.hasNext()).thenAnswer(invocation -> true);
        Mockito.when(statementResult.next()).thenAnswer(invocation -> record);
        Mockito.when(record.get(Mockito.eq(0))).thenAnswer(invocation -> value);
        Mockito.when(value.asEntity()).thenAnswer(invocation -> entity);
        Neo4JEdge edge = new Neo4JEdge(graph, session, edgeIdProvider, outVertex, relationship, inVertex);
        // act
        Neo4JDatabaseCommand command = edge.updateCommand();
        // assert
        Assert.assertNull("Failed to create update command", command);
    }

    @Test
    public void givenNewPropertyShouldCreateUpdateCommand() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(outVertex.matchPattern(Mockito.any())).thenAnswer(invocation -> "(o)");
        Mockito.when(outVertex.matchPredicate(Mockito.any(), Mockito.any())).thenAnswer(invocation -> "ID(o) = {oid}");
        Mockito.when(outVertex.id()).thenAnswer(invocation -> 1L);
        Mockito.when(outVertex.matchStatement(Mockito.anyString(), Mockito.anyString())).thenAnswer(invocation -> "MATCH (o) WHERE ID(o) = {oid}");
        Mockito.when(inVertex.matchPattern(Mockito.any())).thenAnswer(invocation -> "(i)");
        Mockito.when(inVertex.matchPredicate(Mockito.any(), Mockito.any())).thenAnswer(invocation -> "ID(i) = {iid}");
        Mockito.when(inVertex.id()).thenAnswer(invocation -> 2L);
        Mockito.when(inVertex.matchStatement(Mockito.anyString(), Mockito.anyString())).thenAnswer(invocation -> "MATCH (i) WHERE ID(i) = {iid}");
        Mockito.when(relationship.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(relationship.type()).thenAnswer(invocation -> "label");
        Mockito.when(relationship.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(relationship.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 3L);
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> null);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "ID(r)");
        Mockito.when(statementResult.hasNext()).thenAnswer(invocation -> true);
        Mockito.when(statementResult.next()).thenAnswer(invocation -> record);
        Mockito.when(record.get(Mockito.eq(0))).thenAnswer(invocation -> value);
        Mockito.when(value.asEntity()).thenAnswer(invocation -> entity);
        Neo4JEdge edge = new Neo4JEdge(graph, session, edgeIdProvider, outVertex, relationship, inVertex);
        edge.property("key1", "value1");
        // act
        Neo4JDatabaseCommand command = edge.updateCommand();
        // assert
        Assert.assertNotNull("Failed to create insert command", command);
        Assert.assertNotNull("Failed to create insert command statement", command.getStatement());
        Assert.assertEquals("Invalid insert command statement", command.getStatement().text(), "MATCH (o) WHERE ID(o) = {oid} MATCH (i) WHERE ID(i) = {iid} MATCH (o)-[r:`label`]->(i) WHERE ID(r) = {id} SET r = {rp}");
        Assert.assertEquals("Invalid insert command statement", command.getStatement().parameters(), Values.parameters("oid", 1L, "iid", 2L, "id", 3L, "rp", Collections.singletonMap("key1", "value1")));
        Assert.assertNotNull("Failed to create insert command callback", command.getCallback());
        // invoke callback
        command.getCallback().accept(statementResult);
        // assert
        Assert.assertNotNull("Failed get node identifier", edge.id());
    }

    @Test
    public void givenRemovedPropertyShouldCreateUpdateCommand() {
        // arrange
        Mockito.when(graph.tx()).thenAnswer(invocation -> transaction);
        Mockito.when(outVertex.matchPattern(Mockito.any())).thenAnswer(invocation -> "(o)");
        Mockito.when(outVertex.matchPredicate(Mockito.any(), Mockito.any())).thenAnswer(invocation -> "ID(o) = {oid}");
        Mockito.when(outVertex.id()).thenAnswer(invocation -> 1L);
        Mockito.when(outVertex.matchStatement(Mockito.anyString(), Mockito.anyString())).thenAnswer(invocation -> "MATCH (o) WHERE ID(o) = {oid}");
        Mockito.when(inVertex.matchPattern(Mockito.any())).thenAnswer(invocation -> "(i)");
        Mockito.when(inVertex.matchPredicate(Mockito.any(), Mockito.any())).thenAnswer(invocation -> "ID(i) = {iid}");
        Mockito.when(inVertex.id()).thenAnswer(invocation -> 2L);
        Mockito.when(inVertex.matchStatement(Mockito.anyString(), Mockito.anyString())).thenAnswer(invocation -> "MATCH (i) WHERE ID(i) = {iid}");
        Mockito.when(relationship.get(Mockito.eq("id"))).thenAnswer(invocation -> Values.value(1L));
        Mockito.when(relationship.type()).thenAnswer(invocation -> "label");
        Mockito.when(relationship.keys()).thenAnswer(invocation -> Collections.singleton("key1"));
        Mockito.when(relationship.get(Mockito.eq("key1"))).thenAnswer(invocation -> Values.value("value1"));
        Mockito.when(edgeIdProvider.get(Mockito.any())).thenAnswer(invocation -> 3L);
        Mockito.when(edgeIdProvider.fieldName()).thenAnswer(invocation -> null);
        Mockito.when(edgeIdProvider.matchPredicateOperand(Mockito.anyString())).thenAnswer(invocation -> "ID(r)");
        Mockito.when(statementResult.hasNext()).thenAnswer(invocation -> true);
        Mockito.when(statementResult.next()).thenAnswer(invocation -> record);
        Mockito.when(record.get(Mockito.eq(0))).thenAnswer(invocation -> value);
        Mockito.when(value.asEntity()).thenAnswer(invocation -> entity);
        Neo4JEdge edge = new Neo4JEdge(graph, session, edgeIdProvider, outVertex, relationship, inVertex);
        edge.property("key1").remove();
        // act
        Neo4JDatabaseCommand command = edge.updateCommand();
        // assert
        Assert.assertNotNull("Failed to create insert command", command);
        Assert.assertNotNull("Failed to create insert command statement", command.getStatement());
        Assert.assertEquals("Invalid insert command statement", command.getStatement().text(), "MATCH (o) WHERE ID(o) = {oid} MATCH (i) WHERE ID(i) = {iid} MATCH (o)-[r:`label`]->(i) WHERE ID(r) = {id} SET r = {rp}");
        Assert.assertEquals("Invalid insert command statement", command.getStatement().parameters(), Values.parameters("oid", 1L, "iid", 2L, "id", 3L, "rp", Collections.singletonMap("key1", null)));
        Assert.assertNotNull("Failed to create insert command callback", command.getCallback());
        // invoke callback
        command.getCallback().accept(statementResult);
        // assert
        Assert.assertNotNull("Failed get node identifier", edge.id());
    }
}
