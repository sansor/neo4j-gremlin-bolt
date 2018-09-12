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

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;
import org.neo4j.driver.v1.Transaction;
import org.neo4j.driver.v1.Values;

/**
 * @author Rogelio J. Baucells
 */
@RunWith(MockitoJUnitRunner.class)
public class DatabaseSequenceElementIdProviderWhileGeneratingIdTest {

    @Mock
    private Record record;

    @Mock
    private StatementResult result;

    @Mock
    private Transaction transaction;

    @Mock
    private Session session;

    @Mock
    private Driver driver;

    @Test
    public void givenANewProviderShouldRequestPoolOfIdentifiers() {
        // arrange
        Mockito.when(record.get(Mockito.eq(0))).thenAnswer(invocation -> Values.value(2));
        Mockito.when(result.hasNext()).thenAnswer(invocation -> true);
        Mockito.when(result.next()).thenAnswer(invocation -> record);
        Mockito.when(transaction.run(Mockito.any(Statement.class))).thenAnswer(invocation -> result);
        Mockito.when(session.beginTransaction()).thenAnswer(invocation -> transaction);
        Mockito.when(driver.session()).thenAnswer(invocation -> session);
        DatabaseSequenceElementIdProvider provider = new DatabaseSequenceElementIdProvider(driver, 2, "field1", "label");
        // act
        Long id = provider.generate();
        // assert
        Assert.assertNotNull("Invalid identifier value", id);
        Assert.assertTrue("Provider returned an invalid identifier value", id == 1L);
        // act
        id = provider.generate();
        // assert
        Assert.assertNotNull("Invalid identifier value", id);
        Assert.assertTrue("Provider returned an invalid identifier value", id == 2L);
    }

    @Test
    public void givenTwoIdentifierRequestsShouldRequestPoolOfIdentifiers() {
        // arrange
        Mockito.when(record.get(Mockito.eq(0))).thenAnswer(invocation -> Values.value(1));
        Mockito.when(result.hasNext()).thenAnswer(invocation -> true);
        Mockito.when(result.next()).thenAnswer(invocation -> record);
        Mockito.when(transaction.run(Mockito.any(Statement.class))).thenAnswer(invocation -> result);
        Mockito.when(session.beginTransaction()).thenAnswer(invocation -> transaction);
        Mockito.when(driver.session()).thenAnswer(invocation -> session);
        DatabaseSequenceElementIdProvider provider = new DatabaseSequenceElementIdProvider(driver, 1, "field1", "label");
        // act
        Long id = provider.generate();
        // assert
        Assert.assertNotNull("Invalid identifier value", id);
        Assert.assertTrue("Provider returned an invalid identifier value", id == 1L);
        // arrange
        Mockito.when(record.get(Mockito.eq(0))).thenAnswer(invocation -> Values.value(2));
        // act
        id = provider.generate();
        // assert
        Assert.assertNotNull("Invalid identifier value", id);
        Assert.assertTrue("Provider returned an invalid identifier value", id == 2L);
    }
}
