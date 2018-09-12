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

import org.neo4j.driver.v1.Statement;
import org.neo4j.driver.v1.StatementResult;

import java.util.function.Consumer;

/**
 * @author Rogelio J. Baucells
 */
class Neo4JDatabaseCommand {

    private static final Consumer<StatementResult> noop = result -> {
    };

    private final Statement statement;
    private final Consumer<StatementResult> callback;

    public Neo4JDatabaseCommand(Statement statement) {
        this.statement = statement;
        this.callback = noop;
    }

    public Neo4JDatabaseCommand(Statement statement, Consumer<StatementResult> callback) {
        this.statement = statement;
        this.callback = callback;
    }

    public Statement getStatement() {
        return statement;
    }

    public Consumer<StatementResult> getCallback() {
        return callback;
    }
}
