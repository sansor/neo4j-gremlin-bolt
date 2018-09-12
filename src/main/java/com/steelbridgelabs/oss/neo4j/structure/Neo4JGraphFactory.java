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

import com.steelbridgelabs.oss.neo4j.structure.partitions.AnyLabelReadPartition;
import com.steelbridgelabs.oss.neo4j.structure.partitions.NoReadPartition;
import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.neo4j.driver.v1.AuthTokens;
import org.neo4j.driver.v1.Config;
import org.neo4j.driver.v1.Driver;
import org.neo4j.driver.v1.GraphDatabase;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author Rogelio J. Baucells
 */
public class Neo4JGraphFactory {

    private static final Map<String, Driver> instances = new ConcurrentHashMap<>();

    public static Graph open(Configuration configuration) {
        if (configuration == null)
            throw Graph.Exceptions.argumentCanNotBeNull("configuration");
        try {
            // graph name
            String graphName = configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JGraphNameConfigurationKey);
            // create driver instance
            Driver driver = createDriverInstance(configuration);
            // create providers
            Neo4JElementIdProvider<?> vertexIdProvider = loadProvider(driver, configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JVertexIdProviderClassNameConfigurationKey));
            Neo4JElementIdProvider<?> edgeIdProvider = loadProvider(driver, configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JEdgeIdProviderClassNameConfigurationKey));
            // readonly
            boolean readonly = configuration.getBoolean(Neo4JGraphConfigurationBuilder.Neo4JReadonlyConfigurationKey);
            // graph instance
            Neo4JGraph graph;
            // check a read partition is required
            if (graphName != null)
                graph = new Neo4JGraph(new AnyLabelReadPartition(graphName), new String[]{graphName}, driver, vertexIdProvider, edgeIdProvider, configuration, readonly);
            else
                graph = new Neo4JGraph(new NoReadPartition(), new String[]{}, driver, vertexIdProvider, edgeIdProvider, configuration, readonly);
            // return graph instance
            return graph;
        }
        catch (Throwable ex) {
            // throw runtime exception
            throw new RuntimeException("Error creating Graph instance from configuration", ex);
        }
    }

    static Driver createDriverInstance(Configuration configuration) {
        Objects.requireNonNull(configuration, "configuration cannot be null");
        // identifier
        String identifier = Objects.requireNonNull(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JIdentifierConfigurationKey), "Configuration does not contain identifier value");
        // check we have created an instance for this identifier
        return instances.computeIfAbsent(identifier, key -> {
            // neo4j driver configuration
            Config config = Config.build()
                .toConfig();
            // create driver instance
            return GraphDatabase.driver(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JUrlConfigurationKey), AuthTokens.basic(configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JUsernameConfigurationKey), configuration.getString(Neo4JGraphConfigurationBuilder.Neo4JPasswordConfigurationKey)), config);
        });
    }

    static Neo4JElementIdProvider<?> loadProvider(Driver driver, String className) throws ClassNotFoundException, IllegalAccessException, InstantiationException {
        // check class name
        if (className != null) {
            // load class
            Class<?> type = Class.forName(className);
            try {
                // check class has constructor with a Driver parameter
                Constructor<?> constructor = type.getConstructor(Driver.class);
                // create instance
                return (Neo4JElementIdProvider<?>)constructor.newInstance(driver);
            }
            catch (NoSuchMethodException | InvocationTargetException ex) {
                // create instance
                return (Neo4JElementIdProvider<?>)type.newInstance();
            }
        }
        return null;
    }
}
