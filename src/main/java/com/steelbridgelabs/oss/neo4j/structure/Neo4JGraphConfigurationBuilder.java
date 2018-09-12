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

import org.apache.commons.configuration.BaseConfiguration;
import org.apache.commons.configuration.Configuration;

import java.util.Objects;
import java.util.UUID;

/**
 * @author Rogelio J. Baucells
 */
public final class Neo4JGraphConfigurationBuilder {

    public static final String Neo4JGraphNameConfigurationKey = "neo4j.graph.name";
    public static final String Neo4JIdentifierConfigurationKey = "neo4j.identifier";
    public static final String Neo4JUrlConfigurationKey = "neo4j.url";
    public static final String Neo4JHostnameConfigurationKey = "neo4j.hostname";
    public static final String Neo4JPortConfigurationKey = "neo4j.port";
    public static final String Neo4JUsernameConfigurationKey = "neo4j.username";
    public static final String Neo4JPasswordConfigurationKey = "neo4j.password";
    public static final String Neo4JReadonlyConfigurationKey = "neo4j.readonly";
    public static final String Neo4JVertexIdProviderClassNameConfigurationKey = "neo4j.vertexIdProvider";
    public static final String Neo4JEdgeIdProviderClassNameConfigurationKey = "neo4j.edgeIdProvider";
    public static final String Neo4JPropertyIdProviderClassNameConfigurationKey = "neo4j.propertyIdProvider";

    private final String hostname;
    private final short port;
    private final String username;
    private final String password;
    private final boolean readonly;
    private String graphName;
    private String identifier;
    private String vertexIdProviderClassName = null;
    private String edgeIdProviderClassName = null;
    private String propertyIdProviderClassName = null;
    private String elementIdProviderClassName = null;

    private Neo4JGraphConfigurationBuilder(String hostname, short port, String username, String password, boolean readonly) {
        Objects.requireNonNull(hostname, "hostname cannot be null");
        Objects.requireNonNull(username, "username cannot be null");
        Objects.requireNonNull(password, "password cannot be null");
        // store fields
        this.hostname = hostname;
        this.port = port;
        this.username = username;
        this.password = password;
        this.readonly = readonly;
    }

    public static Neo4JGraphConfigurationBuilder connect(String hostname, short port, String username, String password) {
        // create builder instance
        return new Neo4JGraphConfigurationBuilder(hostname, port, username, password, false);
    }

    public static Neo4JGraphConfigurationBuilder connect(String hostname, short port, String username, String password, boolean readonly) {
        // create builder instance
        return new Neo4JGraphConfigurationBuilder(hostname, port, username, password, readonly);
    }

    public static Neo4JGraphConfigurationBuilder connect(String hostname, String username, String password) {
        // create builder instance
        return new Neo4JGraphConfigurationBuilder(hostname, (short)7687, username, password, false);
    }

    public static Neo4JGraphConfigurationBuilder connect(String hostname, String username, String password, boolean readonly) {
        // create builder instance
        return new Neo4JGraphConfigurationBuilder(hostname, (short)7687, username, password, readonly);
    }

    public Neo4JGraphConfigurationBuilder withIdentifier(String identifier) {
        Objects.requireNonNull(identifier, "identifier cannot be null");
        // store identifier
        this.identifier = identifier;
        // return builder
        return this;
    }

    public Neo4JGraphConfigurationBuilder withName(String graphName) {
        // store name
        this.graphName = graphName;
        // return builder
        return this;
    }

    public Neo4JGraphConfigurationBuilder withVertexIdProvider(Class<?> provider) {
        Objects.requireNonNull(provider, "provider cannot be null");
        // store class name
        vertexIdProviderClassName = provider.getName();
        // return builder
        return this;
    }

    public Neo4JGraphConfigurationBuilder withEdgeIdProvider(Class<?> provider) {
        Objects.requireNonNull(provider, "provider cannot be null");
        // store class name
        edgeIdProviderClassName = provider.getName();
        // return builder
        return this;
    }

    public Neo4JGraphConfigurationBuilder withPropertyIdProvider(Class<?> provider) {
        Objects.requireNonNull(provider, "provider cannot be null");
        // store class name
        propertyIdProviderClassName = provider.getName();
        // return builder
        return this;
    }

    public Neo4JGraphConfigurationBuilder withElementIdProvider(Class<?> provider) {
        Objects.requireNonNull(provider, "provider cannot be null");
        // store class name
        elementIdProviderClassName = provider.getName();
        // return builder
        return this;
    }

    public Configuration build() {
        // create configuration instance
        Configuration configuration = new BaseConfiguration();
        // identifier
        configuration.setProperty(Neo4JIdentifierConfigurationKey, identifier != null ? identifier : UUID.randomUUID().toString());
        // url
        configuration.setProperty(Neo4JUrlConfigurationKey, "bolt://" + hostname + ":" + port);
        // hostname
        configuration.setProperty(Neo4JHostnameConfigurationKey, hostname);
        // port
        configuration.setProperty(Neo4JPortConfigurationKey, port);
        // username
        configuration.setProperty(Neo4JUsernameConfigurationKey, username);
        // password
        configuration.setProperty(Neo4JPasswordConfigurationKey, password);
        // readonly
        configuration.setProperty(Neo4JReadonlyConfigurationKey, readonly);
        // graphName
        configuration.setProperty(Neo4JGraphNameConfigurationKey, graphName);
        // vertex id provider
        configuration.setProperty(Neo4JVertexIdProviderClassNameConfigurationKey, vertexIdProviderClassName != null ? vertexIdProviderClassName : elementIdProviderClassName);
        // edge id provider
        configuration.setProperty(Neo4JEdgeIdProviderClassNameConfigurationKey, edgeIdProviderClassName != null ? edgeIdProviderClassName : elementIdProviderClassName);
        // property id provider
        configuration.setProperty(Neo4JPropertyIdProviderClassNameConfigurationKey, propertyIdProviderClassName != null ? propertyIdProviderClassName : elementIdProviderClassName);
        // return configuration
        return configuration;
    }
}
