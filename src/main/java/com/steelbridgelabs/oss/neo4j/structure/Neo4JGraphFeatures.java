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

import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;
import org.apache.tinkerpop.gremlin.structure.util.FeatureDescriptor;
import org.apache.tinkerpop.gremlin.structure.util.StringFactory;

/**
 * @author Rogelio J. Baucells
 */
public final class Neo4JGraphFeatures implements Graph.Features {

    private static class Neo4JGraphGraphFeatures implements GraphFeatures {

        private static class Neo4JVariableFeatures implements VariableFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_VALUES)
            public boolean supportsBooleanValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_VALUES)
            public boolean supportsDoubleValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public boolean supportsIntegerValues() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_VALUES)
            public boolean supportsLongValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_VALUES)
            public boolean supportsStringValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        private VariableFeatures variableFeatures = new Neo4JVariableFeatures();

        @Override
        public VariableFeatures variables() {
            return variableFeatures;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_COMPUTER)
        public boolean supportsComputer() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_THREADED_TRANSACTIONS)
        public boolean supportsThreadedTransactions() {
            return false;
        }
    }

    private static class Neo4JVertexFeatures extends Neo4JElementFeatures implements VertexFeatures {

        private static class Neo4JVertexPropertyFeatures implements VertexPropertyFeatures {

            private final boolean readonly;

            Neo4JVertexPropertyFeatures(boolean readonly) {
                this.readonly = readonly;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
            public boolean supportsRemoveProperty() {
                return !readonly;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
            public boolean supportsUserSuppliedIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_IDS)
            public boolean supportsStringIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UUID_IDS)
            public boolean supportsUuidIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
            public boolean supportsCustomIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_ANY_IDS)
            public boolean supportsAnyIds() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public boolean supportsIntegerValues() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        private final VertexPropertyFeatures vertexPropertyFeatures;
        private final boolean readonly;

        Neo4JVertexFeatures(boolean readonly) {
            super(readonly);
            // initialize fields
            this.readonly = readonly;
            this.vertexPropertyFeatures = new Neo4JVertexPropertyFeatures(readonly);
        }

        @Override
        @FeatureDescriptor(name = FEATURE_ADD_VERTICES)
        public boolean supportsAddVertices() {
            return !readonly;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_REMOVE_VERTICES)
        public boolean supportsRemoveVertices() {
            return !readonly;
        }

        @Override
        public VertexPropertyFeatures properties() {
            return vertexPropertyFeatures;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_META_PROPERTIES)
        public boolean supportsMetaProperties() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_MULTI_PROPERTIES)
        public boolean supportsMultiProperties() {
            return false;
        }

        @Override
        public VertexProperty.Cardinality getCardinality(final String key) {
            return VertexProperty.Cardinality.single;
        }
    }

    private static class Neo4JEdgeFeatures extends Neo4JElementFeatures implements EdgeFeatures {

        private static class Neo4JEdgePropertyFeatures implements EdgePropertyFeatures {

            @Override
            @FeatureDescriptor(name = FEATURE_SERIALIZABLE_VALUES)
            public boolean supportsSerializableValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_VALUES)
            public boolean supportsByteValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_VALUES)
            public boolean supportsFloatValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_VALUES)
            public boolean supportsIntegerValues() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MAP_VALUES)
            public boolean supportsMapValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_MIXED_LIST_VALUES)
            public boolean supportsMixedListValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BOOLEAN_ARRAY_VALUES)
            public boolean supportsBooleanArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_BYTE_ARRAY_VALUES)
            public boolean supportsByteArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_DOUBLE_ARRAY_VALUES)
            public boolean supportsDoubleArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_FLOAT_ARRAY_VALUES)
            public boolean supportsFloatArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_INTEGER_ARRAY_VALUES)
            public boolean supportsIntegerArrayValues() {
                return true;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_STRING_ARRAY_VALUES)
            public boolean supportsStringArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_LONG_ARRAY_VALUES)
            public boolean supportsLongArrayValues() {
                return false;
            }

            @Override
            @FeatureDescriptor(name = FEATURE_UNIFORM_LIST_VALUES)
            public boolean supportsUniformListValues() {
                return false;
            }
        }

        private final EdgePropertyFeatures edgePropertyFeatures = new Neo4JEdgePropertyFeatures();
        private final boolean readonly;

        Neo4JEdgeFeatures(boolean readonly) {
            super(readonly);
            this.readonly = readonly;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_ADD_EDGES)
        public boolean supportsAddEdges() {
            return !readonly;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_REMOVE_EDGES)
        public boolean supportsRemoveEdges() {
            return !readonly;
        }

        @Override
        public EdgePropertyFeatures properties() {
            return edgePropertyFeatures;
        }
    }

    private static class Neo4JElementFeatures implements ElementFeatures {

        private final boolean readonly;

        Neo4JElementFeatures(boolean readonly) {
            this.readonly = readonly;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_ADD_PROPERTY)
        public boolean supportsAddProperty() {
            return !readonly;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_REMOVE_PROPERTY)
        public boolean supportsRemoveProperty() {
            return !readonly;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_USER_SUPPLIED_IDS)
        public boolean supportsUserSuppliedIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_STRING_IDS)
        public boolean supportsStringIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_UUID_IDS)
        public boolean supportsUuidIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_ANY_IDS)
        public boolean supportsAnyIds() {
            return false;
        }

        @Override
        @FeatureDescriptor(name = FEATURE_CUSTOM_IDS)
        public boolean supportsCustomIds() {
            return false;
        }
    }

    private final GraphFeatures graphFeatures = new Neo4JGraphGraphFeatures();
    private final VertexFeatures vertexFeatures;
    private final EdgeFeatures edgeFeatures;

    Neo4JGraphFeatures(boolean readonly) {
        // initialize fields
        this.vertexFeatures = new Neo4JVertexFeatures(readonly);
        this.edgeFeatures = new Neo4JEdgeFeatures(readonly);
    }

    @Override
    public GraphFeatures graph() {
        return graphFeatures;
    }

    @Override
    public VertexFeatures vertex() {
        return vertexFeatures;
    }

    @Override
    public EdgeFeatures edge() {
        return edgeFeatures;
    }

    @Override
    public String toString() {
        return StringFactory.featureString(this);
    }
}