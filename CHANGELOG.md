# Change log

## 0.3.1

* Added support for BOLT+Routing bookmarks in Graph instance

## 0.3.0

* Updated [BOLT driver](https://github.com/neo4j/neo4j-java-driver) to version 1.6.1
* Updated [Apache Tinkerpop](http://tinkerpop.apache.org/) to version 3.3.3
* Added support for BOLT+Routing bookmarks 

## 0.2.27

* Updated [BOLT driver](https://github.com/neo4j/neo4j-java-driver) to version 1.4.4

## 0.2.26

* Updated [Apache Tinkerpop](http://tinkerpop.apache.org/) to version 3.3.0 (#64)
* Fixed bug creating provider with constructor arguments (#59)

## 0.2.25

* Updated [BOLT driver](https://github.com/neo4j/neo4j-java-driver) to version 1.3.0
* Updated [Apache Tinkerpop](http://tinkerpop.apache.org/) to version 3.2.5

## 0.2.24

* Fixed bug reading LIST OF ANY? values, thanks [runnerway](https://github.com/runnerway)

## 0.2.23

* Fixed bug [Different behavior between Tinkergraph, Neo4j (embedded) and neo4j-gremlin-bolt](https://github.com/SteelBridgeLabs/neo4j-gremlin-bolt/issues/52)

## 0.2.22

* Fixed bug in Update statement when removing Edge/Vertex property value

## 0.2.20

* Fixed 0.2.19 release, version was not updated

## 0.2.19

* Updated [BOLT driver](https://github.com/neo4j/neo4j-java-driver) to version 1.1.2
* Fixed bug setting element property to null

## 0.2.18

* Updated [Apache Tinkerpop](http://tinkerpop.apache.org/) to version 3.2.4

## 0.2.17

* Performance optimization while getting a single element by id

## 0.2.16

* Updated [BOLT driver](https://github.com/neo4j/neo4j-java-driver) to version 1.1.0

## 0.2.15

* Fixed bug updating edge, incorrect MERGE statement

## 0.2.14

* Updated [BOLT driver](https://github.com/neo4j/neo4j-java-driver) to version 1.0.6
* Fixed bug updating edge

## 0.2.13

* Fixed bug related to invalid CYPHER statement when using the Neo4JNativeElementIdProvider 

## 0.2.12

* Fixed some bugs related to Apache Tinkerpop Test suite execution (StructureStandardSuite) 

## 0.2.11

* Updated [Apache Tinkerpop](http://tinkerpop.apache.org/) to version 3.2.3
* Neo4J native id generation performance improvements
