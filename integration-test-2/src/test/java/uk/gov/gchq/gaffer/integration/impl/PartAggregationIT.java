/*
 * Copyright 2016-2020 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.integration.impl;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import org.junit.jupiter.api.BeforeEach;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.TraitRequirement;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.integration.util.TestUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.parquetstore.ParquetStore;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.TestTypes;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEdges;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.getEntities;

public class PartAggregationIT extends AbstractStoreIT {

    @Override
    @BeforeEach
    public void resetRemoteProxyGraph() {
        getGraphFactory().reset(createSchema());
    }

    @GafferTest(excludeStores = ParquetStore.class) // Known bug with the Parquet Store
    public void shouldAggregateOnlyRequiredGroups(final GafferTestCase testcase) throws OperationException {
        // Given
        Graph graph = new Graph.Builder()
                .storeProperties(testcase.getStoreProperties())
                .addSchema(createSchema())
                .config(new GraphConfig("test"))
                .build();

        addDefaultElements(graph);

        // When
        final CloseableIterable<? extends Element> elements = graph.execute(
                new GetAllElements(), new User());

        //Then
        final List<Element> resultElements = Lists.newArrayList(elements);
        final List<Element> expectedElements = new ArrayList<>();
        getEntities().values().forEach(e -> {
            final Entity clone = e.emptyClone();
            clone.copyProperties(e.getProperties());
            expectedElements.add(clone);
        });
        getEntities().values().forEach(e -> {
            final Entity clone = new Entity.Builder()
                    .group(TestGroups.ENTITY_4)
                    .vertex(e.getVertex())
                    .build();
            clone.copyProperties(e.getProperties());
            clone.putProperty(TestPropertyNames.COUNT, 2L);
            expectedElements.add(clone);
        });
        getEntities().values().forEach(e -> {
            final Entity clone = new Entity.Builder()
                    .group(TestGroups.ENTITY_4)
                    .vertex(e.getVertex())
                    .build();
            clone.copyProperties(e.getProperties());
            clone.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("a different string"));
            clone.putProperty(TestPropertyNames.COUNT, 2L);
            expectedElements.add(clone);
        });
        getEdges().values().forEach(e -> {
            final Edge clone = e.emptyClone();
            clone.copyProperties(e.getProperties());
            expectedElements.add(clone);
        });
        getEdges().values().forEach(e -> {
            final Edge clone = new Edge.Builder()
                    .group(TestGroups.EDGE_4)
                    .source(e.getSource())
                    .dest(e.getDestination())
                    .directed(e.isDirected())
                    .build();
            clone.copyProperties(e.getProperties());
            clone.putProperty(TestPropertyNames.COUNT, 2L);
            expectedElements.add(clone);
        });
        expectedElements.forEach(e -> {
            if (TestGroups.ENTITY.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
                e.putProperty(TestPropertyNames.COUNT, 2L);
            } else if (TestGroups.EDGE.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.COUNT, 2L);
            } else if (TestGroups.EDGE_2.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.INT, 2);
            }
        });

        // Non aggregated elements should appear twice
        expectedElements.addAll(getNonAggregatedEntities());
        expectedElements.addAll(getNonAggregatedEntities());
        expectedElements.addAll(getNonAggregatedEdges());
        expectedElements.addAll(getNonAggregatedEdges());

        ElementUtil.assertElementEquals(expectedElements, resultElements);
    }

    @TraitRequirement(StoreTrait.QUERY_AGGREGATION)
    @GafferTest
    public void shouldAggregateOnlyRequiredGroupsWithQueryTimeAggregation(final GafferTestCase testcase) throws OperationException {
        // Given
        Graph graph = new Graph.Builder()
                .storeProperties(testcase.getStoreProperties())
                .addSchema(createSchema())
                .config(new GraphConfig("test"))
                .build();

        addDefaultElements(graph);

        // When
        final CloseableIterable<? extends Element> elements = graph.execute(
                new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .edge(TestGroups.EDGE_3, new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .edge(TestGroups.EDGE_4, new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .entity(TestGroups.ENTITY_3, new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .entity(TestGroups.ENTITY_4, new ViewElementDefinition.Builder()
                                        .groupBy()
                                        .build())
                                .build())
                        .build(), new User());

        //Then
        final List<Element> resultElements = Lists.newArrayList(elements);
        final List<Element> expectedElements = new ArrayList<>();
        getEntities().values().forEach(e -> {
            final Entity clone = e.emptyClone();
            clone.copyProperties(e.getProperties());
            expectedElements.add(clone);
        });
        getEntities().values().forEach(e -> {
            final Entity clone = new Entity.Builder()
                    .group(TestGroups.ENTITY_4)
                    .vertex(e.getVertex())
                    .build();
            clone.copyProperties(e.getProperties());
            clone.putProperty(TestPropertyNames.COUNT, 4L);
            final TreeSet<String> treeSet = new TreeSet<>(((TreeSet) e.getProperty(TestPropertyNames.SET)));
            treeSet.add("a different string");
            clone.putProperty(TestPropertyNames.SET, treeSet);
            expectedElements.add(clone);
        });
        getEdges().values().forEach(e -> {
            final Edge clone = e.emptyClone();
            clone.copyProperties(e.getProperties());
            expectedElements.add(clone);
        });
        getEdges().values().forEach(e -> {
            final Edge clone = new Edge.Builder()
                    .group(TestGroups.EDGE_4)
                    .source(e.getSource())
                    .dest(e.getDestination())
                    .directed(e.isDirected())
                    .build();
            clone.copyProperties(e.getProperties());
            clone.putProperty(TestPropertyNames.COUNT, 2L);
            expectedElements.add(clone);
        });
        expectedElements.forEach(e -> {
            if (TestGroups.ENTITY.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("3"));
                e.putProperty(TestPropertyNames.COUNT, 2L);
            } else if (TestGroups.EDGE.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.COUNT, 2L);
            } else if (TestGroups.EDGE_2.equals(e.getGroup())) {
                e.putProperty(TestPropertyNames.INT, 2);
            }
        });

        // Non aggregated elements should appear twice
        expectedElements.addAll(getNonAggregatedEntities());
        expectedElements.addAll(getNonAggregatedEntities());
        expectedElements.addAll(getNonAggregatedEdges());
        expectedElements.addAll(getNonAggregatedEdges());

        ElementUtil.assertElementEquals(expectedElements, resultElements);
    }

    public void addDefaultElements(final Graph graph) throws OperationException {
        // Add elements twice
        for (int i = 0; i < 2; i++) {
            TestUtil.addDefaultElements(graph);

            graph.execute(new AddElements.Builder()
                    .input(getNonAggregatedEntities())
                    .build(), new User());

            graph.execute(new AddElements.Builder()
                    .input(getNonAggregatedEdges())
                    .build(), new User());

            graph.execute(new AddElements.Builder()
                    .input(getEntitiesWithGroupBy())
                    .build(), new User());

            graph.execute(new AddElements.Builder()
                    .input(getEdgesWithGroupBy())
                    .build(), new User());
        }

    }

    private Schema createSchema() {
        final Schema defaultSchema = TestUtil.createDefaultSchema();

        // Add 2 new groups that are the same as the defaults but with no aggregation.
        return new Schema.Builder()
                .merge(defaultSchema)
                .entity(TestGroups.ENTITY_3,
                        new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                                .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                                .property("NonAggregatedProperty", "NonAggregatedString")
                                .property("NonAggregatedProperty", "AggregatedString")
                                .aggregate(false)
                                .groupBy()
                                .build())
                .edge(TestGroups.EDGE_3,
                        new SchemaEdgeDefinition.Builder()
                                .source(TestTypes.ID_STRING)
                                .destination(TestTypes.ID_STRING)
                                .directed(TestTypes.DIRECTED_EITHER)
                                .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                                .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                                .property("NonAggregatedProperty", "NonAggregatedString")
                                .aggregate(false)
                                .groupBy()
                                .build())
                .entity(TestGroups.ENTITY_4,
                        new SchemaEntityDefinition.Builder()
                                .vertex(TestTypes.ID_STRING)
                                .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                                .property(TestPropertyNames.SET, TestTypes.PROP_SET_STRING)
                                .groupBy(TestPropertyNames.SET)
                                .build())
                .edge(TestGroups.EDGE_4,
                        new SchemaEdgeDefinition.Builder()
                                .source(TestTypes.ID_STRING)
                                .destination(TestTypes.ID_STRING)
                                .directed(TestTypes.DIRECTED_EITHER)
                                .property(TestPropertyNames.INT, TestTypes.PROP_INTEGER)
                                .property(TestPropertyNames.COUNT, TestTypes.PROP_COUNT)
                                .groupBy(TestPropertyNames.INT)
                                .build())
                .type("NonAggregatedString",
                        new TypeDefinition.Builder()
                                .clazz(String.class)
                                .build())
                .type("AggregatedString",
                        new TypeDefinition.Builder()
                                .clazz(String.class)
                                .aggregateFunction(new StringConcat())
                                .build())
                .build();
    }

    private List<Edge> getNonAggregatedEdges() {
        final Function<Edge, Edge> edgeTransform = e -> {
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE_3)
                    .source(e.getSource())
                    .dest(e.getDestination())
                    .directed(e.isDirected())
                    .property("NonAggregatedProperty", "some value")
                    .build();
            edge.copyProperties(e.getProperties());
            return edge;
        };

        return Lists.transform(
                Lists.newArrayList(getEdges().values()),
                edgeTransform);
    }

    private List<Entity> getNonAggregatedEntities() {
        final Function<Entity, Entity> entityTransform = e -> {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY_3)
                    .vertex(e.getVertex())
                    .property("NonAggregatedProperty", "some value")
                    .build();
            entity.copyProperties(e.getProperties());
            return entity;
        };
        return Lists.transform(
                Lists.newArrayList(getEntities().values()),
                entityTransform);
    }

    private List<Edge> getEdgesWithGroupBy() {
        final Function<Edge, Edge> edgeTransform = e -> {
            final Edge edge = new Edge.Builder()
                    .group(TestGroups.EDGE_4)
                    .source(e.getSource())
                    .dest(e.getDestination())
                    .directed(e.isDirected())
                    .property(TestPropertyNames.INT, 1L)
                    .property(TestPropertyNames.COUNT, 1L)
                    .build();
            edge.copyProperties(e.getProperties());
            return edge;
        };

        return Lists.transform(
                Lists.newArrayList(getEdges().values()),
                edgeTransform);
    }

    private List<Entity> getEntitiesWithGroupBy() {
        final List<Entity> entities = new ArrayList<>();

        final Function<Entity, Entity> entityTransform = e -> {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY_4)
                    .vertex(e.getVertex())
                    .build();
            entity.copyProperties(e.getProperties());
            return entity;
        };
        entities.addAll(Lists.transform(
                Lists.newArrayList(getEntities().values()),
                entityTransform));

        final Function<Entity, Entity> entityTransform2 = e -> {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY_4)
                    .vertex(e.getVertex())
                    .build();
            entity.copyProperties(e.getProperties());
            entity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("a different string"));
            return entity;
        };
        entities.addAll(Lists.transform(
                Lists.newArrayList(getEntities().values()),
                entityTransform2));

        return entities;
    }
}
