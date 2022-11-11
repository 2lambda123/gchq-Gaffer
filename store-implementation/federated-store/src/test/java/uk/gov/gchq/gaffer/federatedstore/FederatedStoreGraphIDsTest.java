/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.DiscardOutput;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.export.set.ExportToSet;
import uk.gov.gchq.gaffer.operation.impl.export.set.GetSetExport;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.basicEntitySchema;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.contextBlankUser;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.DEPRECATED_GRAPHIDS_OPTION;

public class FederatedStoreGraphIDsTest {
    private FederatedStore federatedStore;

    @AfterAll
    public static void after() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();

        federatedStore = new FederatedStore();
        federatedStore.initialise(GRAPH_ID_TEST_FEDERATED_STORE, null, new FederatedStoreProperties());

        // Setup graph A and graph B with same schema
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_A)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(basicEntitySchema())
                        .isPublic(true)
                        .build(), contextBlankUser());
        federatedStore.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_B)
                        .storeProperties(loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES))
                        .schema(basicEntitySchema())
                        .isPublic(true)
                        .build(), contextBlankUser());

        // Graph A, property = 1
        federatedStore.execute(
                new AddElements.Builder()
                        .input(entity(1))
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                        .build(), contextBlankUser());
        // Graph B, property = 2
        federatedStore.execute(
                new AddElements.Builder()
                        .input(entity(2))
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_B)
                        .build(), contextBlankUser());
    }

    @Test
    public void shouldAggregateAcrossBothGraphsWhenNoGraphIdSelected() throws Exception {
        // Given
        Iterable<? extends Element> results = federatedStore.execute(
                new GetAllElements(), contextBlankUser());

        // When / Then
        assertThat(results)
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(3));
    }

    @Test
    public void shouldIgnoreGraphIdsOptionOnOperationChain() throws Exception {
        // Given
        Iterable<? extends Element> results = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new GetAllElements())
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                        .build(), contextBlankUser());

        // When / Then
        // Pre Gaffer 2.0 this would have sent the whole chain to graph A.
        // Now, the chain is sent to all graphs, because that is the default.
        // The chain's graphIds option is ignored, as every individual
        // operation is wrapped in a FederatedOperation
        assertThat(results)
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(3));
    }

    @Test
    public void shouldSelectGraphUsingFederatedOperation() throws Exception {
        // Given
        Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .graphIdsCSV(GRAPH_ID_A)
                        .build(), contextBlankUser());

        // When / Then
        assertThat((Iterable<Entity>) results).containsExactly(entity(1));
    }

    @Test
    public void shouldSelectGraphUsingGraphIdsOption() throws Exception {
        // Given
        Iterable<? extends Element> results = federatedStore.execute(
                new GetAllElements.Builder()
                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                        .build(), contextBlankUser());

        // When / Then
        assertThat((Iterable<Entity>) results).containsExactly(entity(1));
    }

    @Test
    public void shouldSelectGraphUsingGraphIdsOptionWithinOperationChain() throws Exception {
        // Given
        Iterable<? extends Element> results = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                                .build())
                        .build(), contextBlankUser());

        // When / Then
        assertThat((Iterable<Entity>) results).containsExactly(entity(1));
    }

    @Test
    public void shouldSelectGraphUsingGraphIdsOptionWithinNestedOperationChain() throws Exception {
        // Given
        Iterable<? extends Element> results = federatedStore.execute(
                new OperationChain.Builder()
                        .first(new OperationChain.Builder()
                                .first(new GetAllElements.Builder()
                                        .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                                        .build())
                                .build())
                        .build(), contextBlankUser());

        // When / Then
        assertThat((Iterable<Entity>) results).containsExactly(entity(1));
    }

    @Test
    public void shouldSelectGraphUsingFederatedOperationLongOperationChain() throws Exception {
        Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(
                new OperationChain.Builder()
                        .first(new FederatedOperation.Builder()
                                .op(new GetAllElements())
                                .graphIdsCSV(GRAPH_ID_A)
                                .build())
                        .then(new ExportToSet.Builder<>()
                                .key("results")
                                .build())
                        .then(new DiscardOutput())
                        .then(new FederatedOperation.Builder()
                                .op(new GetAllElements())
                                .graphIdsCSV(GRAPH_ID_B)
                                .build())
                        .then(new ExportToSet.Builder<>()
                                .key("results")
                                .build())
                        .then(new DiscardOutput())
                        .then(new GetSetExport.Builder()
                                .key("results")
                                .build())
                        .build(), contextBlankUser());

        // When / Then
        assertThat((Iterable<Entity>) results)
                .hasSize(2)
                .containsExactlyInAnyOrder(entity(1), entity(2));
    }

    @Test
    public void shouldSelectGraphUsingGraphIdsOptionWithinLongOperationChain() throws Exception {
        // Given
        Iterable<? extends Element> results = (Iterable<? extends Element>) federatedStore.execute(
                new OperationChain.Builder()
                        .first(new GetAllElements.Builder()
                                .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_A)
                                .build())
                        .then(new ExportToSet.Builder<>()
                                .key("results")
                                .build())
                        .then(new DiscardOutput())
                        .then(new GetAllElements.Builder()
                                .option(DEPRECATED_GRAPHIDS_OPTION, GRAPH_ID_B)
                                .build())
                        .then(new ExportToSet.Builder<>()
                                .key("results")
                                .build())
                        .then(new DiscardOutput())
                        .then(new GetSetExport.Builder()
                                .key("results")
                                .build())
                        .build(), contextBlankUser());

        // When / Then
        assertThat((Iterable<Entity>) results)
                .hasSize(2)
                .containsExactlyInAnyOrder(entity(1), entity(2));
    }

    private Entity entity(final int value) {
        return new Entity.Builder()
                .group(GROUP_BASIC_ENTITY)
                .vertex(BASIC_VERTEX)
                .property(PROPERTY_1, value)
                .build();
    }
}
