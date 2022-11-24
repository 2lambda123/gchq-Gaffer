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

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.ConcatenateListMergeFunction;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;
import uk.gov.gchq.koryphe.impl.binaryoperator.Sum;

import static org.assertj.core.api.Assertions.assertThat;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.ACCUMULO_STORE_SINGLE_USE_PROPERTIES;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.BASIC_VERTEX;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.CACHE_SERVICE_CLASS_STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_A;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_B;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GRAPH_ID_TEST_FEDERATED_STORE;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.GROUP_BASIC_ENTITY;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.INTEGER;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.PROPERTY_1;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.STRING;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.loadAccumuloStoreProperties;
import static uk.gov.gchq.gaffer.federatedstore.FederatedStoreTestUtil.resetForFederatedTests;

public class FederatedStoreVisibilityTest {
    private static final AccumuloProperties ACCUMULO_PROPERTIES = loadAccumuloStoreProperties(ACCUMULO_STORE_SINGLE_USE_PROPERTIES);

    public static final String PUBLIC = "public";
    private static final User USER = new User.Builder().dataAuth(PUBLIC).build();
    public static final String VISIBILITY = "visibility";
    public static final String VISIBILITY_1 = "visibility1";
    public static final String VISIBILITY_2 = "visibility2";
    public static final String PRIVATE = "private";

    private Graph federatedGraph;

    @AfterAll
    public static void tearDownCache() {
        resetForFederatedTests();
    }

    @BeforeEach
    public void setUp() throws Exception {
        resetForFederatedTests();
        FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheServiceClass(CACHE_SERVICE_CLASS_STRING);

        federatedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(GRAPH_ID_TEST_FEDERATED_STORE)
                        .build())
                .addStoreProperties(federatedStoreProperties)
                .build();
    }

    @Test
    public void shouldGetDataWhenVisibilityPropertyNamesAreSame() throws Exception {
        // Given
        addGraphs(VISIBILITY, VISIBILITY);
        addElements(GRAPH_ID_A, VISIBILITY, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY, PUBLIC);

        // When
        Iterable<? extends Element> aggregatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .mergeFunction(new ConcatenateListMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated");

        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1");

        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldGetDataWhenVisibilityPropertyNamesAreDifferent() throws Exception {
        // Given
        addGraphs(VISIBILITY_1, VISIBILITY_2);
        addElements(GRAPH_ID_A, VISIBILITY_1, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY_2, PUBLIC);

        // When
        Iterable<? extends Element> aggregatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .mergeFunction(new ConcatenateListMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated");

        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1");

        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldGetDataWhenVisibilityPropertyNamesAndAuthsAreDifferent() throws Exception {
        // Given
        addGraphs(VISIBILITY_1, VISIBILITY_2);
        addElements(GRAPH_ID_A, VISIBILITY_1, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY_2, PRIVATE);

        // When
        Iterable<? extends Element> aggregatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .mergeFunction(new ConcatenateListMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(1), "property is only visible once");

        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(1)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1");

        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldMergeVisibilityPropertyWhenVisibilityPropertyNamesAreDifferentAndGroupNameIsSame() throws Exception {
        // Given
        addGraphs(VISIBILITY_1, VISIBILITY_2);
        addElements(GRAPH_ID_A, VISIBILITY_1, PUBLIC);
        addElements(GRAPH_ID_B, VISIBILITY_2, PUBLIC);

        // When
        Iterable<? extends Element> aggregatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .mergeFunction(new ConcatenateListMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), new User());

        // Then
        // In this case, all results are returned but the visibility1 property has been overwritten
        // This is because 2 schemas were merged which had different visibility property names
        // on the same group, which should be avoided
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated")
                .matches(e -> e.getProperty(VISIBILITY_2).equals(PUBLIC), "visibility2 is present")
                .matches(e -> e.getProperty(VISIBILITY_1) == null, "visibility1 is overwritten in schema merge");
        // If this is to be done, the concatenated results are more intuitive
        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1")
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY_1)))
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY_2)));
        assertThat(noAuthResults).isEmpty();
    }

    @Test
    public void shouldAddDataWhenVisibilityPropertyNamesAreDifferent() throws Exception {
        // Given
        addGraphs(VISIBILITY_1, VISIBILITY_2);
        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group(GROUP_BASIC_ENTITY)
                                .vertex(BASIC_VERTEX)
                                .property(PROPERTY_1, 1)
                                .property(VISIBILITY_1, PUBLIC)
                                .property(VISIBILITY_2, PUBLIC)
                                .build())
                        .build())
                .build(), USER);

        // When
        Iterable<? extends Element> aggregatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), USER);
        Iterable<? extends Element> concatenatedResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .mergeFunction(new ConcatenateListMergeFunction())
                        .build(), USER);
        Iterable<? extends Element> noAuthResults = (Iterable<? extends Element>) federatedGraph.execute(
                new FederatedOperation.Builder()
                        .op(new GetAllElements())
                        .build(), new User());

        // Then
        assertThat(aggregatedResults)
                .isNotEmpty()
                .hasSize(1)
                .first()
                .matches(e -> e.getProperty(PROPERTY_1).equals(2), "property is aggregated")
                .matches(e -> e.getProperty(VISIBILITY_2).equals(PUBLIC), "has visibility2 property")
                .matches(e -> e.getProperty(VISIBILITY_1) == null, "visibility1 is overwritten in schema merge");
        assertThat(concatenatedResults)
                .isNotEmpty()
                .hasSize(2)
                .allMatch(e -> e.getProperty(PROPERTY_1).equals(1), "property value is 1")
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY_1)))
                .anyMatch(e -> PUBLIC.equals(e.getProperty(VISIBILITY_2)));
        assertThat(noAuthResults).isEmpty();
    }

    private void addGraphs(final String graphAVisibilityPropertyName, final String graphBVisibilityPropertyName) throws OperationException {
        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_A)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(getSchema(graphAVisibilityPropertyName))
                        .build(), USER);

        federatedGraph.execute(
                new AddGraph.Builder()
                        .graphId(GRAPH_ID_B)
                        .storeProperties(ACCUMULO_PROPERTIES)
                        .schema(getSchema(graphBVisibilityPropertyName))
                        .build(), USER);
    }

    private void addElements(final String graphId, final String visibilityPropertyName, final String visibilityValue) throws OperationException {
        federatedGraph.execute(new FederatedOperation.Builder()
                .op(new AddElements.Builder()
                        .input(new Entity.Builder()
                                .group(GROUP_BASIC_ENTITY)
                                .vertex(BASIC_VERTEX)
                                .property(PROPERTY_1, 1)
                                .property(visibilityPropertyName, visibilityValue)
                                .build())
                        .build())
                .graphIdsCSV(graphId)
                .build(), USER);
    }

    private Schema getSchema(final String visibilityPropertyName) {
        return new Schema.Builder()
                .entity(GROUP_BASIC_ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(STRING)
                        .property(PROPERTY_1, INTEGER)
                        .property(visibilityPropertyName, VISIBILITY)
                        .build())
                .type(STRING, String.class)
                .type(VISIBILITY, new TypeDefinition.Builder()
                        .clazz(String.class)
                        .aggregateFunction(new StringConcat())
                        .serialiser(new StringSerialiser())
                        .build())
                .type(INTEGER, new TypeDefinition.Builder()
                        .clazz(Integer.class)
                        .aggregateFunction(new Sum())
                        .build())
                .visibilityProperty(visibilityPropertyName)
                .build();
    }
}
