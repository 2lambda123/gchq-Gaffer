/*
 * Copyright 2017-2022 Crown Copyright
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

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.AccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseAccumuloStore;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.CommonConstants;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.GetAllGraphIds;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedAddGraphHandler;
import uk.gov.gchq.gaffer.federatedstore.operation.handler.impl.FederatedGetTraitsHandlerTest;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.OperationImpl;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.GraphLibrary;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.Schema.Builder;
import uk.gov.gchq.gaffer.user.StoreUser;
import uk.gov.gchq.gaffer.user.User;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;
import static org.assertj.core.api.Assertions.fail;
import static org.junit.jupiter.api.Assertions.assertThrows;

import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;
import static uk.gov.gchq.gaffer.operation.export.graph.handler.GraphDelegate.STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S;
import static uk.gov.gchq.gaffer.store.StoreTrait.MATCHED_VERTEX;
import static uk.gov.gchq.gaffer.store.StoreTrait.ORDERED;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.user.StoreUser.TEST_USER_ID;
import static uk.gov.gchq.gaffer.user.StoreUser.blankUser;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedStoreTest {

    public static final String ID_SCHEMA_ENTITY = "basicEntitySchema";
    public static final String ID_SCHEMA_EDGE = "basicEdgeSchema";
    public static final String ID_PROPS_ACC_1 = "miniAccProps1";
    public static final String ID_PROPS_ACC_2 = "miniAccProps2";
    public static final String ID_PROPS_ACC_ALT = "miniAccProps3";
    public static final String INVALID = "invalid";
    private static final String FEDERATED_STORE_ID = "testFederatedStoreId";
    private static final String ACC_ID_1 = "miniAccGraphId1";
    private static final String ACC_ID_2 = "miniAccGraphId2";
    private static final String MAP_ID_1 = "miniMapGraphId1";
    private static final String PATH_ACC_STORE_PROPERTIES_1 = "properties/singleUseAccumuloStore.properties";
    private static final String PATH_ACC_STORE_PROPERTIES_2 = "properties/singleUseAccumuloStore.properties";
    private static final String PATH_ACC_STORE_PROPERTIES_ALT = "properties/singleUseAccumuloStoreAlt.properties";
    private static final String PATH_BASIC_ENTITY_SCHEMA_JSON = "schema/basicEntitySchema.json";
    private static final String PATH_ENTITY_A_SCHEMA_JSON = "schema/entityASchema.json";
    private static final String PATH_ENTITY_B_SCHEMA_JSON = "schema/entityBSchema.json";
    private static final String PATH_BASIC_EDGE_SCHEMA_JSON = "schema/basicEdgeSchema.json";
    public static final String UNUSUAL_KEY = "unusualKey";
    public static final String KEY_DOES_NOT_BELONG = String.format(" %swas added to %s it should not be there", UNUSUAL_KEY, ID_PROPS_ACC_2);
    private static final String ALL_USERS = StoreUser.ALL_USERS;
    private static final HashSet<String> GRAPH_AUTHS = Sets.newHashSet(ALL_USERS);
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private static final String INVALID_CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.invalid";
    private static final String CACHE_SERVICE_NAME = "federatedStoreGraphs";
    public static final String PATH_INCOMPLETE_SCHEMA = "/schema/edgeX2NoTypesSchema.json";
    public static final String PATH_INCOMPLETE_SCHEMA_PART_2 = "/schema/edgeTypeSchema.json";
    private FederatedStore store;
    private FederatedStoreProperties federatedProperties;
    private HashMapGraphLibrary library;
    private Context userContext;
    private User blankUser;
    private IgnoreOptions ignore;

    private static final Class<?> CURRENT_CLASS = new Object() {
    }.getClass().getEnclosingClass();

    private static final AccumuloProperties PROPERTIES_1 = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(CURRENT_CLASS, PATH_ACC_STORE_PROPERTIES_1));
    private static final AccumuloProperties PROPERTIES_2 = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(CURRENT_CLASS, PATH_ACC_STORE_PROPERTIES_2));
    private static final AccumuloProperties PROPERTIES_ALT = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(CURRENT_CLASS, PATH_ACC_STORE_PROPERTIES_ALT));

    @BeforeEach
    public void setUp() throws Exception {
        clearCache();
        federatedProperties = new FederatedStoreProperties();
        federatedProperties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));

        clearLibrary();
        library = new HashMapGraphLibrary();
        library.addProperties(ID_PROPS_ACC_1, PROPERTIES_1);
        library.addProperties(ID_PROPS_ACC_2, PROPERTIES_2);
        library.addProperties(ID_PROPS_ACC_ALT, PROPERTIES_ALT);
        library.addSchema(ID_SCHEMA_EDGE, getSchemaFromPath(PATH_BASIC_EDGE_SCHEMA_JSON));
        library.addSchema(ID_SCHEMA_ENTITY, getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON));

        store = new FederatedStore();
        store.setGraphLibrary(library);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        userContext = new Context(blankUser());
        blankUser = blankUser();

        ignore = new IgnoreOptions();
    }

    @AfterEach
    public void tearDown() throws Exception {
        assertThat(PROPERTIES_1).withFailMessage("Library has changed: " + ID_PROPS_ACC_1).isEqualTo(library.getProperties(ID_PROPS_ACC_1));
        assertThat(PROPERTIES_2).withFailMessage("Library has changed: " + ID_PROPS_ACC_2).isEqualTo(library.getProperties(ID_PROPS_ACC_2));
        assertThat(PROPERTIES_ALT).withFailMessage("Library has changed: " + ID_PROPS_ACC_ALT).isEqualTo(library.getProperties(ID_PROPS_ACC_ALT));

        assertThat(new String(getSchemaFromPath(PATH_BASIC_EDGE_SCHEMA_JSON).toJson(false), CommonConstants.UTF_8))
                .withFailMessage("Library has changed: " + ID_SCHEMA_EDGE)
                .isEqualTo(new String(library.getSchema(ID_SCHEMA_EDGE).toJson(false), CommonConstants.UTF_8));
        assertThat(new String(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON).toJson(false), CommonConstants.UTF_8))
                .withFailMessage("Library has changed: " + ID_SCHEMA_ENTITY)
                .isEqualTo(new String(library.getSchema(ID_SCHEMA_ENTITY).toJson(false), CommonConstants.UTF_8));

        clearLibrary();
        clearCache();
    }

    @Test
    public void shouldLoadGraphsWithIds() throws Exception {
        // When
        final int before = store.getGraphs(blankUser, null, ignore).size();

        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_EDGE);
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC_1, ID_SCHEMA_ENTITY);

        // Then
        final Collection<Graph> graphs = store.getGraphs(blankUser, null, ignore);
        final int after = graphs.size();
        assertThat(before).isEqualTo(0);
        assertThat(after).isEqualTo(2);
        final ArrayList<String> graphNames = Lists.newArrayList(ACC_ID_1, ACC_ID_2);
        for (final Graph graph : graphs) {
            assertThat(graphNames).contains(graph.getGraphId());
        }
    }

    @Test
    public void shouldThrowErrorForFailedSchemaID() throws Exception {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, INVALID));

        assertContains(actual.getCause(), SCHEMA_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S,
                Arrays.toString(new String[] {INVALID}));
    }

    @Test
    public void shouldThrowErrorForFailedPropertyID() throws Exception {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, INVALID, ID_SCHEMA_EDGE));

        assertContains(actual.getCause(), STORE_PROPERTIES_COULD_NOT_BE_FOUND_IN_THE_GRAPH_LIBRARY_WITH_ID_S, INVALID);
    }

    @Test
    public void shouldThrowErrorForMissingProperty() throws Exception {
        // When / Then
        final ArrayList<String> schemas = Lists.newArrayList(ID_SCHEMA_EDGE);
        final Exception actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .isPublic(true)
                        .parentSchemaIds(schemas)
                        .build(), userContext));

        assertContains(actual.getCause(), GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, ACC_ID_2, "StoreProperties");
    }

    @Test
    public void shouldThrowErrorForMissingSchema() throws Exception {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .isPublic(true)
                        .parentPropertiesId(ID_PROPS_ACC_2)
                        .build(), userContext));

        assertContains(actual.getCause(), GRAPH_ID_S_CANNOT_BE_CREATED_WITHOUT_DEFINED_KNOWN_S, ACC_ID_2, "Schema");
    }

    @Test
    public void shouldNotAllowOverwritingOfGraphWithinFederatedScope() throws Exception {
        // Given
        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_ENTITY);

        // When / Then
        Exception actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_EDGE));
        assertContains(actual, "User is attempting to overwrite a graph");
        assertContains(actual, "GraphId: ", ACC_ID_2);

        // When / Then
        actual = assertThrows(Exception.class,
                () -> addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_ALT, ID_SCHEMA_ENTITY));
        assertContains(actual, "User is attempting to overwrite a graph");
        assertContains(actual, "GraphId: ", ACC_ID_2);
    }

    @Test
    public void shouldThrowAppropriateExceptionWhenHandlingAnUnsupportedOperation() {
        // Given
        final Operation op = new OperationImpl();
        // When
        // Expected an UnsupportedOperationException rather than an OperationException

        // Then
        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> store.handleOperation(op, new Context()))
                .withMessage("Operation class uk.gov.gchq.gaffer.operation.impl.OperationImpl is not supported by the FederatedStore.");
    }

    @Test
    public void shouldAlwaysReturnSupportedTraits() throws Exception {
        // Given
        addGraphWithIds(ACC_ID_1, ID_PROPS_ACC_1, ID_SCHEMA_ENTITY);

        final Set<StoreTrait> before = store.execute(new GetTraits.Builder()
                .currentTraits(false)
                .build(), userContext);

        // When
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);

        final Set<StoreTrait> after = store.execute(new GetTraits.Builder()
                .currentTraits(false)
                .build(), userContext);

        // Then
        assertThat(AccumuloStore.TRAITS).hasSameSizeAs(before);
        assertThat(AccumuloStore.TRAITS).hasSameSizeAs(after);
        assertThat(before).isEqualTo(after);
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsAdded() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);
        final Schema before = store.getSchema((Operation) null, blankUser);
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);
        final Schema after = store.getSchema((Operation) null, blankUser);
        // Then
        assertThat(before).isNotEqualTo(after);
    }

    @Test
    public void shouldUpdateSchemaWhenNewGraphIsRemoved() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);
        final Schema was = store.getSchema((Operation) null, blankUser);
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);

        final Schema before = store.getSchema((Operation) null, blankUser);

        // When
        store.remove(ACC_ID_2, blankUser);

        final Schema after = store.getSchema((Operation) null, blankUser);
        assertThat(before).isNotEqualTo(after);
        assertThat(was).isEqualTo(after);
    }

    @Test
    public void shouldFailWithIncompleteSchema() throws Exception {
        // When / Then
        final Exception actual = assertThrows(Exception.class,
                () -> addGraphWithPaths(ACC_ID_1, PROPERTIES_ALT, PATH_INCOMPLETE_SCHEMA));
        assertContains(actual, FederatedAddGraphHandler.ERROR_ADDING_GRAPH_GRAPH_ID_S, ACC_ID_1);
    }

    @Test
    public void shouldTakeCompleteSchemaFromTwoFiles() throws Exception {
        // Given
        final int before = store.getGraphs(blankUser, null, ignore).size();
        addGraphWithPaths(ACC_ID_1, PROPERTIES_ALT, PATH_INCOMPLETE_SCHEMA, PATH_INCOMPLETE_SCHEMA_PART_2);

        // When
        final int after = store.getGraphs(blankUser, null, ignore).size();

        // Then
        assertThat(before).isEqualTo(0);
        assertThat(after).isEqualTo(1);
    }

    @Test
    public void shouldAddTwoGraphs() throws Exception {
        // Given
        final int sizeBefore = store.getGraphs(blankUser, null, ignore).size();

        // When
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);
        addGraphWithPaths(ACC_ID_1, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);

        final int sizeAfter = store.getGraphs(blankUser, null, ignore).size();

        // Then
        assertThat(sizeBefore).isEqualTo(0);
        assertThat(sizeAfter).isEqualTo(2);
    }

    @Test
    public void shouldCombineTraitsToMin() throws Exception {
        // When
        final Set<StoreTrait> before = store.execute(new GetTraits.Builder()
                .currentTraits(true)
                .build(), userContext);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        store.execute(new AddGraph.Builder()
                .schema(new Schema())
                .isPublic(true)
                .graphId(ACC_ID_1)
                .storeProperties(PROPERTIES_1)
                .build(), new Context(testUser()));

        final Set<StoreTrait> afterAcc = store.execute(new GetTraits.Builder()
                .currentTraits(true)
                .build(), userContext);

        final StoreProperties TestStoreImp = new StoreProperties();
        TestStoreImp.setStoreClass(FederatedGetTraitsHandlerTest.TestStoreImpl.class);

        store.execute(new AddGraph.Builder()
                .schema(new Schema())
                .isPublic(true)
                .graphId(MAP_ID_1)
                .storeProperties(TestStoreImp)
                .build(), new Context(testUser()));

        final Set<StoreTrait> afterMap = store.execute(new GetTraits.Builder()
                .currentTraits(true)
                .build(), userContext);

        // Then
        assertThat(SingleUseAccumuloStore.TRAITS).isNotEqualTo(new HashSet<>(Arrays.asList(
                StoreTrait.INGEST_AGGREGATION,
                StoreTrait.PRE_AGGREGATION_FILTERING,
                StoreTrait.POST_AGGREGATION_FILTERING,
                StoreTrait.TRANSFORMATION,
                StoreTrait.POST_TRANSFORMATION_FILTERING,
                StoreTrait.MATCHED_VERTEX)));
        assertThat(before).withFailMessage("No traits should be found for an empty FederatedStore").isEmpty();
        assertThat(afterAcc).isEqualTo(Sets.newHashSet(
                TRANSFORMATION,
                PRE_AGGREGATION_FILTERING,
                POST_AGGREGATION_FILTERING,
                POST_TRANSFORMATION_FILTERING,
                ORDERED,
                MATCHED_VERTEX));
        assertThat(afterMap).isEqualTo(Sets.newHashSet(
                TRANSFORMATION,
                PRE_AGGREGATION_FILTERING,
                POST_AGGREGATION_FILTERING,
                POST_TRANSFORMATION_FILTERING,
                MATCHED_VERTEX));
    }

    @Test
    public void shouldContainNoElements() throws Exception {
        // When
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);
        final Set<Element> after = getElements();

        // Then
        assertThat(after).isEmpty();
    }

    @Test
    public void shouldAddEdgesToOneGraph() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);

        final AddElements op = new AddElements.Builder()
                .input(new Edge.Builder()
                        .group("BasicEdge")
                        .source("testSource")
                        .dest("testDest")
                        .property("property1", 12)
                        .build())
                .build();

        // When
        store.execute(op, userContext);

        // Then
        assertThat(getElements()).hasSize(1);
    }

    @Test
    public void shouldReturnGraphIds() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);

        // When
        final Collection<String> allGraphIds = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphIds)
                .hasSize(2)
                .contains(ACC_ID_1, ACC_ID_2);

    }

    @Test
    public void shouldUpdateGraphIds() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_1, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);

        // When
        final Collection<String> allGraphId = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphId).hasSize(1)
                .contains(ACC_ID_1)
                .doesNotContain(ACC_ID_2);

        // When
        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_ENTITY);
        final Collection<String> allGraphId2 = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphId2).hasSize(2).contains(ACC_ID_1, ACC_ID_2);

        // When
        store.remove(ACC_ID_1, blankUser);
        final Collection<String> allGraphId3 = store.getAllGraphIds(blankUser);

        // Then
        assertThat(allGraphId3).hasSize(1)
                .doesNotContain(ACC_ID_1)
                .contains(ACC_ID_2);

    }

    @Test
    public void shouldGetAllGraphIdsInUnmodifiableSet() throws Exception {
        // Given
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);

        // When / Then
        final Collection<String> allGraphIds = store.getAllGraphIds(blankUser);

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> allGraphIds.add("newId"))
                .isNotNull();

        assertThatExceptionOfType(UnsupportedOperationException.class).isThrownBy(() -> allGraphIds.remove("newId"))
                .isNotNull();
    }

    @Test
    public void shouldNotUseSchema() throws Exception {
        // Given
        final Schema unusedMock = Mockito.mock(Schema.class);
        // When
        store.initialise(FEDERATED_STORE_ID, unusedMock, federatedProperties);
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);
        // Then
        Mockito.verifyNoMoreInteractions(unusedMock);
    }

    @Test
    public void shouldAddGraphFromLibrary() throws Exception {
        // Given
        library.add(ACC_ID_2, library.getSchema(ID_SCHEMA_ENTITY), library.getProperties(ID_PROPS_ACC_2));

        // When
        final int before = store.getGraphs(blankUser, null, ignore).size();
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .build(), new Context(blankUser));

        final int after = store.getGraphs(blankUser, null, ignore).size();

        // Then
        assertThat(before).isEqualTo(0);
        assertThat(after).isEqualTo(1);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibrary() throws Exception {
        // When
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .parentPropertiesId(ID_PROPS_ACC_ALT)
                .isPublic(true)
                .schema(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build(), userContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, ignore)).hasSize(1);
        assertThat(PROPERTIES_ALT).isEqualTo(library.getProperties(ID_PROPS_ACC_ALT));
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibrary() throws Exception {
        // When
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .storeProperties(PROPERTIES_ALT)
                .isPublic(true)
                .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_ENTITY))
                .build(), userContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, ignore)).hasSize(1);
        assertThat(library.getSchema(ID_SCHEMA_ENTITY).toString()).isEqualTo(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON).toString());
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibrary() throws Exception {
        // When
        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_ALT, ID_SCHEMA_ENTITY);

        // Then
        assertThat(store.getGraphs(blankUser, null, ignore)).hasSize(1);
        final Graph graph = store.getGraphs(blankUser, ACC_ID_2, ignore).iterator().next();
        assertThat(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON)).isEqualTo(graph.getSchema());
        assertThat(graph.getStoreProperties()).isEqualTo(PROPERTIES_ALT);
    }

    @Test
    public void shouldAddGraphWithPropertiesFromGraphLibraryOverridden() throws Exception {
        // Given
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();

        // When
        final Builder schema = new Builder();
        for (final String path : new String[] {PATH_BASIC_ENTITY_SCHEMA_JSON}) {
            schema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .storeProperties(PROPERTIES_ALT)
                .parentPropertiesId(ID_PROPS_ACC_2)
                .isPublic(true)
                .schema(schema.build())
                .build(), userContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, ignore)).hasSize(1);
        assertThat(store.getGraphs(blankUser, null, ignore).iterator().next().getStoreProperties().containsKey(UNUSUAL_KEY)).isTrue();
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();
        assertThat(store.getGraphs(blankUser, null, ignore).iterator().next().getStoreProperties().getProperties().getProperty(UNUSUAL_KEY)).isNotNull();
    }

    @Test
    public void shouldAddGraphWithSchemaFromGraphLibraryOverridden() throws Exception {
        final ArrayList<String> schemas = Lists.newArrayList(ID_SCHEMA_ENTITY);
        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .isPublic(true)
                .schema(getSchemaFromPath(PATH_BASIC_EDGE_SCHEMA_JSON))
                .parentSchemaIds(schemas)
                .parentPropertiesId(ID_PROPS_ACC_2)
                .build(), userContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, ignore)).hasSize(1);
        assertThat(store.getGraphs(blankUser, null, ignore).iterator().next().getSchema().getEntityGroups()).contains("BasicEntity");
    }

    @Test
    public void shouldAddGraphWithPropertiesAndSchemaFromGraphLibraryOverridden() throws Exception {
        // Given
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();

        // When
        final Builder tempSchema = new Builder();
        for (final String path : new String[] {PATH_BASIC_EDGE_SCHEMA_JSON}) {
            tempSchema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(ACC_ID_2)
                .isPublic(true)
                .storeProperties(PROPERTIES_ALT)
                .parentPropertiesId(ID_PROPS_ACC_2)
                .schema(tempSchema.build())
                .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_ENTITY))
                .build(), userContext);

        // Then
        assertThat(store.getGraphs(blankUser, null, ignore)).hasSize(1);
        assertThat(store.getGraphs(blankUser, null, ignore).iterator().next().getStoreProperties().containsKey(UNUSUAL_KEY)).isTrue();
        assertThat(library.getProperties(ID_PROPS_ACC_2).containsKey(UNUSUAL_KEY)).withFailMessage(KEY_DOES_NOT_BELONG).isFalse();
        assertThat(store.getGraphs(blankUser, null, ignore).iterator().next().getStoreProperties().getProperties().getProperty(UNUSUAL_KEY)).isNotNull();
        assertThat(store.getGraphs(blankUser, null, ignore).iterator().next().getSchema().getEntityGroups().contains("BasicEntity")).isTrue();
    }

    @Test
    public void shouldNotAllowOverridingOfKnownGraphInLibrary() throws Exception {
        // Given
        library.add(ACC_ID_2, getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON), PROPERTIES_ALT);

        // When / Then
        Exception actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .parentPropertiesId(ID_PROPS_ACC_1)
                        .isPublic(true)
                        .build(), userContext));
        assertContains(actual.getCause(), "Graph: " + ACC_ID_2 + " already exists so you cannot use a different StoreProperties");

        // When / Then
        actual = assertThrows(Exception.class,
                () -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_EDGE))
                        .isPublic(true)
                        .build(), userContext));

        assertContains(actual.getCause(), "Graph: " + ACC_ID_2 + " already exists so you cannot use a different Schema");
    }

    @Test
    public void shouldFederatedIfUserHasCorrectAuths() throws Exception {
        // Given
        store.addGraphs(GRAPH_AUTHS, null, false, new GraphSerialisable.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(ACC_ID_2)
                        .build())
                .properties(PROPERTIES_ALT)
                .schema(getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON))
                .build());

        // When
        final Iterable<? extends Element> elements = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(blankUser.getUserId())
                        .opAuth(ALL_USERS)
                        .build()));

        // Then
        assertThat(elements.iterator()).isExhausted();

        // When - user cannot see any graphs
        final Iterable<? extends Element> elements2 = store.execute(new GetAllElements(),
                new Context(new User.Builder()
                        .userId(blankUser.getUserId())
                        .opAuths("x")
                        .build()));

        // Then
        assertThat(elements2).isEmpty();
    }

    @Test
    public void shouldReturnSpecificGraphsFromCSVString() throws Exception {
        // Given
        final List<Collection<GraphSerialisable>> graphLists = populateGraphs(1, 2, 4);
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);
        final Collection<GraphSerialisable> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, "mockGraphId1,mockGraphId2,mockGraphId4", ignore);

        // Then
        assertThat(returnedGraphs)
                .hasSize(3)
                .containsAll(toGraphs(expectedGraphs));

        assertThat(checkUnexpected(toGraphs(unexpectedGraphs), returnedGraphs)).isFalse();
    }

    @Test
    public void shouldReturnEnabledByDefaultGraphsForNullString() throws Exception {
        // Given
        populateGraphs();

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, null, ignore);

        // Then
        final Set<String> graphIds = returnedGraphs.stream().map(Graph::getGraphId).collect(Collectors.toSet());
        assertThat(graphIds).containsExactly("mockGraphId0", "mockGraphId2", "mockGraphId4");
    }

    @Test
    public void shouldReturnNotReturnEnabledOrDisabledGraphsWhenNotInCsv() throws Exception {
        // Given
        populateGraphs();

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, "mockGraphId0,mockGraphId1", ignore);

        // Then
        final Set<String> graphIds = returnedGraphs.stream().map(Graph::getGraphId).collect(Collectors.toSet());
        assertThat(graphIds).containsExactly("mockGraphId0", "mockGraphId1");
    }

    @Test
    public void shouldReturnNoGraphsFromEmptyString() throws Exception {
        // Given

        final List<Collection<GraphSerialisable>> graphLists = populateGraphs();
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, "", ignore);

        // Then
        assertThat(returnedGraphs).withFailMessage(returnedGraphs.toString()).isEmpty();
        assertThat(expectedGraphs).withFailMessage(expectedGraphs.toString()).isEmpty();
    }

    @Test
    public void shouldReturnGraphsWithLeadingCommaString() throws Exception {
        // Given
        final List<Collection<GraphSerialisable>> graphLists = populateGraphs(2, 4);
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);
        final Collection<GraphSerialisable> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, ",mockGraphId2,mockGraphId4", ignore);

        // Then
        assertThat(returnedGraphs)
                .hasSize(2)
                .containsAll(toGraphs(expectedGraphs));

        assertThat(checkUnexpected(toGraphs(unexpectedGraphs), returnedGraphs)).isFalse();
    }

    @Test
    public void shouldAddGraphIdWithAuths() throws Exception {
        // Given
        final Graph fedGraph = new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId(FEDERATED_STORE_ID)
                        .library(library)
                        .build())
                .addStoreProperties(federatedProperties)
                .build();

        addGraphWithIds(ACC_ID_2, ID_PROPS_ACC_2, ID_SCHEMA_ENTITY);

        library.add(ACC_ID_2, getSchemaFromPath(PATH_BASIC_ENTITY_SCHEMA_JSON), PROPERTIES_ALT);

        // When
        int before = 0;
        for (@SuppressWarnings("unused") final String ignore : fedGraph.execute(
                new GetAllGraphIds(),
                blankUser)) {
            before++;
        }

        fedGraph.execute(
                new AddGraph.Builder()
                        .graphAuths("auth")
                        .graphId(ACC_ID_2)
                        .build(),
                blankUser);

        int after = 0;
        for (@SuppressWarnings("unused") final String ignore : fedGraph.execute(
                new GetAllGraphIds(),
                blankUser)) {
            after++;
        }

        fedGraph.execute(new AddElements.Builder()
                .input(new Entity.Builder()
                        .group("BasicEntity")
                        .vertex("v1")
                        .build())
                .build(),
                blankUser);

        final Iterable<? extends Element> elements = fedGraph.execute(
                new GetAllElements(),
                new User.Builder()
                        .userId(TEST_USER_ID + "Other")
                        .opAuth("auth")
                        .build());

        final Iterable<? extends Element> elements2 = fedGraph.execute(new GetAllElements(),
                new User.Builder()
                        .userId(TEST_USER_ID + "Other")
                        .opAuths("x")
                        .build());
        assertThat(elements2).isEmpty();

        // Then
        assertThat(before).isEqualTo(0);
        assertThat(after).isEqualTo(1);
        assertThat(elements).isNotNull();
        assertThat(elements.iterator()).hasNext();
    }

    @Test
    public void shouldThrowWithPropertiesErrorFromGraphLibrary() throws Exception {
        final Builder schema = new Builder();
        for (final String path : new String[] {PATH_BASIC_EDGE_SCHEMA_JSON}) {
            schema.merge(getSchemaFromPath(path));
        }
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "test Something went wrong";
        Mockito.when(mockLibrary.getProperties(ID_PROPS_ACC_2)).thenThrow(new IllegalArgumentException(error));
        store.setGraphLibrary(mockLibrary);
        clearCache();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When / Then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .parentPropertiesId(ID_PROPS_ACC_2)
                        .isPublic(true)
                        .schema(schema.build())
                        .build(), userContext))
                .withStackTraceContaining(error);
        Mockito.verify(mockLibrary).getProperties(ID_PROPS_ACC_2);
    }

    @Test
    public void shouldThrowWithSchemaErrorFromGraphLibrary() throws Exception {
        // Given
        final GraphLibrary mockLibrary = Mockito.mock(GraphLibrary.class);
        final String error = "test Something went wrong";
        Mockito.when(mockLibrary.getSchema(ID_SCHEMA_ENTITY)).thenThrow(new IllegalArgumentException(error));
        store.setGraphLibrary(mockLibrary);
        clearCache();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // When / Then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new AddGraph.Builder()
                        .graphId(ACC_ID_2)
                        .storeProperties(PROPERTIES_ALT)
                        .isPublic(true)
                        .parentSchemaIds(Lists.newArrayList(ID_SCHEMA_ENTITY))
                        .build(), userContext))
                .withStackTraceContaining(error);
        Mockito.verify(mockLibrary).getSchema(ID_SCHEMA_ENTITY);
    }

    @Test
    public void shouldReturnASingleGraph() throws Exception {
        // Given
        final List<Collection<GraphSerialisable>> graphLists = populateGraphs(1);
        final Collection<GraphSerialisable> expectedGraphs = graphLists.get(0);
        final Collection<GraphSerialisable> unexpectedGraphs = graphLists.get(1);

        // When
        final Collection<Graph> returnedGraphs = store.getGraphs(blankUser, "mockGraphId1", ignore);

        // Then
        assertThat(returnedGraphs)
                .hasSize(1)
                .containsAll(toGraphs(expectedGraphs));

        assertThat(checkUnexpected(toGraphs(unexpectedGraphs), returnedGraphs)).isFalse();
    }

    private List<Graph> toGraphs(final Collection<GraphSerialisable> graphSerialisables) {
        return graphSerialisables.stream().map(GraphSerialisable::getGraph).collect(Collectors.toList());
    }

    @Test
    public void shouldThrowExceptionWithInvalidCacheClass() throws StoreException {
        federatedProperties.setCacheProperties(INVALID_CACHE_SERVICE_CLASS_STRING);

        clearCache();

        assertThatIllegalArgumentException().isThrownBy(() -> store.initialise(FEDERATED_STORE_ID, null, federatedProperties))
                .withMessageContaining("Failed to instantiate cache");
    }

    @Test
    public void shouldReuseGraphsAlreadyInCache() throws Exception {
        // Check cache is empty
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        assertThat(CacheServiceLoader.getService()).isNull();

        // initialise FedStore
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // add something so it will be in the cache
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_2))
                .properties(PROPERTIES_ALT)
                .schema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        store.addGraphs(null, TEST_USER_ID, true, graphToAdd);

        // check the store and the cache
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
        assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME))
                .contains(ACC_ID_2, ACC_ID_2);

        // restart the store
        store = new FederatedStore();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // check the graph is already in there from the cache
        assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME))
                .withFailMessage(String.format("Keys: %s did not contain %s", CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME), ACC_ID_2)).contains(ACC_ID_2);
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
    }

    @Test
    public void shouldInitialiseWithCache() throws StoreException {
        assertThat(CacheServiceLoader.getService()).isNull();
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        assertThat(CacheServiceLoader.getService()).isNull();
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        assertThat(CacheServiceLoader.getService()).isNotNull();
    }

    @Test
    public void shouldThrowExceptionWithoutInitialisation() throws StoreException {
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Given
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .properties(PROPERTIES_ALT)
                .schema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        clearCache();

        // When / Then
        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.addGraphs(null, TEST_USER_ID, false, graphToAdd))
                .withMessageContaining("No cache has been set");
    }

    @Test
    public void shouldNotThrowExceptionWhenInitialisedWithNoCacheClassInProperties() throws StoreException {
        // Given
        federatedProperties = new FederatedStoreProperties();

        // When / Then
        try {
            store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        } catch (final StoreException e) {
            fail("FederatedStore does not have to have a cache.");
        }
    }

    @Test
    public void shouldAddGraphsToCache() throws Exception {
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // Given
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .properties(PROPERTIES_ALT)
                .schema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        // When
        store.addGraphs(null, TEST_USER_ID, true, graphToAdd);

        // Then
        assertThat(store.getGraphs(blankUser, ACC_ID_1, ignore)).hasSize(1);

        // When
        final Collection<Graph> storeGraphs = store.getGraphs(blankUser, null, ignore);

        // Then
        assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1);
        assertThat(storeGraphs).contains(graphToAdd.getGraph());

        // When
        store = new FederatedStore();

        // Then
        assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1);
    }

    @Test
    public void shouldAddMultipleGraphsToCache() throws Exception {
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);
        // Given

        final List<GraphSerialisable> graphsToAdd = new ArrayList<>();
        for (int i = 0; i < 10; i++) {
            graphsToAdd.add(new GraphSerialisable.Builder()
                    .config(new GraphConfig(ACC_ID_1 + i))
                    .properties(PROPERTIES_ALT)
                    .schema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                    .build());
        }

        // When
        store.addGraphs(null, TEST_USER_ID, false, graphsToAdd.toArray(new GraphSerialisable[graphsToAdd.size()]));

        // Then
        for (int i = 0; i < 10; i++) {
            assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1 + i);
        }

        // When
        store = new FederatedStore();

        // Then
        for (int i = 0; i < 10; i++) {
            assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1 + i);
        }
    }

    @Test
    public void shouldAddAGraphRemoveAGraphAndBeAbleToReuseTheGraphId() throws Exception {
        // Given
        // When
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_ENTITY_SCHEMA_JSON);
        store.execute(new RemoveGraph.Builder()
                .graphId(ACC_ID_2)
                .build(), userContext);
        addGraphWithPaths(ACC_ID_2, PROPERTIES_ALT, PATH_BASIC_EDGE_SCHEMA_JSON);

        // Then
        final Collection<Graph> graphs = store.getGraphs(userContext.getUser(), ACC_ID_2, ignore);
        assertThat(graphs).hasSize(1);
        JsonAssert.assertEquals(JSONSerialiser.serialise(Schema.fromJson(StreamUtil.openStream(getClass(), PATH_BASIC_EDGE_SCHEMA_JSON))),
                JSONSerialiser.serialise(graphs.iterator().next().getSchema()));
    }

    @Test
    public void shouldNotAddGraphToLibraryWhenReinitialisingFederatedStoreWithGraphFromCache() throws Exception {
        // Check cache is empty
        federatedProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);
        assertThat(CacheServiceLoader.getService()).isNull();

        // initialise FedStore
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // add something so it will be in the cache
        final GraphSerialisable graphToAdd = new GraphSerialisable.Builder()
                .config(new GraphConfig(ACC_ID_1))
                .properties(PROPERTIES_1)
                .schema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_EDGE_SCHEMA_JSON))
                .build();

        store.addGraphs(null, TEST_USER_ID, true, graphToAdd);

        // check is in the store
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
        // check is in the cache
        assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME)).contains(ACC_ID_1);
        // check isn't in the LIBRARY
        assertThat(store.getGraphLibrary().get(ACC_ID_1)).isNull();

        // restart the store
        store = new FederatedStore();
        // clear and set the GraphLibrary again
        store.setGraphLibrary(library);
        // initialise the FedStore
        store.initialise(FEDERATED_STORE_ID, null, federatedProperties);

        // check is in the cache still
        assertThat(CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME))
                .withFailMessage(String.format("Keys: %s did not contain %s", CacheServiceLoader.getService().getAllKeysFromCache(CACHE_SERVICE_NAME), ACC_ID_1)).contains(ACC_ID_1);
        // check is in the store from the cache
        assertThat(store.getAllGraphIds(blankUser)).hasSize(1);
        // check the graph isn't in the GraphLibrary
        assertThat(store.getGraphLibrary().get(ACC_ID_1)).isNull();
    }

    private boolean checkUnexpected(final Collection<Graph> unexpectedGraphs, final Collection<Graph> returnedGraphs) {
        for (final Graph graph : unexpectedGraphs) {
            if (returnedGraphs.contains(graph)) {
                return true;
            }
        }
        return false;
    }

    private List<Collection<GraphSerialisable>> populateGraphs(final int... expectedIds) throws Exception {
        final Collection<GraphSerialisable> expectedGraphs = new ArrayList<>();
        final Collection<GraphSerialisable> unexpectedGraphs = new ArrayList<>();
        for (int i = 0; i < 6; i++) {
            final GraphSerialisable tempGraph = new GraphSerialisable.Builder()
                    .config(new GraphConfig.Builder()
                            .graphId("mockGraphId" + i)
                            .build())
                    .properties(PROPERTIES_ALT)
                    .schema(StreamUtil.openStream(FederatedStoreTest.class, PATH_BASIC_ENTITY_SCHEMA_JSON))
                    .build();
            // Odd ids are disabled by default
            final boolean disabledByDefault = 1 == Math.floorMod(i, 2);
            store.addGraphs(Sets.newHashSet(ALL_USERS), null, true, disabledByDefault, tempGraph);
            for (final int j : expectedIds) {
                if (i == j) {
                    expectedGraphs.add(tempGraph);
                }
            }
            if (!expectedGraphs.contains(tempGraph)) {
                unexpectedGraphs.add(tempGraph);
            }
        }
        final List<Collection<GraphSerialisable>> graphLists = new ArrayList<>();
        graphLists.add(expectedGraphs);
        graphLists.add(unexpectedGraphs);
        return graphLists;
    }

    private Set<Element> getElements() throws uk.gov.gchq.gaffer.operation.OperationException {
        final Iterable<? extends Element> elements = store
                .execute(new GetAllElements.Builder()
                        .view(new View.Builder()
                                .edges(store.getSchema().getEdgeGroups())
                                .entities(store.getSchema().getEntityGroups())
                                .build())
                        .build(), new Context(blankUser));

        return (null == elements) ? Sets.newHashSet() : Sets.newHashSet(elements);
    }

    private void assertContains(final Throwable e, final String format, final String... s) {
        final String expectedStr = String.format(format, s);
        assertThat(e.getMessage())
                .withFailMessage("\"" + e.getMessage() + "\" does not contain string \"" + expectedStr + "\"").contains(expectedStr);
    }

    private void addGraphWithIds(final String graphId, final String propertiesId, final String... schemaId)
            throws OperationException {
        final ArrayList<String> schemas = Lists.newArrayList(schemaId);
        store.execute(new AddGraph.Builder()
                .graphId(graphId)
                .parentPropertiesId(propertiesId)
                .isPublic(true)
                .parentSchemaIds(schemas)
                .build(), userContext);
    }

    private void addGraphWithPaths(final String graphId, final StoreProperties properties, final String... schemaPath)
            throws OperationException {
        final Schema.Builder schema = new Builder();
        for (final String path : schemaPath) {
            schema.merge(getSchemaFromPath(path));
        }

        store.execute(new AddGraph.Builder()
                .graphId(graphId)
                .storeProperties(properties)
                .isPublic(true)
                .schema(schema.build())
                .build(), userContext);
    }

    private Schema getSchemaFromPath(final String path) {
        return Schema.fromJson(StreamUtil.openStream(Schema.class, path));
    }

    private void clearCache() {
        CacheServiceLoader.shutdown();
    }

    private void clearLibrary() {
        HashMapGraphLibrary.clear();
    }

    @Test
    public void shouldGetAllElementsWhileHasConflictingSchemasDueToDiffVertexSerialiser() throws OperationException {
        // given
        final String expectedExceptionMeassage = "Unable to merge the schemas for all of your federated graphs: "
                + "\\[graph., graph.\\]\\. You can limit which graphs to query for using the operation option: "
                + "gaffer\\.federatedstore\\.operation\\.graphIds";

        final Entity A = getEntityA();
        final Entity B = getEntityB();

        final ArrayList<Entity> expectedAB = Lists.newArrayList(A, B);

        addElementsToNewGraph(A, "graphA", PATH_ENTITY_A_SCHEMA_JSON);
        addElementsToNewGraph(B, "graphB", PATH_ENTITY_B_SCHEMA_JSON);

        assertThatExceptionOfType(SchemaException.class).isThrownBy(() -> store.execute(new GetSchema.Builder().build(), userContext))
                .withMessageMatching(expectedExceptionMeassage);

        // when
        final Iterable<? extends Element> responseGraphsWithNoView = store.execute(new GetAllElements.Builder().build(), userContext);
        // then
        ElementUtil.assertElementEquals(expectedAB, responseGraphsWithNoView);
    }

    @Test
    public void shouldGetAllElementsFromSelectedRemoteGraphWhileHasConflictingSchemasDueToDiffVertexSerialiser()
            throws OperationException {
        // given
        final String expectedExceptionMeassage = "Unable to merge the schemas for all of your federated graphs: "
                + "\\[graph., graph.\\]\\. You can limit which graphs to query for using the operation option: "
                + "gaffer\\.federatedstore\\.operation\\.graphIds";

        final Entity A = getEntityA();
        final Entity B = getEntityB();

        final ArrayList<Entity> expectedA = Lists.newArrayList(A);
        final ArrayList<Entity> expectedB = Lists.newArrayList(B);

        addElementsToNewGraph(A, "graphA", PATH_ENTITY_A_SCHEMA_JSON);
        addElementsToNewGraph(B, "graphB", PATH_ENTITY_B_SCHEMA_JSON);

        assertThatExceptionOfType(SchemaException.class).isThrownBy(() -> store.execute(new GetSchema.Builder().build(), userContext))
                .withMessageMatching(expectedExceptionMeassage);

        // when
        final Iterable<? extends Element> responseGraphA = store.execute(new GetAllElements.Builder().option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphA").build(), userContext);
        final Iterable<? extends Element> responseGraphB = store.execute(new GetAllElements.Builder().option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphB").build(), userContext);
        // then
        ElementUtil.assertElementEquals(expectedA, responseGraphA);
        ElementUtil.assertElementEquals(expectedB, responseGraphB);

    }

    @Test
    public void shouldGetAllElementsFromSelectedGraphsWithViewOfExistingEntityGroupWhileHasConflictingSchemasDueToDiffVertexSerialiser()
            throws OperationException {
        // given
        final String expectedExceptionMeassage = "Unable to merge the schemas for all of your federated graphs: "
                + "\\[graph., graph.\\]\\. You can limit which graphs to query for using the operation option: "
                + "gaffer\\.federatedstore\\.operation\\.graphIds";

        final Entity A = getEntityA();
        final Entity B = getEntityB();

        final ArrayList<Entity> expectedA = Lists.newArrayList(A);
        final ArrayList<Entity> expectedB = Lists.newArrayList(B);

        addElementsToNewGraph(A, "graphA", PATH_ENTITY_A_SCHEMA_JSON);
        addElementsToNewGraph(B, "graphB", PATH_ENTITY_B_SCHEMA_JSON);

        assertThatExceptionOfType(SchemaException.class).isThrownBy(() -> store.execute(new GetSchema.Builder().build(), userContext))
                .withMessageMatching(expectedExceptionMeassage);

        // when
        final Iterable<? extends Element> responseGraphAWithAView = store.execute(new GetAllElements.Builder().option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphA").view(new View.Builder().entity("entityA").build()).build(), userContext);
        final Iterable<? extends Element> responseGraphBWithBView = store.execute(new GetAllElements.Builder().option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphB").view(new View.Builder().entity("entityB").build()).build(), userContext);
        final Iterable<? extends Element> responseAllGraphsWithAView = store.execute(new GetAllElements.Builder().option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphA,graphB").view(new View.Builder().entity("entityA").build()).build(), userContext);
        final Iterable<? extends Element> responseAllGraphsWithBView = store.execute(new GetAllElements.Builder().option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphA,graphB").view(new View.Builder().entity("entityB").build()).build(), userContext);
        // then
        ElementUtil.assertElementEquals(expectedA, responseGraphAWithAView);
        ElementUtil.assertElementEquals(expectedB, responseGraphBWithBView);
        ElementUtil.assertElementEquals(expectedA, responseAllGraphsWithAView);
        ElementUtil.assertElementEquals(expectedB, responseAllGraphsWithBView);

    }

    @Test
    public void shouldFailGetAllElementsFromSelectedGraphsWithViewOfMissingEntityGroupWhileHasConflictingSchemasDueToDiffVertexSerialiser()
            throws OperationException {
        // given
        final String expectedExceptionMeassageMatch = "Unable to merge the schemas for all of your federated graphs: "
                + "\\[graph., graph.\\]\\. You can limit which graphs to query for using the operation option: "
                + "gaffer\\.federatedstore\\.operation\\.graphIds";

        final Entity A = getEntityA();
        final Entity B = getEntityB();

        addElementsToNewGraph(A, "graphA", PATH_ENTITY_A_SCHEMA_JSON);
        addElementsToNewGraph(B, "graphB", PATH_ENTITY_B_SCHEMA_JSON);

        assertThatExceptionOfType(SchemaException.class).isThrownBy(() -> store.execute(new GetSchema.Builder().build(), userContext))
                .withMessageMatching(expectedExceptionMeassageMatch);

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new GetAllElements.Builder()
                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphA")
                        .view(new View.Builder().entity("entityB").build()).build(), userContext))
                .withMessage("Operation chain is invalid. Validation errors: \n"
                        + "View is not valid for graphIds:[graphA]\n"
                        + "(graphId: graphA) View for operation uk.gov.gchq.gaffer.operation.impl.get.GetAllElements is not valid. \n"
                        + "(graphId: graphA) Entity group entityB does not exist in the schema");

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new GetAllElements.Builder()
                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphB")
                        .view(new View.Builder().entity("entityA").build()).build(), userContext))
                .withMessage("Operation chain is invalid. Validation errors: \n"
                        + "View is not valid for graphIds:[graphB]\n"
                        + "(graphId: graphB) View for operation uk.gov.gchq.gaffer.operation.impl.get.GetAllElements is not valid. \n"
                        + "(graphId: graphB) Entity group entityA does not exist in the schema");

        addGraphWithPaths("graphC", PROPERTIES_1, PATH_ENTITY_B_SCHEMA_JSON);

        assertThatExceptionOfType(Exception.class)
                .isThrownBy(() -> store.execute(new GetAllElements.Builder()
                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, "graphB,graphC")
                        .view(new View.Builder().entity("entityA").build()).build(), userContext))
                .withMessage("Operation chain is invalid. Validation errors: \n"
                        + "View is not valid for graphIds:[graphB,graphC]\n"
                        + "(graphId: graphB) View for operation uk.gov.gchq.gaffer.operation.impl.get.GetAllElements is not valid. \n"
                        + "(graphId: graphB) Entity group entityA does not exist in the schema\n"
                        + "(graphId: graphC) View for operation uk.gov.gchq.gaffer.operation.impl.get.GetAllElements is not valid. \n"
                        + "(graphId: graphC) Entity group entityA does not exist in the schema");
    }

    protected void addElementsToNewGraph(final Entity input, final String graphName, final String pathSchemaJson)
            throws OperationException {
        addGraphWithPaths(graphName, PROPERTIES_1, pathSchemaJson);
        store.execute(new AddElements.Builder()
                .input(input)
                .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, graphName)
                .build(), userContext);
    }

    protected Entity getEntityB() {
        return new Entity.Builder()
                .group("entityB")
                .vertex(7)
                .build();
    }

    protected Entity getEntityA() {
        return new Entity.Builder()
                .group("entityA")
                .vertex("A")
                .build();
    }

    private class IgnoreOptions extends GetAllElements {
        @Override
        public void setOptions(final Map<String, String> options) {
            // nothing
        }
    }
}
