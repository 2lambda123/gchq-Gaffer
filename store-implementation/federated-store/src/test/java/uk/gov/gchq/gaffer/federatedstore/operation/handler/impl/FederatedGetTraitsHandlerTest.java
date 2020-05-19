/*
 * Copyright 2017-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import com.google.common.collect.Sets;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreConstants;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAdjacentIds;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.serialisation.Serialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.StoreTrait;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetTraits;
import uk.gov.gchq.gaffer.store.operation.handler.OperationHandler;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.store.StoreTrait.MATCHED_VERTEX;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.POST_TRANSFORMATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.PRE_AGGREGATION_FILTERING;
import static uk.gov.gchq.gaffer.store.StoreTrait.TRANSFORMATION;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedGetTraitsHandlerTest {
    public static final String ALT_STORE = "altStore";
    public static final String FED_STORE_ID = "fedStoreId";
    public static final String ACC_STORE = "accStore";
    private FederatedStore federatedStore;
    private FederatedStoreProperties properties;

    private static SingleUseMiniAccumuloStore byteEntityStore;
    private static AccumuloProperties byteEntityStoreProperties;

    @BeforeAll
    public static void setUpDatabase() throws StoreException {
        byteEntityStore = new SingleUseMiniAccumuloStore();
        byteEntityStoreProperties = (AccumuloProperties) byteEntityStore.setUpTestDB(StoreProperties.loadStoreProperties("/properties/singleUseMiniAccStore.properties"));
    }

    @AfterAll
    public static void tearDown() {
        byteEntityStore.tearDownTestDB();
    }

    @BeforeEach
    public void setUp() throws Exception {
        federatedStore = new FederatedStore();
        properties = new FederatedStoreProperties();
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldGetAllTraitsForEmptyStore() throws Exception {
        // Given
        federatedStore.initialise(FED_STORE_ID, null, properties);

        // When
        final Set<StoreTrait> traits = federatedStore.execute(
                new GetTraits.Builder()
                        .currentTraits(false)
                        .build(),
                new Context(testUser()));

        // Then
        assertEquals(StoreTrait.ALL_TRAITS, traits);
    }

    @Test
    public void shouldGetAllTraitsForEmptyStoreWithCurrentTraits() throws Exception {
        // Given
        federatedStore.initialise(FED_STORE_ID, null, properties);
        assertEquals(0, federatedStore.getAllGraphIds(testUser()).size(), "graph is not starting empty");

        // When
        final Set<StoreTrait> traits = federatedStore.execute(new GetTraits.Builder()
                .currentTraits(true)
                .build(), new Context(testUser()));

        // Then
        assertEquals(StoreTrait.ALL_TRAITS, traits);
    }

    @Test
    public void shouldGetAllTraitsWhenContainsStoreWithOtherTraits() throws Exception {
        // Given
        federatedStore.initialise(FED_STORE_ID, null, properties);
        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(ALT_STORE)
                .storeProperties(new TestStorePropertiesImpl())
                .schema(new Schema())
                .build(), new Context(testUser()));

        // When
        final Set<StoreTrait> traits = federatedStore.execute(
                new GetTraits.Builder()
                        .currentTraits(false)
                        .build(),
                new Context(testUser()));

        // Then
        assertEquals(StoreTrait.ALL_TRAITS, traits);
    }

    @Test
    public void shouldGetCurrentTraitsWhenContainsStoreWithOtherTraits() throws Exception {
        // Given
        federatedStore.initialise(FED_STORE_ID, null, properties);
        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(ALT_STORE)
                .storeProperties(new TestStorePropertiesImpl())
                .schema(new Schema())
                .build(), new Context(testUser()));

        // When
        final Set<StoreTrait> traits = federatedStore.execute(
                new GetTraits.Builder()
                        .currentTraits(true)
                        .build(),
                new Context(testUser()));

        // Then
        assertEquals(
                Sets.newHashSet(
                        TRANSFORMATION,
                        MATCHED_VERTEX,
                        PRE_AGGREGATION_FILTERING,
                        POST_AGGREGATION_FILTERING,
                        POST_TRANSFORMATION_FILTERING
                ),
                traits);
    }

    @Test
    public void shouldGetCurrentTraitsWhenContainsStoreWithOtherTraitsWithOptions() throws Exception {
        // Given
        federatedStore.initialise(FED_STORE_ID, null, properties);

        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(ALT_STORE)
                .storeProperties(new TestStorePropertiesImpl())
                .schema(new Schema())
                .build(), new Context(testUser()));

        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(ACC_STORE)
                .storeProperties(byteEntityStoreProperties)
                .schema(new Schema())
                .build(), new Context(testUser()));

        // When
        final Set<StoreTrait> traits = federatedStore.execute(
                new GetTraits.Builder()
                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, ALT_STORE)
                        .currentTraits(true)
                        .build(),
                new Context(testUser()));

        // Then
        assertEquals(
                Sets.newHashSet(
                        TRANSFORMATION,
                        MATCHED_VERTEX,
                        PRE_AGGREGATION_FILTERING,
                        POST_AGGREGATION_FILTERING,
                        POST_TRANSFORMATION_FILTERING
                ),
                traits);
    }

    @Test
    public void shouldGetAllTraitsWhenContainsStoreWithOtherTraitsWithOptions() throws Exception {
        // Given
        federatedStore.initialise(FED_STORE_ID, null, properties);

        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(ALT_STORE)
                .storeProperties(new TestStorePropertiesImpl())
                .schema(new Schema())
                .build(), new Context(testUser()));

        federatedStore.execute(new AddGraph.Builder()
                .isPublic(true)
                .graphId(ACC_STORE)
                .storeProperties(byteEntityStoreProperties)
                .schema(new Schema())
                .build(), new Context(testUser()));

        // When
        final Set<StoreTrait> traits = federatedStore.execute(
                new GetTraits.Builder()
                        .option(FederatedStoreConstants.KEY_OPERATION_OPTIONS_GRAPH_IDS, ALT_STORE)
                        .currentTraits(false)
                        .build(),
                new Context(testUser()));

        // Then
        assertEquals(StoreTrait.ALL_TRAITS, traits);
    }

    public static class TestStorePropertiesImpl extends StoreProperties {
        public TestStorePropertiesImpl() {
            super(TestStoreImpl.class);
        }
    }

    public static class TestStoreImpl extends Store {

        @Override
        public Set<StoreTrait> getTraits() {
            return new HashSet<>(Arrays.asList(
                    StoreTrait.INGEST_AGGREGATION,
                    StoreTrait.PRE_AGGREGATION_FILTERING,
                    StoreTrait.POST_AGGREGATION_FILTERING,
                    StoreTrait.TRANSFORMATION,
                    StoreTrait.POST_TRANSFORMATION_FILTERING,
                    StoreTrait.MATCHED_VERTEX));
        }

        @Override
        protected void addAdditionalOperationHandlers() {

        }

        @Override
        protected OutputOperationHandler<GetElements, CloseableIterable<? extends Element>> getGetElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<GetAllElements, CloseableIterable<? extends Element>> getGetAllElementsHandler() {
            return null;
        }

        @Override
        protected OutputOperationHandler<? extends GetAdjacentIds, CloseableIterable<? extends EntityId>> getAdjacentIdsHandler() {
            return null;
        }

        @Override
        protected OperationHandler<? extends AddElements> getAddElementsHandler() {
            return null;
        }

        @Override
        protected Class<? extends Serialiser> getRequiredParentSerialiserClass() {
            return null;
        }
    }
}
