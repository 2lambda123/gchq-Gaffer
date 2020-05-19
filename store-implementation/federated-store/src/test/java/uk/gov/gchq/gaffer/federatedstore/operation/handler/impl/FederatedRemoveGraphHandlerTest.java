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


import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMiniAccumuloStore;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.RemoveGraph;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collection;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static uk.gov.gchq.gaffer.user.StoreUser.testUser;

public class FederatedRemoveGraphHandlerTest {
    private static final String FEDERATEDSTORE_GRAPH_ID = "federatedStore";
    private static final String EXPECTED_GRAPH_ID = "testGraphID";
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";
    private User testUser;
    private static SingleUseMiniAccumuloStore byteEntityStore;
    private static AccumuloProperties byteEntityStoreProperties;

    @BeforeAll
    public static void setUpDatabase() throws StoreException {
        AccumuloProperties allProperties = new AccumuloProperties();
        allProperties.setStoreClass(SingleUseMiniAccumuloStore.class);
        allProperties.setZookeepers("aZookeeper");
        allProperties.setInstance("instance01");
        allProperties.setUser("user01");
        allProperties.setPassword("password01");
        byteEntityStore = new SingleUseMiniAccumuloStore();
        byteEntityStoreProperties = (AccumuloProperties) byteEntityStore.setUpTestDB(allProperties);
    }

    @AfterAll
    public static void tearDown() {
        byteEntityStore.tearDownTestDB();
    }

    @BeforeEach
    public void setUp() throws Exception {
        CacheServiceLoader.shutdown();
        testUser = testUser();
    }

    @Test
    public void shouldRemoveGraphForAddingUser() throws Exception {
        FederatedStore store = new FederatedStore();
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);

        store.addGraphs(testUser.getOpAuths(), testUser.getUserId(), false, new GraphSerialisable.Builder()
                .config(new GraphConfig(EXPECTED_GRAPH_ID))
                .schema(new Schema.Builder().build())
                .properties(byteEntityStoreProperties)
                .build());

        assertEquals(1, store.getGraphs(testUser, null).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null);

        assertEquals(0, graphs.size());

    }

    @Test
    public void shouldNotRemoveGraphForNonAddingUser() throws Exception {
        FederatedStore store = new FederatedStore();
        final FederatedStoreProperties federatedStoreProperties = new FederatedStoreProperties();
        federatedStoreProperties.setCacheProperties(CACHE_SERVICE_CLASS_STRING);

        store.initialise(FEDERATEDSTORE_GRAPH_ID, null, federatedStoreProperties);
        AccumuloProperties storeProperties = new AccumuloProperties();
        storeProperties.setStoreClass(SingleUseMockAccumuloStore.class);

        store.addGraphs(testUser.getOpAuths(), "other", false, new GraphSerialisable.Builder()
                .config(new GraphConfig(EXPECTED_GRAPH_ID))
                .schema(new Schema.Builder().build())
                .properties(storeProperties)
                .build());

        assertEquals(1, store.getGraphs(testUser, null).size());

        new FederatedRemoveGraphHandler().doOperation(
                new RemoveGraph.Builder()
                        .graphId(EXPECTED_GRAPH_ID)
                        .build(),
                new Context(testUser),
                store);

        Collection<Graph> graphs = store.getGraphs(testUser, null);

        assertEquals(1, graphs.size());

    }
}
