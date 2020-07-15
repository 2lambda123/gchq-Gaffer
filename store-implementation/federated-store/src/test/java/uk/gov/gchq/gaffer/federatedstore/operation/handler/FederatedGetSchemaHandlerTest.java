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
package uk.gov.gchq.gaffer.federatedstore.operation.handler;

import com.google.common.collect.Lists;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MiniAccumuloClusterManager;
import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStore;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.operation.GetSchema;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static uk.gov.gchq.gaffer.store.TestTypes.DIRECTED_EITHER;

public class FederatedGetSchemaHandlerTest {
    private static final Logger LOGGER = LoggerFactory.getLogger(FederatedGetSchemaHandlerTest.class);
    private FederatedGetSchemaHandler handler;
    private FederatedStore fStore;
    private Context context;
    private User user;
    private StoreProperties properties;
    private static final String ACC_PROP_ID = "accProp";
    private static final String EDGE_SCHEMA_ID = "edgeSchema";

    private static final String TEST_FED_STORE = "testFedStore";
    private final HashMapGraphLibrary library = new HashMapGraphLibrary();
    private static final Schema STRING_SCHEMA = new Schema.Builder()
            .type("string", new TypeDefinition.Builder()
                    .clazz(String.class)
                    .serialiser(new StringSerialiser())
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();

    private static Class currentClass = new Object() { }.getClass().getEnclosingClass();
    private static final AccumuloProperties PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(currentClass, "properties/accumuloStore.properties"));
    private static MiniAccumuloClusterManager miniAccumuloClusterManager;

    @ClassRule
    public static TemporaryFolder storeBaseFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void setUpStore() {
        File storeFolder = null;
        try {
            storeFolder = storeBaseFolder.newFolder();
        } catch (IOException e) {
            LOGGER.error("Failed to create sub folder in : " + storeBaseFolder.getRoot().getAbsolutePath() + ": " + e.getMessage());
        }
        miniAccumuloClusterManager = new MiniAccumuloClusterManager(PROPERTIES, storeFolder.getAbsolutePath());
    }

    @AfterClass
    public static void tearDownStore() {
        miniAccumuloClusterManager.close();
    }

    @Before
    public void setup() throws StoreException {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();

        handler = new FederatedGetSchemaHandler();
        user = new User("testUser");
        context = new Context(user);
        properties = new FederatedStoreProperties();
        properties.set(HashMapCacheService.STATIC_CACHE, String.valueOf(true));

        fStore = new FederatedStore();
        fStore.initialise(TEST_FED_STORE, null, properties);

        library.clear();
    }

    @After
    public void after() {
        HashMapGraphLibrary.clear();
        CacheServiceLoader.shutdown();
    }

    @Test
    public void shouldReturnSchema() throws OperationException {
        library.addProperties(ACC_PROP_ID, PROPERTIES);
        fStore.setGraphLibrary(library);

        final Schema edgeSchema = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .directed(DIRECTED_EITHER)
                        .property("prop1", "string")
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema(EDGE_SCHEMA_ID, edgeSchema);

        fStore.execute(Operation.asOperationChain(
                new AddGraph.Builder()
                        .graphId("schema")
                        .parentPropertiesId(ACC_PROP_ID)
                        .parentSchemaIds(Lists.newArrayList(EDGE_SCHEMA_ID))
                        .build()), context);

        final GetSchema operation = new GetSchema.Builder()
                .compact(true)
                .build();

        // When
        final Schema result = handler.doOperation(operation, context, fStore);

        // Then
        assertNotNull(result);
        JsonAssert.assertEquals(edgeSchema.toJson(true), result.toJson(true));
    }

    @Test
    public void shouldReturnSchemaOnlyForEnabledGraphs() throws OperationException {
        library.addProperties(ACC_PROP_ID, PROPERTIES);
        fStore.setGraphLibrary(library);

        final Schema edgeSchema1 = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .property("prop1", "string")
                        .directed(DIRECTED_EITHER)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema("edgeSchema1", edgeSchema1);

        final Schema edgeSchema2 = new Schema.Builder()
                .edge("edge", new SchemaEdgeDefinition.Builder()
                        .source("string")
                        .destination("string")
                        .property("prop2", "string")
                        .directed(DIRECTED_EITHER)
                        .build())
                .vertexSerialiser(new StringSerialiser())
                .type(DIRECTED_EITHER, Boolean.class)
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema("edgeSchema2", edgeSchema2);

        fStore.execute(Operation.asOperationChain(
                new AddGraph.Builder()
                        .graphId("schemaEnabled")
                        .parentPropertiesId(ACC_PROP_ID)
                        .parentSchemaIds(Lists.newArrayList("edgeSchema1"))
                        .disabledByDefault(false)
                        .build()), context);

        fStore.execute(Operation.asOperationChain(
                new AddGraph.Builder()
                        .graphId("schemaDisabled")
                        .parentPropertiesId(ACC_PROP_ID)
                        .parentSchemaIds(Lists.newArrayList("edgeSchema2"))
                        .disabledByDefault(true)
                        .build()), context);

        final GetSchema operation = new GetSchema.Builder()
                .compact(true)
                .build();

        // When
        final Schema result = handler.doOperation(operation, context, fStore);

        // Then
        assertNotNull(result);
        JsonAssert.assertEquals(edgeSchema1.toJson(true), result.toJson(true));
    }

    @Test
    public void shouldThrowExceptionForANullOperation() throws OperationException {
        library.addProperties(ACC_PROP_ID, PROPERTIES);
        fStore.setGraphLibrary(library);

        final GetSchema operation = null;

        try {
            handler.doOperation(operation, context, fStore);
        } catch (final OperationException e) {
            assertTrue(e.getMessage().contains("Operation cannot be null"));
        }
    }
}
