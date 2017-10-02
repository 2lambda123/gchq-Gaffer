/*
 * Copyright 2017 Crown Copyright
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
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.MockAccumuloStore;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.data.elementdefinition.exception.SchemaException;
import uk.gov.gchq.gaffer.federatedstore.operation.AddGraph;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.library.HashMapGraphLibrary;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.binaryoperator.StringConcat;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class FederatedStoreSchemaTest {

    private static final String STRING = "string";
    private static final Schema STRING_SCHEMA = new Schema.Builder()
            .type(STRING, new TypeDefinition.Builder()
                    .clazz(String.class)
                    .aggregateFunction(new StringConcat())
                    .build())
            .build();
    private static final User TEST_USER = new User("testUser");
    private static final String TEST_FED_STORE = "testFedStore";


    private FederatedStore fStore;
    private static final AccumuloProperties ACCUMULO_PROPERTIES = new AccumuloProperties();
    private static final StoreProperties FEDERATED_PROPERTIES = new StoreProperties();
    private static final String CACHE_SERVICE_CLASS_STRING = "uk.gov.gchq.gaffer.cache.impl.HashMapCacheService";

    @Before
    public void setUp() throws Exception {
        ACCUMULO_PROPERTIES.setStoreClass(MockAccumuloStore.class.getName());
        ACCUMULO_PROPERTIES.setStorePropertiesClass(AccumuloProperties.class);
        ACCUMULO_PROPERTIES.set(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);

        FEDERATED_PROPERTIES.setStoreClass(FederatedStore.class.getName());
        FEDERATED_PROPERTIES.setStorePropertiesClass(StoreProperties.class);
        FEDERATED_PROPERTIES.set(CacheProperties.CACHE_SERVICE_CLASS, CACHE_SERVICE_CLASS_STRING);

        fStore = new FederatedStore();
        fStore.initialise(TEST_FED_STORE, null, FEDERATED_PROPERTIES);

        HashMapGraphLibrary.clear();
    }

    @After
    public void tearDown() throws Exception {
        HashMapGraphLibrary.clear();

    }

    @Test
    public void shouldNotDeadLockWhenPreviousAddGraphHasSchemaCollision() throws Exception {
        final HashMapGraphLibrary library = new HashMapGraphLibrary();
        library.addProperties("accProp", ACCUMULO_PROPERTIES);
        fStore.setGraphLibrary(library);


        final Schema aSchema = new Schema.Builder()
                .edge("e1", getProp("prop1"))
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema("aSchema", aSchema);

        fStore.execute(Operation.asOperationChain(
                new AddGraph.Builder()
                        .graphId("a")
                        .parentPropertiesId("accProp")
                        .parentSchemaIds(Lists.newArrayList("aSchema"))
                        .build()), TEST_USER);

        final Schema bSchema = new Schema.Builder()
                .edge("e1", getProp("prop2"))
                .merge(STRING_SCHEMA)
                .build();

        library.addSchema("bSchema", bSchema);

        assertFalse(library.exists("b"));

        boolean addingGraphBWasSuccessful = true;

        try {
            fStore.execute(Operation.asOperationChain(new AddGraph.Builder()
                    .graphId("b")
                    .parentPropertiesId("accProp")
                    .parentSchemaIds(Lists.newArrayList("bSchema"))
                    .build()), TEST_USER);
        } catch (final Exception e) {
            addingGraphBWasSuccessful = false;
            assertTrue(e instanceof SchemaException);
            assertEquals("Element group properties cannot be defined in different" +
                    " schema parts, they must all be defined in a single " +
                    "schema part. Please fix this group: e1", e.getMessage());
        }

        try {
            fStore.execute(Operation.asOperationChain(new AddGraph.Builder()
                    .graphId("c")
                    .parentPropertiesId("accProp")
                    .parentSchemaIds(Lists.newArrayList("aSchema"))
                    .build()), TEST_USER);

            assertFalse("If this assertion failed then it is possible this " +
                    "test is no longer needed, because Schema Collisions are not" +
                    " being thrown when adding graph \"a\". So deadlock will not" +
                    " occur, please examine.", addingGraphBWasSuccessful);

        } catch (final Exception e) {
            assertFalse("This test is not behaving how it was designed, " +
                    "Adding graph\"c\" should never fail if adding graph \"b\" was successful!", addingGraphBWasSuccessful);
            assertFalse("Deadlock has occurred, If exception is thrown, then graph \"b\" should not have been added to the library", library.exists("b"));
        }
    }

    private SchemaEdgeDefinition getProp(final String propName) {
        return new SchemaEdgeDefinition.Builder()
                .source(STRING)
                .destination(STRING)
                .property(propName, STRING)
                .build();
    }


}