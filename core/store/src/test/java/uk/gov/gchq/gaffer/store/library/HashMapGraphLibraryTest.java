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

package uk.gov.gchq.gaffer.store.library;

import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.pair.Pair;
import uk.gov.gchq.gaffer.store.exception.OverwritingException;
import uk.gov.gchq.gaffer.store.StoreProperties;
import uk.gov.gchq.gaffer.store.schema.Schema;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class HashMapGraphLibraryTest {

    HashMapGraphLibrary hashMapGraphLibrary = new HashMapGraphLibrary();
    private static final String GRAPH_ID = "hashMapTestGraphId";
    private static final String SCHEMA_ID = "hashMapTestSchemaId";
    private static final String PROPERTIES_ID = "hashMapTestPropertiesId";
    private static StoreProperties storeProperties = new StoreProperties(PROPERTIES_ID);
    private static Schema schema = new Schema.Builder().id(SCHEMA_ID).build();

    @Test
    public void shouldGetIdsInHashMapGraphLibrary() {

        // When
        hashMapGraphLibrary.add(GRAPH_ID, schema, storeProperties);

        // Then
        assertEquals(new Pair<>(SCHEMA_ID, PROPERTIES_ID), hashMapGraphLibrary.getIds(GRAPH_ID));
    }

    @Test
    public void shouldClearHashMaps() {

        // Given
        hashMapGraphLibrary.add(GRAPH_ID, schema, storeProperties);

        // When
        hashMapGraphLibrary.clear();

        // Then
        assertEquals(null, hashMapGraphLibrary.getIds(GRAPH_ID));
        assertEquals(null, hashMapGraphLibrary.getSchema(SCHEMA_ID));
        assertEquals(null, hashMapGraphLibrary.getProperties(PROPERTIES_ID));
    }

    @Test
    public void shouldThrowExceptionWithInvalidGraphId() {

        // When / Then
        try {
            hashMapGraphLibrary.add(GRAPH_ID + "@#", schema, storeProperties);
            fail("Exception expected");
        } catch (IllegalArgumentException e) {
            assertNotNull(e.getMessage());
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentSchemaExists() {

        // Given
        hashMapGraphLibrary.add(GRAPH_ID, schema, storeProperties);
        Schema schema1 = new Schema.Builder()
                .id("hashMapTestSchemaId1")
                .build();

        // When / Then
        try {
            hashMapGraphLibrary.add(GRAPH_ID, schema1, storeProperties);
            fail("Exception expected");
        } catch (OverwritingException e) {
            assertTrue(e.getMessage().contains("already exists with a different schema"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGraphIdWithDifferentPropertiesExists() {

        // Given
        hashMapGraphLibrary.add(GRAPH_ID, schema, storeProperties);
        StoreProperties storeProperties1 = new StoreProperties("hashMapTestPropertiesId1");

        // When / Then
        try {
            hashMapGraphLibrary.add(GRAPH_ID, schema, storeProperties1);
            fail("Exception expected");
        } catch (Exception e) {
            assertTrue(e.getMessage().contains("already exists with a different store properties"));
        }
    }

}
