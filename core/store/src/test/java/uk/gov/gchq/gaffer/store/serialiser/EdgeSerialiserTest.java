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

package uk.gov.gchq.gaffer.store.serialiser;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.implementation.StringSerialiser;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEdgeDefinition;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class EdgeSerialiserTest {

    private Schema schema;
    private EdgeSerialiser serialiser;

    @BeforeEach
    public void setUp() {

        final SchemaEdgeDefinition edgeDef = new SchemaEdgeDefinition.Builder()
                .build();

        schema = new Schema.Builder()
                .vertexSerialiser(new StringSerialiser())
                .edge(TestGroups.EDGE, edgeDef)
                .build();
        serialiser = new EdgeSerialiser(schema);
    }

    @Test
    public void testNullSerialiser() {
        // Given
        schema = new Schema.Builder().build();

        // When / Then
        final Exception exception = assertThrows(IllegalArgumentException.class, () -> new EdgeSerialiser(schema));
        assertEquals("Vertex serialiser is required", exception.getMessage());
    }

    @Test
    public void testCanSerialiseEdge() throws SerialisationException {
        // Given
        final Edge edge = new Edge.Builder().group(TestGroups.EDGE)
                .source("source")
                .dest("destination")
                .directed(true)
                .build();

        // When
        final byte[] serialisedEdge = serialiser.serialise(edge);
        final Edge deserialisedEdge = serialiser.deserialise(serialisedEdge);

        // Then
        assertEquals(edge, deserialisedEdge);
    }

    @Test
    public void testCantSerialiseIntegerClass() {
        assertFalse(serialiser.canHandle(Integer.class));
    }

    @Test
    public void testCanSerialiseEdgeClass() {
        assertTrue(serialiser.canHandle(Edge.class));
    }

    @Test
    public void testDeserialiseEmpty() throws SerialisationException {
        assertNull(serialiser.deserialiseEmpty());
    }

    @Test
    public void testPreserveObjectOrdering() {
        assertFalse(serialiser.preservesObjectOrdering());
    }
}
