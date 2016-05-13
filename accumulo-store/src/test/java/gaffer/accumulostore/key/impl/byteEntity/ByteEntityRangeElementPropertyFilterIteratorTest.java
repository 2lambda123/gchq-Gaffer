/*
 * Copyright 2016 Crown Copyright
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

package gaffer.accumulostore.key.impl.byteEntity;

import static org.junit.Assert.assertEquals;

import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityRangeElementPropertyFilterIterator;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.TestGroups;
import gaffer.data.element.Edge;
import gaffer.data.element.Element;
import gaffer.data.element.Entity;
import gaffer.operation.OperationException;
import gaffer.store.schema.Schema;
import gaffer.store.schema.SchemaEdgeDefinition;
import gaffer.store.schema.SchemaEntityDefinition;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.junit.Test;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ByteEntityRangeElementPropertyFilterIteratorTest {
    private static final Schema SCHEMA = new Schema.Builder()
            .edge(TestGroups.EDGE, new SchemaEdgeDefinition.Builder()
                    .source(String.class)
                    .destination(String.class)
                    .directed(Boolean.class)
                    .build())
            .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                    .vertex(String.class)
                    .build())
            .build();

    private static final List<Element> ELEMENTS = Arrays.asList(
            new Edge(TestGroups.EDGE, "vertexA", "vertexB", true),
            new Edge(TestGroups.EDGE, "vertexD", "vertexC", true),
            new Edge(TestGroups.EDGE, "vertexE", "vertexE", true),
            new Edge(TestGroups.EDGE, "vertexF", "vertexG", false),
            new Edge(TestGroups.EDGE, "vertexH", "vertexH", false),
            new Entity(TestGroups.ENTITY, "vertexI")
    );

    private final ByteEntityAccumuloElementConverter converter = new ByteEntityAccumuloElementConverter(SCHEMA);

    @Test
    public void shouldOnlyAcceptDeduplicatedEdges() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
            put(AccumuloStoreConstants.DEDUPLICATE_UNDIRECTED_EDGES, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final Pair<Key> keys = converter.getKeysFromElement(element);
            // First key is deduplicated, but only edges should be excepted
            assertEquals("Failed for element: " + element.toString(), element instanceof Edge, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDeduplicatedDirectedEdges() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.DEDUPLICATE_UNDIRECTED_EDGES, "true");
            put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
            put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final Pair<Key> keys = converter.getKeysFromElement(element);
            // First key is deduplicated, but only directed edges should be excepted
            final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDeduplicatedUndirectedEdges() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.DEDUPLICATE_UNDIRECTED_EDGES, "true");
            put(AccumuloStoreConstants.UNDIRECTED_EDGE_ONLY, "true");
            put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final Pair<Key> keys = converter.getKeysFromElement(element);
            // First key is deduplicated, but only undirected edges should be excepted
            final boolean expectedResult = element instanceof Edge && !((Edge) element).isDirected();
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptDirectedEdges() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
            final Pair<Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptUndirectedEdges() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.UNDIRECTED_EDGE_ONLY, "true");
            put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Edge && !((Edge) element).isDirected();
            final Pair<Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptIncomingEdges() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
            put(AccumuloStoreConstants.INCOMING_EDGE_ONLY, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final Pair<Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldOnlyAcceptOutgoingEdges() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.DIRECTED_EDGE_ONLY, "true");
            put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final Pair<Key> keys = converter.getKeysFromElement(element);
            final boolean expectedResult = element instanceof Edge && ((Edge) element).isDirected();
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // self elements are not added the other way round
                assertEquals("Failed for element: " + element.toString(), false, filter.accept(keys.getSecond(), value));
            }
        }
    }

    @Test
    public void shouldAcceptOnlyEntities() throws OperationException, AccumuloElementConversionException {
        // Given
        final ByteEntityRangeElementPropertyFilterIterator filter = new ByteEntityRangeElementPropertyFilterIterator();
        final Map<String, String> options = new HashMap<String, String>() {{
            put(AccumuloStoreConstants.NO_EDGES, "true");
            put(AccumuloStoreConstants.INCLUDE_ENTITIES, "true");
            put(AccumuloStoreConstants.OUTGOING_EDGE_ONLY, "true");
        }};
        filter.validateOptions(options);

        final Value value = null; // value should not be used

        // When / Then
        for (Element element : ELEMENTS) {
            final boolean expectedResult = element instanceof Entity;
            final Pair<Key> keys = converter.getKeysFromElement(element);
            assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getFirst(), value));
            if (null != keys.getSecond()) {
                // entities and self edges are not added the other way round
                assertEquals("Failed for element: " + element.toString(), expectedResult, filter.accept(keys.getSecond(), value));
            }
        }
    }
}
