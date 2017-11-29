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
package uk.gov.gchq.gaffer.integration.impl;

import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.commonutil.TestTypes;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaEntityDefinition;

import java.util.Arrays;

import static org.junit.Assert.assertTrue;

public class AddElementsIT extends AbstractStoreIT {

    public static final Entity VALID = new Entity.Builder()
            .group(TestGroups.ENTITY_2)
            .vertex("1")
            .property(TestPropertyNames.TIMESTAMP, Long.MAX_VALUE)
            .property(TestPropertyNames.INT, 1)
            .build();
    public static final Entity INVALID = new Entity.Builder()
            .group(TestGroups.ENTITY_2)
            .vertex("2")
            .property(TestPropertyNames.TIMESTAMP, 1L)
            .property(TestPropertyNames.INT, 21)
            .build();

    @Override
    public void addDefaultElements() throws OperationException {
        // do not add any elements
    }

    @Test
    public void shouldThrowExceptionWithUsefulMessageWhenInvalidElementsAdded() throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(VALID, INVALID)
                .build();


        // When / Then
        try {
            graph.execute(addElements, getUser());
        } catch (final Exception e) {
            String msg = e.getMessage();
            if (!msg.contains("Element of type Entity") && null != e.getCause()) {
                msg = e.getCause().getMessage();
            }

            assertTrue("Message was: " + msg, msg.contains("IsLessThan"));
            assertTrue("Message was: " + msg, msg.contains("returned false for properties: {intProperty: <java.lang.Integer>21}"));
            assertTrue("Message was: " + msg, msg.contains("AgeOff"));
            assertTrue("Message was: " + msg, msg.contains("returned false for properties: {timestamp: <java.lang.Long>1}"));
        }
    }

    @Test
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithSkipInvalidSetToTrue() throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(VALID, INVALID)
                .skipInvalidElements(true)
                .build();

        // When
        graph.execute(addElements, getUser());

        // Then - no exceptions
    }

    @Test
    public void shouldNotThrowExceptionWhenInvalidElementsAddedWithValidateSetToFalse() throws OperationException {
        // Given
        final AddElements addElements = new AddElements.Builder()
                .input(VALID, INVALID)
                .validate(false)
                .build();

        // When
        graph.execute(addElements, getUser());

        // Then - no exceptions
    }

    @Test
    public void shouldAddElementsWithSameTimestampWithoutAggregation() throws OperationException {
        // Given
        final Graph graphWithNoAggregation = createGraphWithNoAggregation();
        final Entity entity = new Entity.Builder()
                .group(TestGroups.ENTITY)
                .vertex("1")
                .property(TestPropertyNames.TIMESTAMP, 1L)
                .build();

        final AddElements addElements = new AddElements.Builder()
                .input(entity, entity)
                .build();

        // When
        graphWithNoAggregation.execute(addElements, getUser());

        // Then
        final CloseableIterable<? extends Element> allElements = graphWithNoAggregation.execute(new GetAllElements(), getUser());
        ElementUtil.assertElementEquals(Arrays.asList(entity, entity), allElements);
    }

    private Graph createGraphWithNoAggregation() {
        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("integrationTestGraphWithNoAggregation")
                        .build())
                .storeProperties(getStoreProperties())
                .addSchema(createSchemaNoVisibility())
                .addSchema(getStoreSchema())
                .build();
    }

    private Schema createSchemaNoVisibility() {
        return new Schema.Builder()
                .entity(TestGroups.ENTITY, new SchemaEntityDefinition.Builder()
                        .vertex(TestTypes.ID_STRING)
                        .property(TestPropertyNames.TIMESTAMP, TestTypes.TIMESTAMP)
                        .aggregate(false)
                        .build())
                .type(TestTypes.ID_STRING, String.class)
                .type(TestTypes.TIMESTAMP, Long.class)
                .timestampProperty(TestTypes.TIMESTAMP)
                .build();
    }
}
