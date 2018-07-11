/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.graph.hook.migrate;

import org.junit.Test;

import uk.gov.gchq.gaffer.JSONSerialisationTest;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.data.element.function.ElementTransformer;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.function.migration.ToInteger;
import uk.gov.gchq.gaffer.operation.function.migration.ToLong;

import static org.junit.Assert.assertEquals;

public class MigrateElementTest extends JSONSerialisationTest<MigrateElement> {

    @Test
    public void shouldJsonSerialiseAndDeserialiseTwice() {
        // Given
        final MigrateElement testObject = getTestObject();

        // When
        final byte[] json = toJson(testObject);
        final MigrateElement deserialisedOnceTestObject = fromJson(json);

        // Then
        assertEquals(getTestObject(), deserialisedOnceTestObject);

        // When
        final byte[] seriailisedTwiceJson = toJson(deserialisedOnceTestObject);
        final MigrateElement deserialisedTwiceTestObject = fromJson(seriailisedTwiceJson);

        // Then
        assertEquals(getTestObject(), deserialisedTwiceTestObject);
    }

    @Override
    protected MigrateElement getTestObject() {
        return new MigrateElement(
                MigrateElement.ElementType.ENTITY,
                "entityOld",
                "entityNew",
                new ElementTransformer.Builder()
                        .select("count")
                        .execute(new ToLong())
                        .project("count")
                        .build(),
                new ElementTransformer.Builder()
                        .select("count")
                        .execute(new ToInteger())
                        .project("count")
                        .build());
    }

    protected MigrateElement fromJson(final String path) {
        try {
            return JSONSerialiser.deserialise(StreamUtil.openStream(getClass(), path), MigrateElement.class);
        } catch (final SerialisationException e) {
            throw new RuntimeException(e);
        }
    }
}
