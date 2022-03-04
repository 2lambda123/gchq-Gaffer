/*
 * Copyright 2021 Crown Copyright
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

package uk.gov.gchq.gaffer.time.function;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.time.CommonTimeUtil.TimeBucket;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ToTimeBucketTest extends FunctionTest<ToTimeBucket> {
    private static final Long MILLI_TIMESTAMPS = Instant.now().toEpochMilli();

    @Test
    public void shouldCreateTimeBucketWithSingleTimeInIt() {
        // Given
        final ToTimeBucket toTimeBucket = new ToTimeBucket();
        toTimeBucket.setBucket(TimeBucket.MILLISECOND);
        // When
        Long result = toTimeBucket.apply(MILLI_TIMESTAMPS);

        // Then
        assertEquals(MILLI_TIMESTAMPS, result);

    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Long.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{Long.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ToTimeBucket toTimeBucket =
                new ToTimeBucket();
        // When
        final String json = new String(JSONSerialiser.serialise(toTimeBucket));
        ToTimeBucket deserialisedToTimeBucket = JSONSerialiser.deserialise(json, ToTimeBucket.class);
        // Then
        assertEquals(toTimeBucket, deserialisedToTimeBucket);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.ToTimeBucket\"}", json);
    }

    @Override
    protected ToTimeBucket getInstance() {
        return new ToTimeBucket();
    }

    @Override
    protected Iterable<ToTimeBucket> getDifferentInstancesOrNull() {
        return null;
    }
}
