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
import uk.gov.gchq.gaffer.time.CommonTimeUtil;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Instant;

import static org.junit.jupiter.api.Assertions.assertEquals;

class ToTimeBucketStartTest extends FunctionTest<ToTimeBucketStart> {
    private static final Long SECOND_TIMESTAMPS = Instant.now().getEpochSecond();

    @Test
    public void shouldCreateTimeBucketWithSingleTimeInIt() {
        // Given
        final ToTimeBucketStart toTimeBucketStart = new ToTimeBucketStart();
        toTimeBucketStart.setBucket(CommonTimeUtil.TimeBucket.SECOND);
        // When
        Long result = toTimeBucketStart.apply(SECOND_TIMESTAMPS);
        long expected = (((long) Math.ceil(SECOND_TIMESTAMPS)) / 1000) * 1000;
        // Then
        assertEquals(expected, result);
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
        final ToTimeBucketStart toTimeBucketStart =
                new ToTimeBucketStart();
        // When
        final String json = new String(JSONSerialiser.serialise(toTimeBucketStart));
        ToTimeBucketStart deserialisedToTimeBucketStart = JSONSerialiser.deserialise(json, ToTimeBucketStart.class);
        // Then
        assertEquals(toTimeBucketStart, deserialisedToTimeBucketStart);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.ToTimeBucketStart\"}", json);
    }

    @Override
    protected ToTimeBucketStart getInstance() {
        return new ToTimeBucketStart();
    }

    @Override
    protected Iterable<ToTimeBucketStart> getDifferentInstancesOrNull() {
        return null;
    }
}
