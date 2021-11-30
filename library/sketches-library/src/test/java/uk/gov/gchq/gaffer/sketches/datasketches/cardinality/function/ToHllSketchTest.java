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
package uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function;

import com.yahoo.sketches.hll.HllSketch;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;

import static org.assertj.core.api.Assertions.assertThat;
import static org.junit.jupiter.api.Assertions.assertEquals;

class ToHllSketchTest extends FunctionTest<ToHllSketch> {

    @Test
    public void shouldCreateEmptyWhenNull() {
        //Given
        ToHllSketch toHllSketch = new ToHllSketch();

        //When
        HllSketch result = toHllSketch.apply(null);

        //Then
        assertThat(result.getEstimate()).isEqualTo(0);
    }

    @Test
    public void shouldCreateHllSketch() {
        //Given
        ToHllSketch toHllSketch = new ToHllSketch();

        //When
        HllSketch result = toHllSketch.apply("input");

        //Then
        assertThat(result.getEstimate()).isEqualTo(1);
    }

    @Override
    protected Class[] getExpectedSignatureInputClasses() {
        return new Class[]{Object.class};
    }

    @Override
    protected Class[] getExpectedSignatureOutputClasses() {
        return new Class[]{HllSketch.class};
    }

    @Test
    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final ToHllSketch toHllSketch =
                new ToHllSketch();
        // When
        final String json = new String(JSONSerialiser.serialise(toHllSketch));
        ToHllSketch deserialisedToHllSketch = JSONSerialiser.deserialise(json, ToHllSketch.class);
        // Then
        assertEquals(toHllSketch, deserialisedToHllSketch);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.sketches.datasketches.cardinality.function.ToHllSketch\"}", json);
    }

    @Override
    protected ToHllSketch getInstance() {
        return new ToHllSketch();
    }

    @Override
    protected Iterable<ToHllSketch> getDifferentInstancesOrNull() {
        return null;
    }
}
