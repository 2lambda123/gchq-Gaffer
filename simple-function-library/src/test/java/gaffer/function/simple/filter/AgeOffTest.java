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
package gaffer.function.simple.filter;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNotSame;
import static org.junit.Assert.assertTrue;

import gaffer.exception.SerialisationException;
import gaffer.function.FilterFunctionTest;
import gaffer.jsonserialisation.JSONSerialiser;
import org.junit.Test;

public class AgeOffTest extends FilterFunctionTest {
    public static final int MINUTE_IN_MILLISECONDS = 60000;
    public static final long CUSTOM_AGE_OFF = 100000;

    @Test
    public void shouldUseDefaultAgeOffTime() {
        // Given
        final AgeOff filter = new AgeOff();

        // When
        final long ageOfTime = filter.getAgeOffTime();

        // Then
        assertEquals(AgeOff.AGE_OFF_TIME_DEFAULT, ageOfTime);
    }

    @Test
    public void shouldSetAgeOffInDays() {
        // Given
        final int ageOffInDays = 10;
        final AgeOff filter = new AgeOff();
        filter.setAgeOffDays(ageOffInDays);

        // When
        final long ageOfTime = filter.getAgeOffTime();

        // Then
        assertEquals(AgeOff.DAYS_TO_MILLISECONDS * ageOffInDays, ageOfTime);
    }

    @Test
    public void shouldSetAgeOffInHours() {
        // Given
        final int ageOffInHours = 10;
        final AgeOff filter = new AgeOff();
        filter.setAgeOffHours(ageOffInHours);

        // When
        final long ageOfTime = filter.getAgeOffTime();

        // Then
        assertEquals(AgeOff.HOURS_TO_MILLISECONDS * ageOffInHours, ageOfTime);
    }

    @Test
    public void shouldAcceptWhenWithinAgeOffLimit() {
        // Given
        final AgeOff filter = new AgeOff(CUSTOM_AGE_OFF);

        // When
        final boolean accepted = filter._isValid(System.currentTimeMillis() - CUSTOM_AGE_OFF + MINUTE_IN_MILLISECONDS);

        // Then
        assertTrue(accepted);
    }

    @Test
    public void shouldAcceptWhenOutsideAgeOffLimit() {
        // Given
        final AgeOff filter = new AgeOff(CUSTOM_AGE_OFF);

        // When
        final boolean accepted = filter._isValid(System.currentTimeMillis() - CUSTOM_AGE_OFF - MINUTE_IN_MILLISECONDS);

        // Then
        assertFalse(accepted);
    }

    @Test
    public void shouldClone() {
        // Given
        final AgeOff filter = new AgeOff(CUSTOM_AGE_OFF);

        // When
        final AgeOff clonedFilter = filter.statelessClone();

        // Then
        assertNotSame(filter, clonedFilter);
        assertNotSame(CUSTOM_AGE_OFF, clonedFilter.getAgeOffTime());
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() throws SerialisationException {
        // Given
        final AgeOff filter = new AgeOff(CUSTOM_AGE_OFF);

        // When
        final String json = new String(new JSONSerialiser().serialise(filter, true));

        // Then
        assertEquals("{\n" +
                "  \"class\" : \"gaffer.function.simple.filter.AgeOff\",\n" +
                "  \"ageOffTime\" : 100000\n" +
                "}", json);

        // When 2
        final AgeOff deserialisedFilter = new JSONSerialiser().deserialise(json.getBytes(), AgeOff.class);

        // Then 2
        assertNotNull(deserialisedFilter);
        assertEquals(CUSTOM_AGE_OFF, deserialisedFilter.getAgeOffTime());
    }

    @Override
    protected Class<AgeOff> getFunctionClass() {
        return AgeOff.class;
    }

    @Override
    protected AgeOff getInstance() {
        return new AgeOff();
    }

    @Override
    protected Object[] getSomeAcceptedInput() {
        return new Object[]{System.currentTimeMillis()};
    }
}
