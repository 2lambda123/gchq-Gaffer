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

package uk.gov.gchq.gaffer.serialisation.implementation.ordered.datetime;

import uk.gov.gchq.gaffer.serialisation.DelegateSerialiser;
import uk.gov.gchq.gaffer.serialisation.implementation.ordered.OrderedLongSerialiser;
import java.time.LocalDateTime;
import java.time.ZoneOffset;

public class OrderedLocalDateTimeSerialiser extends DelegateSerialiser<LocalDateTime, Long> {

    private static final long serialVersionUID = 6636121009320739764L;
    private static final OrderedLongSerialiser LONG_SERIALISER = new OrderedLongSerialiser();

    public OrderedLocalDateTimeSerialiser() {
        super(LONG_SERIALISER);
    }

    @Override
    public LocalDateTime fromDelegateType(final Long object) {
        return LocalDateTime.ofEpochSecond(object, 0, ZoneOffset.UTC);
    }

    @Override
    public Long toDelegateType(final LocalDateTime object) {
        return object.toEpochSecond(ZoneOffset.UTC);
    }

    @Override
    public boolean canHandle(final Class clazz) {
        return LocalDateTime.class.equals(clazz);
    }
}
