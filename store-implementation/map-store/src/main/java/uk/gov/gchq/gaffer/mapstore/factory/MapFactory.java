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
package uk.gov.gchq.gaffer.mapstore.factory;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.mapstore.multimap.MultiMap;
import uk.gov.gchq.gaffer.store.schema.Schema;
import java.util.Map;

public interface MapFactory {
    void initialise(final MapStoreProperties properties);

    <K, V> Map<K, V> getMap(final String mapName);

    void clear();

    <K, V> MultiMap<K, V> getMultiMap(String mapName);

    default <K, V> void updateValue(Map<K, V> map, K key, V adaptedValue) {
        // no action required.
    }

    Element cloneElement(final Element element, final Schema schema);
}
