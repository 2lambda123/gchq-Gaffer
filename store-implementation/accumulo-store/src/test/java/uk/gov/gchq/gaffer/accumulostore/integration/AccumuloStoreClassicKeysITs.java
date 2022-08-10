/*
 * Copyright 2016-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.accumulostore.integration;

import org.apache.commons.collections4.SetUtils;
import org.junit.platform.suite.api.ConfigurationParameter;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Optional;
import java.util.Set;


@ConfigurationParameter(key = "initClass", value = "uk.gov.gchq.gaffer.accumulostore.integration.AccumuloStoreClassicKeysITs")
public class AccumuloStoreClassicKeysITs extends AccumuloStoreITs {

    private static final AccumuloProperties STORE_PROPERTIES = AccumuloProperties.loadStoreProperties(StreamUtil.openStream(AccumuloStoreClassicKeysITs.class, "/accumuloStoreClassicKeys.properties"));

    private static final Set<Object> OBJECTS = SetUtils.unmodifiableSet(new Schema(), STORE_PROPERTIES);

    @Override
    public Optional<Set<Object>> getObjects() {
        return Optional.of(OBJECTS);
    }
}
