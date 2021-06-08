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

package uk.gov.gchq.gaffer.federatedstore.integration;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;

import java.util.Collections;

public class AbstractStandaloneFederatedStoreIT extends AbstractStoreIT {

    private static final FederatedStoreProperties STORE_PROPERTIES = FederatedStoreProperties.loadStoreProperties(
            StreamUtil.openStream(FederatedStoreITs.class, "publicAccessPredefinedFederatedStore.properties"));

    public AbstractStandaloneFederatedStoreIT() {
        AbstractStoreIT.setStoreProperties(STORE_PROPERTIES);
        AbstractStoreIT.setSkipTests(Collections.emptyMap());
        AbstractStoreIT.setSkipTestMethods(Collections.emptyMap());
    }
}
