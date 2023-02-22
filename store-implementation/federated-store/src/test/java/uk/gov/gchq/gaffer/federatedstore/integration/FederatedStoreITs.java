/*
 * Copyright 2017-2022 Crown Copyright
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

import org.junit.platform.suite.api.ConfigurationParameter;

import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.federatedstore.FederatedStoreProperties;
import uk.gov.gchq.gaffer.integration.AbstractStoreITs;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.Collections;
import java.util.Map;

import static uk.gov.gchq.gaffer.integration.junit.extensions.IntegrationTestSuiteExtension.INIT_CLASS;

@ConfigurationParameter(key = INIT_CLASS, value = "uk.gov.gchq.gaffer.federatedstore.integration.FederatedStoreITs")
public class FederatedStoreITs extends AbstractStoreITs {

    /*
     * Currently this file overrides the default merges used by IT's,
     * this means the IT do not test FederatedStore out the box.
     * Either change IT for all store - No.
     * or Update default merge for GetAllElements to handle Post-Transform - Hard, do later.
     * or set default merge for GetAlLElements to concatenation.
     */
    private static final FederatedStoreProperties STORE_PROPERTIES = FederatedStoreProperties.loadStoreProperties(
            StreamUtil.openStream(FederatedStoreITs.class, "integrationTestPublicAccessPredefinedFederatedStore.properties"));

    private static final Schema SCHEMA = new Schema();

    private static final Map<String, String> TESTS_TO_SKIP =
            Collections.singletonMap("shouldReturnNoResultsWhenNoEntityResults",
                    "Fails due to the way we split the entities and edges into 2 graphs");

    FederatedStoreITs() {
        setSchema(SCHEMA);
        setStoreProperties(STORE_PROPERTIES);
        setTestsToSkip(TESTS_TO_SKIP);
    }
}
