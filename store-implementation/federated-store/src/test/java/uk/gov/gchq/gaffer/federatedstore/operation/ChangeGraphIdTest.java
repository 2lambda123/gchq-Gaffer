/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation;

import static org.assertj.core.api.Assertions.assertThat;

public class ChangeGraphIdTest extends FederationOperationTest<ChangeGraphId> {
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final ChangeGraphId testObject = getTestObject();
        assertThat(testObject.isUserRequestingAdminUsage()).isTrue();
        assertThat(testObject.getGraphId()).isEqualTo("graphA");
        assertThat(testObject.getOptions()).containsEntry("a", "b");
    }

    @Override
    public void shouldShallowCloneOperation() {
        final ChangeGraphId testObject = getTestObject();

        final ChangeGraphId changeGraphId = testObject.shallowClone();

        assertThat(changeGraphId)
                .isNotNull()
                .isEqualTo(testObject);
    }

    @Override
    protected ChangeGraphId getTestObject() {
        return new ChangeGraphId.Builder()
                .graphId("graphA")
                .setUserRequestingAdminUsage(true)
                .option("a", "b")
                .build();
    }
}
