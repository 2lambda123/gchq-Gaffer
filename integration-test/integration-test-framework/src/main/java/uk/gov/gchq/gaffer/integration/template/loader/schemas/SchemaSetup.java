/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.template.loader.schemas;

import uk.gov.gchq.gaffer.store.schema.TestSchema;

public enum SchemaSetup {

    AGGREGATION(TestSchema.AGGREGATION_SCHEMA, new AggregationSchemaLoader()),
    BASIC(TestSchema.BASIC_SCHEMA, new BasicSchemaLoader()),
    VISIBILITY(TestSchema.VISIBILITY_SCHEMA, new VisibilitySchemaLoader()),
    FULL(TestSchema.FULL_SCHEMA, new FullSchemaLoader());

    private final ISchemaLoader schemaLoader;
    private final TestSchema testSchema;

    SchemaSetup(final TestSchema testSchema, final ISchemaLoader loader) {
        this.testSchema = testSchema;
        this.schemaLoader = loader;
    }

    public TestSchema getTestSchema() {
        return testSchema;
    }

    public ISchemaLoader getLoader() {
        return schemaLoader;
    }
}
