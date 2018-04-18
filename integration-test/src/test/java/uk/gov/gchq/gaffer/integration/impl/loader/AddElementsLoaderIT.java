/*
 * Copyright 2018 Crown Copyright
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

package uk.gov.gchq.gaffer.integration.impl.loader;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.integration.impl.loader.schemas.SchemaLoader;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.store.schema.TestSchema;
import uk.gov.gchq.gaffer.user.User;

import java.util.Map;

/**
 * {@link AbstractLoaderIT} implementation for the {@link AddElements} operation.
 */
public class AddElementsLoaderIT extends ParameterizedLoaderIT<AddElements> {

    public AddElementsLoaderIT(final TestSchema schema, final SchemaLoader loader, final Map<String, User> userMap) {
        super(schema, loader, userMap);
    }

    @Override
    public void configure(final Iterable<? extends Element> elements) {
        // Empty
    }

    @Override
    public AddElements createOperation(final Iterable<? extends Element> elements) {
        return new AddElements.Builder()
                .input(elements)
                .build();
    }

}
