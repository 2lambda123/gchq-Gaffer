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

package gaffer.data.elementdefinition.view;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import org.junit.Test;

public class ViewElementDefinitionTest {
    @Test
    public void shouldBuildElementDefinition() {
        // Given
        final ElementTransformer transformer = mock(ElementTransformer.class);
        final ElementFilter filter = mock(ElementFilter.class);

        // When
        final ViewElementDefinition elementDef = new ViewElementDefinition.Builder()
                .transientProperty(TestPropertyNames.PROP_1, String.class)
                .transientProperty(TestPropertyNames.PROP_2, String.class)
                .transformer(transformer)
                .filter(filter)
                .build();

        // Then
        assertEquals(2, elementDef.getTransientProperties().size());
        assertTrue(elementDef.containsTransientProperty(TestPropertyNames.PROP_1));
        assertTrue(elementDef.containsTransientProperty(TestPropertyNames.PROP_2));

        assertSame(filter, elementDef.getFilter());
        assertSame(transformer, elementDef.getTransformer());
    }
}