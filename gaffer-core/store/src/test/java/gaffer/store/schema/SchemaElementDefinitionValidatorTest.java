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

package gaffer.store.schema;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;

import gaffer.commonutil.TestPropertyNames;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.ElementDefinitionValidator;
import gaffer.data.elementdefinition.ElementDefinitionValidatorTest;
import gaffer.function.AggregateFunction;
import gaffer.function.context.PassThroughFunctionContext;
import org.junit.Test;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;

public class SchemaElementDefinitionValidatorTest extends ElementDefinitionValidatorTest {

    @Test
    public void shouldValidateAndReturnTrueWhenNoPropertiesAggregated() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<String>());
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(mock(ElementAggregator.class));

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertTrue(isValid);
    }

    @Test
    public void shouldValidateAndReturnTrueWhenAggregatorIsValid() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final ElementAggregator aggregator = mock(ElementAggregator.class);
        final PassThroughFunctionContext<ElementComponentKey, AggregateFunction> context1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        final ElementComponentKey key1 = new ElementComponentKey(TestPropertyNames.PROP_1);
        final ElementComponentKey key2 = new ElementComponentKey(TestPropertyNames.PROP_2);
        final List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Arrays.asList(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)));
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(context1.getSelection()).willReturn(Arrays.asList(key1, key2));
        given(function.getInputClasses()).willReturn(new Class[]{String.class, Integer.class});
        given(context1.getFunction()).willReturn(function);
        given(aggregator.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) Integer.class);
        given(elementDef.getClass(key1)).willReturn((Class) String.class);
        given(elementDef.getClass(key2)).willReturn((Class) Integer.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertTrue(isValid);
        verify(elementDef).getClass(key1);
        verify(elementDef).getClass(key2);
        verify(function).getInputClasses();
    }

    @Test
    public void shouldValidateAndReturnFalseWhenAPropertyDoesNotHaveAnAggregateFunction() {
        // Given
        final SchemaElementDefinition elementDef = mock(SchemaElementDefinition.class);
        final SchemaElementDefinitionValidator validator = new SchemaElementDefinitionValidator();
        final ElementAggregator aggregator = mock(ElementAggregator.class);
        final PassThroughFunctionContext<ElementComponentKey, AggregateFunction> context1 = mock(PassThroughFunctionContext.class);
        final AggregateFunction function = mock(AggregateFunction.class);
        final ElementComponentKey key1 = new ElementComponentKey(TestPropertyNames.PROP_1);
        final List<PassThroughFunctionContext<ElementComponentKey, AggregateFunction>> contexts = new ArrayList<>();
        contexts.add(context1);

        given(elementDef.getIdentifiers()).willReturn(new HashSet<IdentifierType>());
        given(elementDef.getProperties()).willReturn(new HashSet<>(Arrays.asList(TestPropertyNames.PROP_1, TestPropertyNames.PROP_2)));
        given(elementDef.getValidator()).willReturn(mock(ElementFilter.class));
        given(elementDef.getAggregator()).willReturn(aggregator);
        given(context1.getSelection()).willReturn(Collections.singletonList(key1));
        given(function.getInputClasses()).willReturn(new Class[]{String.class, Integer.class});
        given(context1.getFunction()).willReturn(function);
        given(aggregator.getFunctions()).willReturn(contexts);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_1)).willReturn((Class) String.class);
        given(elementDef.getPropertyClass(TestPropertyNames.PROP_2)).willReturn((Class) Integer.class);

        // When
        final boolean isValid = validator.validate(elementDef);

        // Then
        assertFalse(isValid);
    }

    @Override
    protected ElementDefinitionValidator newElementDefinitionValidator() {
        return new SchemaElementDefinitionValidator();
    }
}