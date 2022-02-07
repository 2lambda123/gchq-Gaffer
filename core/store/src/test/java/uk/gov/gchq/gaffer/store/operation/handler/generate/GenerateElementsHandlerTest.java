/*
 * Copyright 2016-2020 Crown Copyright
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

package uk.gov.gchq.gaffer.store.operation.handler.generate;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.ElementGenerator;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;

import java.util.Iterator;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.BDDMockito.given;

@ExtendWith(MockitoExtension.class)
public class GenerateElementsHandlerTest {

    @SuppressWarnings({"rawtypes", "unchecked"})
    @Test
    public void shouldReturnElements(@Mock final Store store,
                                     @Mock final GenerateElements<String> operation,
                                     @Mock final Iterable<Element> elements,
                                     @Mock final ElementGenerator<String> elementGenerator,
                                     @Mock final Iterable objs,
                                     @Mock final Iterator<Element> elementsIter)
            throws OperationException {
        // Given
        final GenerateElementsHandler<String> handler = new GenerateElementsHandler<>();
        final Context context = new Context();

        given(elements.iterator()).willReturn(elementsIter);
        given(elementGenerator.apply(objs)).willReturn(elements);
        given(operation.getInput()).willReturn(objs);
        given(operation.getElementGenerator()).willReturn(elementGenerator);

        // When
        final Iterable<? extends Element> result = handler.doOperation(operation, context, store);

        // Then
        assertThat(result.iterator()).isSameAs(elementsIter);
    }
}
