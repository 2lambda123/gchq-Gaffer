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
package uk.gov.gchq.gaffer.operation.impl.compare;

import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.core.type.TypeReference;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.MultiInput;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import java.util.Comparator;

/**
 * A <code>Min</code> operation is intended as a terminal operation for retrieving the "minimum" element from an {@link java.lang.Iterable}.
 * This operation can be executed in two modes:
 * <ul><li>property comparator - a {@link java.util.Comparator} is provided, along with a property name. The supplied comparator is applied to all values of the specified property, and the element containing the minimum value (as specified by the {@link java.util.Comparator}) is returned.</li><li>element comparator - an {@link uk.gov.gchq.gaffer.data.element.Element} {@link java.util.Comparator} is provided, and is applied to all elements in the input {@link java.lang.Iterable}. The minimum element (as specified by the {@link java.util.Comparator} is returned.</li></ul>
 *
 * @see uk.gov.gchq.gaffer.operation.impl.compare.Min.Builder
 */
public class Min implements
        Operation,
        InputOutput<Iterable<? extends Element>, Element>,
        MultiInput<Element>,
        ElementComparison {

    private Iterable<? extends Element> input;
    private Comparator<Element> comparator;

    public Min() {
        // Empty
    }

    @Override
    @JsonTypeInfo(use = JsonTypeInfo.Id.CLASS, include = JsonTypeInfo.As.PROPERTY, property = "class")
    public Comparator<Element> getComparator() {
        return comparator;
    }

    public void setComparator(final Comparator<Element> comparator) {
        this.comparator = comparator;
    }

    @Override
    public Iterable<? extends Element> getInput() {
        return input;
    }

    @Override
    public void setInput(final Iterable<? extends Element> input) {
        this.input = input;
    }

    @Override
    public TypeReference<Element> getOutputTypeReference() {
        return new TypeReferenceImpl.Element();
    }

    public static final class Builder
            extends Operation.BaseBuilder<Min, Min.Builder>
            implements InputOutput.Builder<Min, Iterable<? extends Element>, Element, Min.Builder>,
            MultiInput.Builder<Min, Element, Min.Builder> {
        public Builder() {
            super(new Min());
        }

        public Min.Builder comparator(final Comparator<Element> comparator) {
            _getOp().setComparator(comparator);
            return _self();
        }
    }
}
