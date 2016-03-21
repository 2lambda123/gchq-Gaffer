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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.element.function.ElementTransformer;
import gaffer.data.elementdefinition.ElementDefinition;
import gaffer.function.FilterFunction;
import gaffer.function.TransformFunction;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.ConsumerProducerFunctionContext;
import java.util.List;

/**
 * A <code>ViewElementDefinition</code> extends {@link ElementDefinition} and adds
 * the ability to specify a {@link gaffer.data.element.function.ElementTransformer} and a
 * {@link gaffer.data.element.function.ElementFilter}.
 */
public abstract class ViewElementDefinition extends ElementDefinition {
    private ElementTransformer transformer;
    private ElementFilter filter;

    public ViewElementDefinition() {
        super(new ViewElementDefinitionValidator());
    }

    @JsonIgnore
    public ElementTransformer getTransformer() {
        return transformer;
    }

    public void setTransformer(final ElementTransformer transformer) {
        this.transformer = transformer;
    }

    @JsonIgnore
    public ElementFilter getFilter() {
        return filter;
    }

    public void setFilter(final ElementFilter filter) {
        this.filter = filter;
    }

    @JsonGetter("filterFunctions")
    public List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> getFilterFunctions() {
        return null != filter ? filter.getFunctions() : null;
    }

    @JsonSetter("filterFunctions")
    public void addFilterFunctions(final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> functions) {
        if (null == filter) {
            filter = new ElementFilter();
        }

        filter.addFunctions(functions);
    }

    @JsonGetter("transformFunctions")
    public List<ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction>> getTransformFunctions() {
        return null != transformer ? transformer.getFunctions() : null;
    }

    @JsonSetter("transformFunctions")
    public void addTransformFunctions(final List<ConsumerProducerFunctionContext<ElementComponentKey, TransformFunction>> functions) {
        transformer = new ElementTransformer();
        transformer.addFunctions(functions);
    }

    public abstract static class Builder extends ElementDefinition.Builder {
        public Builder(final ViewElementDefinition elDef) {
            super(elDef);
        }

        protected Builder property(final String propertyName, final Class<?> clazz) {
            return (Builder) super.property(propertyName, clazz.getName());
        }

        protected Builder identifier(final IdentifierType identifierType, final Class<?> clazz) {
            return (Builder) super.identifier(identifierType, clazz.getName());
        }

        protected Builder filter(final ElementFilter filter) {
            getElementDef().setFilter(filter);
            return this;
        }

        protected Builder transformer(final ElementTransformer transformer) {
            getElementDef().setTransformer(transformer);
            return this;
        }

        @Override
        protected ViewElementDefinition build() {
            return (ViewElementDefinition) super.build();
        }

        @Override
        protected ViewElementDefinition getElementDef() {
            return (ViewElementDefinition) super.getElementDef();
        }
    }
}
