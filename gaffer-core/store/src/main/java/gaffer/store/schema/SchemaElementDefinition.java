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

import com.fasterxml.jackson.annotation.JsonGetter;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonSetter;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import gaffer.data.TransformIterable;
import gaffer.data.element.ElementComponentKey;
import gaffer.data.element.IdentifierType;
import gaffer.data.element.function.ElementAggregator;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.ElementDefinition;
import gaffer.function.FilterFunction;
import gaffer.function.IsA;
import gaffer.function.context.ConsumerFunctionContext;
import gaffer.function.context.PassThroughFunctionContext;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * A <code>SchemaElementDefinition</code> is the representation of a single group in a
 * {@link Schema}.
 * Each element needs identifiers and can optionally have properties, an aggregator and a validator.
 *
 * @see SchemaElementDefinition.Builder
 */
public abstract class SchemaElementDefinition extends ElementDefinition {
    private static final long serialVersionUID = -8077961120272676568L;
    private ElementFilter validator;

    /**
     * The <code>TypeDefinitions</code> provides the different element identifier value types and property value types.
     *
     * @see TypeDefinitions
     */
    private TypeDefinitions typesLookup;

    /**
     * Constructs a <code>SchemaElementDefinition</code> with a <code>SchemaElementDefinitionValidator</code> to validate
     * this <code>SchemaElementDefinition</code>.
     *
     * @see SchemaElementDefinitionValidator
     */
    public SchemaElementDefinition() {
        super(new SchemaElementDefinitionValidator());
    }

    /**
     * @return a cloned instance of {@link ElementAggregator} fully populated with all the
     * {@link gaffer.function.AggregateFunction}s defined in this
     * {@link SchemaElementDefinition} and also the
     * {@link gaffer.function.AggregateFunction}s defined in the corresponding property value
     * {@link TypeDefinition}s.
     */
    @JsonIgnore
    public ElementAggregator getAggregator() {
        final ElementAggregator aggregator = new ElementAggregator();
        for (Map.Entry<String, String> entry : getPropertyMap().entrySet()) {
            addTypeAggregateFunctions(aggregator, new ElementComponentKey(entry.getKey()), entry.getValue());
        }

        return aggregator;
    }

    /**
     * @return a cloned instance of {@link gaffer.data.element.function.ElementFilter} fully populated with all the
     * {@link gaffer.function.FilterFunction}s defined in this
     * {@link SchemaElementDefinition} and also the
     * {@link SchemaElementDefinition} and also the
     * {@link gaffer.function.FilterFunction}s defined in the corresponding identifier and property value
     * {@link TypeDefinition}s.
     */
    @JsonIgnore
    public ElementFilter getValidator() {
        return getValidator(true);
    }

    public ElementFilter getValidator(final boolean includeIsA) {
        final ElementFilter fullValidator = null != validator ? validator.clone() : new ElementFilter();
        for (Map.Entry<IdentifierType, String> entry : getIdentifierMap().entrySet()) {
            final ElementComponentKey key = new ElementComponentKey(entry.getKey());
            if (includeIsA) {
                addIsAFunction(fullValidator, key, entry.getValue());
            }
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }
        for (Map.Entry<String, String> entry : getPropertyMap().entrySet()) {
            final ElementComponentKey key = new ElementComponentKey(entry.getKey());
            if (includeIsA) {
                addIsAFunction(fullValidator, key, entry.getValue());
            }
            addTypeValidatorFunctions(fullValidator, key, entry.getValue());
        }

        return fullValidator;
    }

    @JsonSetter("validator")
    private void setValidator(final ElementFilter validator) {
        this.validator = validator;
    }

    @SuppressFBWarnings(value = "PZLA_PREFER_ZERO_LENGTH_ARRAYS", justification = "null is only returned when the validator is null")
    @JsonGetter("validateFunctions")
    public ConsumerFunctionContext<ElementComponentKey, FilterFunction>[] getOriginalValidateFunctions() {
        if (null != validator) {
            final List<ConsumerFunctionContext<ElementComponentKey, FilterFunction>> functions = validator.getFunctions();
            return functions.toArray(new ConsumerFunctionContext[functions.size()]);
        }

        return null;
    }

    @JsonSetter("validateFunctions")
    public void addValidateFunctions(final ConsumerFunctionContext<ElementComponentKey, FilterFunction>... functions) {
        if (null == validator) {
            validator = new ElementFilter();
        }
        validator.addFunctions(Arrays.asList(functions));
    }

    public void setTypesLookup(final TypeDefinitions newTypes) {
        if (null != typesLookup && null != newTypes) {
            newTypes.merge(typesLookup);
        }

        typesLookup = newTypes;
    }

    @JsonIgnore
    public Iterable<TypeDefinition> getPropertyTypeDefs() {
        return new TransformIterable<String, TypeDefinition>(getPropertyMap().values()) {
            @Override
            protected TypeDefinition transform(final String typeName) {
                return getTypeDef(typeName);
            }
        };
    }

    @JsonIgnore
    public Iterable<TypeDefinition> getIdentifierTypeDefs() {
        return new TransformIterable<String, TypeDefinition>(getIdentifierMap().values()) {
            @Override
            protected TypeDefinition transform(final String typeName) {
                return getTypeDef(typeName);
            }
        };
    }

    public TypeDefinition getPropertyTypeDef(final String property) {
        if (containsProperty(property)) {
            return getTypeDef(getPropertyMap().get(property));
        }

        return null;
    }

    public TypeDefinition getIdentifierTypeDef(final IdentifierType idType) {
        if (containsIdentifier(idType)) {
            return getTypeDef(getIdentifierMap().get(idType));
        }

        return null;
    }

    @Override
    public Class<?> getPropertyClass(final String propertyName) {
        final String typeName = super.getPropertyTypeName(propertyName);
        return null != typeName ? getTypeDef(typeName).getClazz() : null;
    }

    @Override
    public Class<?> getIdentifierClass(final IdentifierType idType) {
        final String typeName = super.getIdentifierTypeName(idType);
        return null != typeName ? getTypeDef(typeName).getClazz() : null;
    }

    @Override
    public void merge(final ElementDefinition elementDef) {
        if (elementDef instanceof SchemaElementDefinition) {
            merge(((SchemaElementDefinition) elementDef));
        } else {
            super.merge(elementDef);
        }
    }

    public void merge(final SchemaElementDefinition elementDef) {
        super.merge(elementDef);
        if (null == validator) {
            validator = elementDef.validator;
        } else if (null != elementDef.getOriginalValidateFunctions()) {
            validator.addFunctions(Arrays.asList(elementDef.getOriginalValidateFunctions()));
        }
    }

    @JsonIgnore
    protected TypeDefinitions getTypesLookup() {
        if (null == typesLookup) {
            setTypesLookup(new TypeDefinitions());
        }

        return typesLookup;
    }

    private void addTypeValidatorFunctions(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        final TypeDefinition type = getTypeDef(classOrTypeName);
        if (null != type.getValidator()) {
            for (ConsumerFunctionContext<ElementComponentKey, FilterFunction> function : type.getValidator().clone().getFunctions()) {
                final List<ElementComponentKey> selection = function.getSelection();
                if (null == selection || selection.isEmpty()) {
                    function.setSelection(Collections.singletonList(key));
                } else if (!selection.contains(key)) {
                    selection.add(key);
                }
                fullValidator.addFunction(function);
            }
        }
    }

    private void addTypeAggregateFunctions(final ElementAggregator aggregator, final ElementComponentKey key, final String typeName) {
        final TypeDefinition type = getTypeDef(typeName);
        if (null != type.getAggregateFunction()) {
            aggregator.addFunction(new PassThroughFunctionContext<>(type.getAggregateFunction().statelessClone(), Collections.singletonList(key)));
        }
    }

    private void addIsAFunction(final ElementFilter fullValidator, final ElementComponentKey key, final String classOrTypeName) {
        fullValidator.addFunction(
                new ConsumerFunctionContext<ElementComponentKey, FilterFunction>(
                        new IsA(getTypeDef(classOrTypeName).getClazz()), Collections.singletonList(key)));
    }

    private TypeDefinition getTypeDef(final String typeName) {
        return getTypesLookup().getType(typeName);
    }

    protected static class Builder extends ElementDefinition.Builder {
        protected Builder(final SchemaElementDefinition elDef) {
            super(elDef);
        }

        protected Builder validator(final ElementFilter validator) {
            getElementDef().setValidator(validator);
            return this;
        }

        protected Builder property(final String propertyName, final Class<?> clazz) {
            return property(propertyName, clazz.getName(), clazz);
        }

        protected Builder property(final String propertyName, final String typeName, final TypeDefinition type) {
            type(typeName, type);
            return (Builder) property(propertyName, typeName);
        }

        protected Builder property(final String propertyName, final String typeName, final Class<?> typeClass) {
            return property(propertyName, typeName, new TypeDefinition(typeClass));
        }

        protected Builder type(final String typeName, final TypeDefinition type) {
            final TypeDefinitions types = getElementDef().getTypesLookup();
            final TypeDefinition exisitingType = types.get(typeName);
            if (null == exisitingType) {
                types.put(typeName, type);
            } else if (!exisitingType.equals(type)) {
                throw new IllegalArgumentException("The type provided conflicts with an existing type with the same name");
            }

            return this;
        }

        protected SchemaElementDefinition build() {
            return (SchemaElementDefinition) super.build();
        }

        protected SchemaElementDefinition getElementDef() {
            return (SchemaElementDefinition) super.getElementDef();
        }
    }
}
