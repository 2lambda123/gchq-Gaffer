/*
 * Copyright 2018-2019 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl;

import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.operation.serialisation.TypeReferenceImpl;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.Map;

@JsonPropertyOrder(value = {"class", "operationChain"}, alphabetic = true)
@Since("1.7.0")
@Summary("Validates an OperationChain")
public class ValidateOperationChain implements Output<ValidationResult> {
    @Required
    private OperationChain operationChain;
    private Map<String, String> options;

    public OperationChain getOperationChain() {
        return operationChain;
    }

    public void setOperationChain(final OperationChain operationChain) {
        this.operationChain = operationChain;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    @Override
    public TypeReference<ValidationResult> getOutputTypeReference() {
        return new TypeReferenceImpl.ValidationResult();
    }

    @Override
    public ValidateOperationChain shallowClone() {
        return new ValidateOperationChain.Builder()
                .operationChain(operationChain)
                .options(options)
                .build();
    }

    @Override
    public boolean equals(final Object o) {
        if (this == o) return true;

        if (o == null || getClass() != o.getClass()) return false;

        final ValidateOperationChain that = (ValidateOperationChain) o;

        return new EqualsBuilder()
                .append(operationChain, that.operationChain)
                .append(options, that.options)
                .isEquals();
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(17, 37)
                .append(operationChain)
                .append(options)
                .toHashCode();
    }

    public static class Builder extends BaseBuilder<ValidateOperationChain, Builder> implements
            Output.Builder<ValidateOperationChain, ValidationResult, Builder> {
        public Builder() {
            super(new ValidateOperationChain());
        }

        public Builder operationChain(final OperationChain opChain) {
            _getOp().setOperationChain(opChain);
            return _self();
        }
    }
}
