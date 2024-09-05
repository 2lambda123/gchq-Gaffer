/*
 * Copyright 2024 Crown Copyright
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

package uk.gov.gchq.gaffer.operation.impl.get;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.core.type.TypeReference;
import org.apache.commons.lang3.exception.CloneFailedException;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.koryphe.Since;
import uk.gov.gchq.koryphe.Summary;

import java.util.Map;

@Since("2.3.1")
@Summary("Operation to retrieve Graph created date")
@JsonPropertyOrder(alphabetic = true)
public class GetGraphCreatedTime implements Output<Map<String, String>> {
    private Map<String, String> options;

    @Override
    public Operation shallowClone() throws CloneFailedException {
        return new Builder()
                .options(options)
                .build();
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
    public TypeReference<Map<String, String>> getOutputTypeReference() {
        return new TypeReference<Map<String, String>>() { };

    }

    public static class Builder extends Operation.BaseBuilder<GetGraphCreatedTime, Builder> implements Output.Builder<GetGraphCreatedTime, Map<String, String>, Builder> {
        public Builder() {
            super(new GetGraphCreatedTime());
        }

    }
}