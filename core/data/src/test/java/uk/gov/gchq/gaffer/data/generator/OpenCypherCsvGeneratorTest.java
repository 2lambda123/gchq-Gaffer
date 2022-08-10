/*
 * Copyright 2022 Crown Copyright
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

package uk.gov.gchq.gaffer.data.generator;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.data.element.Entity;

import java.util.LinkedHashMap;

import static org.assertj.core.api.Assertions.assertThat;

class OpenCypherCsvGeneratorTest {

    @Test
    public void builderShouldCreatePopulatedGenerator() {
        // Given
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator.Builder()
                .build();

        // Then
        assertThat(openCypherCsvGenerator.getFields())
                .hasSize(5);
        assertThat(openCypherCsvGenerator.isNeo4jFormat()).isFalse();
    }

    @Test
    public void shouldSetHeadersToMatchNeptunesFormat() {
        // Given
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("countProperty", "int");
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator.Builder()
                .headers(fields)
                .neo4jFormat(false)
                .build();
        openCypherCsvGenerator.setFields(fields);
        // Then
        assertThat(openCypherCsvGenerator.getHeader()).isEqualTo(":ID,:LABEL,:TYPE,:START_ID,:END_ID,countProperty:int");

    }

    @Test
    public void shouldSetHeadersToMatchNeo4jFormat() {
        // Given
        final LinkedHashMap<String, String> fields = new LinkedHashMap<>();
        fields.put("countProperty", "int");
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator.Builder()
                .headers(fields)
                .neo4jFormat(true)
                .build();
        openCypherCsvGenerator.setFields(fields);
        // Then
        assertThat(openCypherCsvGenerator.getHeader()).isEqualTo("_id,_labels,_type,_start,_end,countProperty:int");

    }

    @Test
    public void shouldReturnEmptyStringIfBuilderNotUsed() {
        // Given
        Entity entity = new Entity.Builder()
                .group("Foo")
                .vertex("vertex")
                .property("propertyName", "propertyValue")
                .build();
        final OpenCypherCsvGenerator openCypherCsvGenerator = new OpenCypherCsvGenerator();
        // Then
        assertThat(openCypherCsvGenerator._apply(entity)).isEqualTo("");
    }
}
