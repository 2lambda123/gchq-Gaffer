/*
 * Copyright 2020 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.integration;

import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.web.client.RestTemplate;

import uk.gov.gchq.gaffer.rest.serialisation.ObjectMapperProvider;

@Configuration
public class RestTemplateProvider {

    @Bean
    public RestTemplate createRestTemplate() {
        final RestTemplate restTemplate = new TestRestTemplate().getRestTemplate();
        restTemplate.getMessageConverters().removeIf(e -> e instanceof MappingJackson2HttpMessageConverter);
        restTemplate.getMessageConverters().add(createJsonMessageConverter());

        return restTemplate;
    }

    private MappingJackson2HttpMessageConverter createJsonMessageConverter() {
        final MappingJackson2HttpMessageConverter messageConverter = new MappingJackson2HttpMessageConverter();
        messageConverter.setObjectMapper(new ObjectMapperProvider().getObjectMapper());
        return messageConverter;
    }
}
