/*
 * Copyright 2020-2022 Crown Copyright
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

package uk.gov.gchq.gaffer.rest.controller;

import io.swagger.v3.oas.annotations.tags.Tag;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.servlet.mvc.method.annotation.StreamingResponseBody;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.rest.model.OperationDetail;

import java.util.Set;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;
import static org.springframework.web.bind.annotation.RequestMethod.GET;
import static org.springframework.web.bind.annotation.RequestMethod.POST;

@Tag(name = "operations")
@RequestMapping("/graph/operations")
public interface IOperationController {

    @RequestMapping(
            method = GET,
            path = "",
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Retrieves a list of supported operations"
    )
    Set<Class<? extends Operation>> getOperations();

    @RequestMapping(
            method = GET,
            path = "/details",
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Returns the details of every operation supported by the store"
    )
    Set<OperationDetail> getAllOperationDetails();

    @RequestMapping(
            method = GET,
            path = "/details/all",
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Returns the details of every operation"
    )
    Set<OperationDetail> getAllOperationDetailsIncludingUnsupported();

    @RequestMapping(
            method = GET,
            value = "{className}",
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Gets details about the specified operation class"
    )
    OperationDetail getOperationDetails(final String className);

    @RequestMapping(
            method = GET,
            value = "{className}/next",
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Gets the operations that can be chained after a given operation"
    )
    Set<Class<? extends Operation>> getNextOperations(final String className);

    @RequestMapping(
            method = GET,
            value = "{className}/example",
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Gets an example of an operation class"
    )
    Operation getOperationExample(final String className);


    @RequestMapping(
            method = POST,
            path = "/execute",
            consumes = APPLICATION_JSON_VALUE,
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Executes an operation against a Store"
    )
    ResponseEntity<Object> execute(final Operation operation);

    @RequestMapping(
            method = POST,
            path = "/execute/chunked",
            consumes = APPLICATION_JSON_VALUE,
            produces = APPLICATION_JSON_VALUE
    )
    @io.swagger.v3.oas.annotations.Operation(
            summary = "Executes an operation against a Store, returning a chunked output"
    )
    ResponseEntity<StreamingResponseBody> executeChunked(final Operation operation);
}
