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

package uk.gov.gchq.gaffer.rest.controller;

import io.swagger.annotations.ApiOperation;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;

import uk.gov.gchq.gaffer.jobtracker.Job;
import uk.gov.gchq.gaffer.jobtracker.JobDetail;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationException;

import static org.springframework.http.MediaType.APPLICATION_JSON_VALUE;

@RequestMapping("/graph/jobs")
public interface IJobController {

    @PostMapping(
            consumes = APPLICATION_JSON_VALUE,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Kicks off an asynchronous job",
            response = JobDetail.class
    )
    ResponseEntity<JobDetail> startJob(final Operation operation) throws OperationException;

    @PostMapping(
            path = "/schedule",
            consumes = APPLICATION_JSON_VALUE,
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "schedules an asynchronous job",
            response = JobDetail.class
    )
    ResponseEntity<JobDetail> scheduleJob(final Job job) throws OperationException;

    @GetMapping(
            path = "/{id}",
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Retrieves the details of an asynchronous job",
            response = JobDetail.class
    )
    JobDetail getDetails(final String id) throws OperationException;

    @GetMapping(
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation(
            value = "Retrieves the details of all the asynchronous jobs",
            response = JobDetail.class,
            responseContainer = "List"
    )
    Iterable<JobDetail> getAllDetails() throws OperationException;

    @GetMapping(
            path = "/{id}/results",
            produces = APPLICATION_JSON_VALUE
    )
    @ApiOperation("Retrieves the results of an asynchronous job")
    Object getResults(final String id) throws OperationException;
}
