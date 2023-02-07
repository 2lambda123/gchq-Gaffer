/*
 * Copyright 2016-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.jobtracker;

import uk.gov.gchq.gaffer.cache.Cache;
import uk.gov.gchq.gaffer.cache.exception.CacheOperationException;
import uk.gov.gchq.gaffer.user.User;

import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.nonNull;

/**
 * A {@code JobTracker} is an entry in a Gaffer cache service which is used to store
 * details of jobs submitted to the graph.
 */
public class JobTracker extends Cache<String, JobDetail> {

    public static final String CACHE_SERVICE_NAME_PREFIX = "JobTracker";

    public JobTracker() {
        this(null);
    }

    public JobTracker(final String cacheNameSuffix) {
        super(String.format("%s%s", CACHE_SERVICE_NAME_PREFIX,
                nonNull(cacheNameSuffix)
                        ? "_" + cacheNameSuffix.toLowerCase()
                        : ""));
    }

    /**
     * Add or update the job details relating to a job in the job tracker cache.
     *
     * @param jobDetail the job details to update
     * @param user      the user making the request
     */
    public void addOrUpdateJob(final JobDetail jobDetail, final User user) {
        validateJobDetail(jobDetail);
        try {
            super.addToCache(jobDetail.getJobId(), jobDetail, true);
        } catch (final CacheOperationException e) {
            throw new RuntimeException("Failed to add jobDetail " + jobDetail.toString() + " to the cache", e);
        }
    }

    /**
     * Get the details of a specific job.
     *
     * @param jobId the ID of the job to lookup
     * @param user  the user making the request to the job tracker
     * @return the {@link JobDetail} object for the requested job
     */
    public JobDetail getJob(final String jobId, final User user) {
        try {
            return super.getFromCache(jobId);
        } catch (final CacheOperationException e) {
            throw new RuntimeException(e);
        }
    }

    /**
     * Get all jobs from the job tracker cache.
     *
     * @param user the user making the request to the job tracker
     * @return a {@link Iterable} containing all of the job details
     */
    public Iterable<JobDetail> getAllJobs(final User user) {

        return getAllJobsMatching(user, jd -> true);
    }

    /**
     * Get all scheduled jobs from the job tracker cache.
     *
     * @return a {@link Iterable} containing all of the scheduled job details
     */
    public Iterable<JobDetail> getAllScheduledJobs() {

        return getAllJobsMatching(new User(), jd -> jd.getStatus().equals(JobStatus.SCHEDULED_PARENT));
    }

    private Iterable<JobDetail> getAllJobsMatching(final User user, final Predicate<JobDetail> jobDetailPredicate) {

        final Set<String> jobIds = getAllKeys();
        final List<JobDetail> jobs = jobIds.stream()
                .filter(Objects::nonNull)
                .map(jobId -> getJob(jobId, user))
                .filter(Objects::nonNull)
                .filter(jobDetailPredicate)
                .collect(Collectors.toList());

        return jobs;
    }


    private void validateJobDetail(final JobDetail jobDetail) {
        if (null == jobDetail) {
            throw new IllegalArgumentException("JobDetail is required");
        }

        if (null == jobDetail.getJobId() || jobDetail.getJobId().isEmpty()) {
            throw new IllegalArgumentException("jobId is required");
        }
    }
}
