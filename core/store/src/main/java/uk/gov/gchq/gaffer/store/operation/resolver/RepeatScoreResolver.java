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
package uk.gov.gchq.gaffer.store.operation.resolver;

import uk.gov.gchq.gaffer.operation.impl.Repeat;

/**
 * A {@code RepeatScoreResolver} is an implementation of {@link ScoreResolver} for
 * the {@link Repeat} operation.
 * <p>The score will simply be the number of repeats
 * multiplied by the score of the delegate operation,
 * or if the delegate is an implementation of {@link uk.gov.gchq.gaffer.operation.Operations},
 * the sum of the scores of the operations contained within.</p>
 */
public class RepeatScoreResolver implements ScoreResolver<Repeat> {
    @Override
    public Integer getScore(final Repeat repeat, final ScoreResolver defaultScoreResolver) {
        if (null == repeat || null == repeat.getOperation()) {
            return 0;
        }

        return repeat.getTimes() * defaultScoreResolver.getScore(repeat.getOperation());
    }

    @Override
    public Integer getScore(final Repeat operation) {
        throw new UnsupportedOperationException("Default score resolver has not been provided.");
    }
}
