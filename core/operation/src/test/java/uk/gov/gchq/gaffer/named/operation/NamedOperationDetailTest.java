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

package uk.gov.gchq.gaffer.named.operation;

import org.junit.jupiter.api.Test;

import uk.gov.gchq.gaffer.access.ResourceType;
import uk.gov.gchq.gaffer.access.predicate.CustomAccessPredicate;
import uk.gov.gchq.gaffer.access.predicate.DefaultAccessPredicate;
import uk.gov.gchq.gaffer.user.User;

import java.util.Collections;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static java.util.Collections.singletonMap;
import static org.junit.jupiter.api.Assertions.assertEquals;

public class NamedOperationDetailTest {

    @Test
    public void shouldBeNamedOperationResourceType() {
        assertEquals(ResourceType.NamedOperation, new NamedOperationDetail().getResourceType());
    }

    @Test
    public void shouldDefaultReadAccessPredicateIfNoneSpecified() {
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder().build();
        assertEquals(
                new DefaultAccessPredicate(new User.Builder().userId("creatorUserId").build(), asList("readerAuth1", "readerAuth2")),
                namedOperationDetail.getReadAccessPredicate());
        assertEquals(
                asList("readerAuth1", "readerAuth2", "writerAuth1", "writerAuth2"),
                namedOperationDetail.getAuths());
    }

    @Test
    public void shouldDefaultWriteAccessPredicateIfNoneSpecified() {
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder().build();
        assertEquals(
                new DefaultAccessPredicate(new User.Builder().userId("creatorUserId").build(), asList("writerAuth1", "writerAuth2")),
                namedOperationDetail.getWriteAccessPredicate());
        assertEquals(
                asList("readerAuth1", "readerAuth2", "writerAuth1", "writerAuth2"),
                namedOperationDetail.getAuths());
    }

    @Test
    public void shouldConfigureCustomReadAccessPredicateWhenSpecified() {
        final CustomAccessPredicate customAccessPredicate = new CustomAccessPredicate("userId", singletonMap("key", "value"), asList("customAuth1", "customAuth2"));
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .readAccessPredicate(customAccessPredicate)
                .build();
        assertEquals(customAccessPredicate, namedOperationDetail.getReadAccessPredicate());
        assertEquals(
                asList("customAuth1", "customAuth2", "writerAuth1", "writerAuth2"),
                namedOperationDetail.getAuths());
    }

    @Test
    public void shouldConfigureCustomWriteAccessPredicateWhenSpecified() {
        final CustomAccessPredicate customAccessPredicate = new CustomAccessPredicate("userId", singletonMap("key", "value"), asList("customAuth1", "customAuth2"));
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .writeAccessPredicate(customAccessPredicate)
                .build();
        assertEquals(customAccessPredicate, namedOperationDetail.getWriteAccessPredicate());
        assertEquals(
                asList("customAuth1", "customAuth2", "readerAuth1", "readerAuth2"),
                namedOperationDetail.getAuths());
    }

    @Test
    public void shouldReturnSortedAndDeduplicatedAuthListBuiltFromPredicates() {
        final NamedOperationDetail namedOperationDetail = getBaseNamedOperationDetailBuilder()
                .readAccessPredicate(new CustomAccessPredicate("userId", emptyMap(), asList("z1", "x2")))
                .writeAccessPredicate(new CustomAccessPredicate("userId", emptyMap(), asList("b2", "a1", "z1", "x2", "b1")))
                .build();
        assertEquals(
                asList("a1", "b1", "b2", "x2", "z1"),
                namedOperationDetail.getAuths());
    }

    private NamedOperationDetail.Builder getBaseNamedOperationDetailBuilder() {
        return new NamedOperationDetail.Builder()
                .operationName("operationName")
                .labels(asList("label1", "label2"))
                .inputType("inputType")
                .description("description")
                .creatorId("creatorUserId")
                .readers(asList("readerAuth1", "readerAuth2"))
                .writers(asList("writerAuth1", "writerAuth2"))
                .parameters(Collections.emptyMap())
                .operationChain("{\"operations\":[{\"class\":\"uk.gov.gchq.gaffer.store.operation.GetSchema\",\"compact\":false}]}")
                .score(1);
    }
}
