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

package uk.gov.gchq.gaffer.store.operation.handler.named.cache;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import uk.gov.gchq.gaffer.cache.CacheServiceLoader;
import uk.gov.gchq.gaffer.cache.impl.HashMapCacheService;
import uk.gov.gchq.gaffer.cache.util.CacheProperties;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.exception.OverwritingException;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedViewDetail;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;

import java.util.Properties;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NamedViewCacheTest {
    private static NamedViewCache cache;
    private static final String EXCEPTION_EXPECTED = "Exception expected";
    private static final String STANDARD_VIEW_NAME = "standardView";
    private static final String ALTERNATIVE_VIEW_NAME = "alternativeView";
    private View standardView = new View.Builder().build();
    private View alternativeView = new View.Builder().edge(TestGroups.EDGE).build();

    private NamedViewDetail standard = new NamedViewDetail.Builder()
            .name(STANDARD_VIEW_NAME)
            .description("standard View")
            .view(standardView)
            .build();

    private final NamedViewDetail standardNamedViewAsDetail = new NamedViewDetail.Builder().name(STANDARD_VIEW_NAME).view(standardView).description("standard View").build();

    private NamedViewDetail alternative = new NamedViewDetail.Builder()
            .name(ALTERNATIVE_VIEW_NAME)
            .description("alternative View")
            .view(alternativeView)
            .build();

    private final NamedViewDetail alternativeNamedViewAsDetail = new NamedViewDetail.Builder().name(ALTERNATIVE_VIEW_NAME).view(alternativeView).description("alternative View").build();

    @BeforeClass
    public static void setUp() {
        Properties properties = new Properties();
        properties.setProperty(CacheProperties.CACHE_SERVICE_CLASS, HashMapCacheService.class.getName());
        CacheServiceLoader.initialise(properties);
        cache = new NamedViewCache();
    }

    @Before
    public void beforeEach() throws CacheOperationFailedException {
        cache.clearCache();
    }

    @Test
    public void shouldAddNamedView() throws CacheOperationFailedException {
        cache.addNamedView(standard, false);
        NamedViewDetail namedViewFromCache = cache.getNamedView(standard.getName());

        assertEquals(standardNamedViewAsDetail, namedViewFromCache);
    }

    @Test
    public void shouldThrowExceptionIfNamedViewAlreadyExists() throws CacheOperationFailedException {
        cache.addNamedView(standard, false);
        try {
            cache.addNamedView(standard, false);
            fail(EXCEPTION_EXPECTED);
        } catch (OverwritingException e) {
            assertTrue(e.getMessage().equals("Cache entry already exists for key: " + STANDARD_VIEW_NAME));
        }
    }

    @Test
    public void shouldThrowExceptionWhenDeletingIfKeyIsNull() throws CacheOperationFailedException {
        try {
            cache.deleteNamedView(null);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("NamedView name cannot be null"));
        }
    }

    @Test
    public void shouldThrowExceptionWhenGettingIfKeyIsNull() throws CacheOperationFailedException {
        try {
            cache.getNamedView(null);
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("NamedView name cannot be null"));
        }
    }

    @Test
    public void shouldReturnEmptySetIfThereAreNoOperationsInTheCache() throws CacheOperationFailedException {
        CloseableIterable<NamedViewDetail> views = cache.getAllNamedViews();
        assertEquals(0, Iterables.size(views));
    }

    @Test
    public void shouldBeAbleToReturnAllNamedViewsFromCache() throws CacheOperationFailedException {
        cache.addNamedView(standard, false);
        cache.addNamedView(alternative, false);

        Set<NamedViewDetail> allViews = Sets.newHashSet(cache.getAllNamedViews());

        assertTrue(allViews.contains(standardNamedViewAsDetail));
        assertTrue(allViews.contains(alternativeNamedViewAsDetail));
        assertEquals(2, allViews.size());
    }
}
