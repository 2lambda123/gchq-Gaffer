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

package uk.gov.gchq.gaffer.graph.hook;

import com.google.common.collect.Maps;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewParameterDetail;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.named.operation.cache.exception.CacheOperationFailedException;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.operation.handler.named.cache.NamedViewCache;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.Map;

import static org.junit.Assert.assertTrue;
import static org.mockito.BDDMockito.given;
import static org.mockito.Mockito.mock;

public class NamedViewResolverTest {

    private static final String NAMED_VIEW_NAME = "namedViewName";
    private static final String NESTED_NAMED_VIEW_NAME = "nestedNamedViewName";
    private static final String IS_MORE_THAN_X_PARAM_KEY = "IS_MORE_THAN_X";
    private static final String EDGE_NAME_PARAM_KEY = "EDGE_NAME";
    private static final String VALUE_JSON_STRING = "\"value\":";
    private static final NamedViewCache CACHE = mock(NamedViewCache.class);
    private static final Context CONTEXT = new Context(mock(User.class));
    private static final NamedViewResolver RESOLVER = new NamedViewResolver(CACHE);
    private static final NamedView FULL_NAMED_VIEW = new NamedView.Builder()
            .name(NAMED_VIEW_NAME)
            .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                    .preAggregationFilter(new ElementFilter.Builder()
                            .select(TestPropertyNames.PROP_1)
                            .execute(new ExampleFilterFunction())
                            .build())
                    .build())
            .build();

    @Test
    public void shouldResolveNamedView() throws CacheOperationFailedException, SerialisationException {
        // Given
        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(FULL_NAMED_VIEW);

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .build())
                        .build())
                .build();

        // When
        RESOLVER.preExecute(opChain, CONTEXT);
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertNamedViewAndViewEqual(FULL_NAMED_VIEW, getElements.getView());
    }

    @Test
    public void shouldResolveNamedViewAndMergeAnotherView() throws CacheOperationFailedException, SerialisationException {
        // Given
        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(FULL_NAMED_VIEW);
        final View viewToMerge = new View.Builder().edge(TestGroups.EDGE).build();

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .merge(viewToMerge)
                                .build())
                        .build())
                .build();
        final NamedView namedViewMerged = new NamedView.Builder().merge(FULL_NAMED_VIEW).merge(viewToMerge).build();

        // When
        RESOLVER.preExecute(opChain, CONTEXT);
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertNamedViewAndViewEqual(namedViewMerged, getElements.getView());
    }

    @Test
    public void shouldResolveNamedViewAndMergeAnotherNamedView() throws CacheOperationFailedException, SerialisationException {
        // Given
        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(FULL_NAMED_VIEW);
        final NamedView namedViewToMerge = new NamedView.Builder().name(NAMED_VIEW_NAME + 1).edge(TestGroups.EDGE).build();
        given(CACHE.getNamedView(NAMED_VIEW_NAME + 1)).willReturn(namedViewToMerge);

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .merge(namedViewToMerge)
                                .build())
                        .build())
                .build();
        final NamedView namedViewMerged = new NamedView.Builder().merge(FULL_NAMED_VIEW).merge(namedViewToMerge).build();

        // When
        RESOLVER.preExecute(opChain, CONTEXT);
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertNamedViewAndViewEqual(namedViewMerged, getElements.getView());
    }

    @Test
    public void shouldResolveNestedNamedViews() throws CacheOperationFailedException, SerialisationException {
        // Given
        final NamedView nestedNamedView1 = new NamedView.Builder().name(NESTED_NAMED_VIEW_NAME + 1).entity(TestGroups.ENTITY_2).build();
        final NamedView nestedNamedView = new NamedView.Builder().name(NESTED_NAMED_VIEW_NAME).edge(TestGroups.EDGE).merge(nestedNamedView1).build();
        NamedView namedViewWithNestedNamedView = new NamedView.Builder()
                .name(NAMED_VIEW_NAME)
                .merge(nestedNamedView)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();

        assertTrue(namedViewWithNestedNamedView.getMergedNamedViewNames().size() == 2);
        assertTrue(namedViewWithNestedNamedView.getMergedNamedViewNames().contains(NESTED_NAMED_VIEW_NAME));
        assertTrue(namedViewWithNestedNamedView.getMergedNamedViewNames().contains(NESTED_NAMED_VIEW_NAME + 1));

        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(namedViewWithNestedNamedView);
        given(CACHE.getNamedView(NESTED_NAMED_VIEW_NAME)).willReturn(nestedNamedView);
        given(CACHE.getNamedView(NESTED_NAMED_VIEW_NAME + 1)).willReturn(nestedNamedView1);

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .build())
                        .build())
                .build();

        // When
        RESOLVER.preExecute(opChain, CONTEXT);
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertNamedViewAndViewEqual(namedViewWithNestedNamedView, getElements.getView());
    }

    @Test
    public void shouldResolveNamedViewWithParameter() throws CacheOperationFailedException {
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put(EDGE_NAME_PARAM_KEY, TestGroups.EDGE_2);

        ViewParameterDetail param = new ViewParameterDetail.Builder()
                .defaultValue(TestGroups.EDGE)
                .description("edge name param")
                .valueClass(String.class)
                .build();

        Map<String, ViewParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put(EDGE_NAME_PARAM_KEY, param);

        // Make a real NamedView with a parameter
        final NamedView extendedNamedView = new NamedView.Builder()
                .name(NAMED_VIEW_NAME)
                .edge("${"+EDGE_NAME_PARAM_KEY + "}", new ViewElementDefinition.Builder().preAggregationFilter(new ElementFilter.Builder()
                        .select(IdentifierType.VERTEX.name())
                        .execute(new ExampleFilterFunction())
                        .build()).build())
                .parameters(paramDetailMap)
                .build();

        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(extendedNamedView);

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .build())
                        .build())
                .build();

        // When
        RESOLVER.preExecute(opChain, CONTEXT);
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertTrue(getElements.getView().getEdge(TestGroups.EDGE) != null);

        final OperationChain<?> opChain1 = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .parameterValues(paramMap)
                                .build())
                        .build())
                .build();

        // When
        RESOLVER.preExecute(opChain1, CONTEXT);
        GetElements getElements1 = (GetElements) opChain1.getOperations().get(0);

        // Then
        assertTrue(getElements1.getView().getEdge(TestGroups.EDGE_2) != null);
    }

    @Test
    public void shouldResolveNamedViewWithParametersToMakeCompleteFilter() throws CacheOperationFailedException {
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put(IS_MORE_THAN_X_PARAM_KEY, 7L);

        ViewParameterDetail param = new ViewParameterDetail.Builder()
                .defaultValue(2L)
                .description("more than filter")
                .valueClass(Long.class)
                .build();

        Map<String, ViewParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put(IS_MORE_THAN_X_PARAM_KEY, param);

        // Make a real NamedView with a parameter
        final NamedView extendedNamedView = new NamedView.Builder()
                .name(NAMED_VIEW_NAME)
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder().preAggregationFilter(new ElementFilter.Builder()
                        .select(IdentifierType.VERTEX.name())
                        .execute(new IsMoreThan("${" + IS_MORE_THAN_X_PARAM_KEY + "}"))
                        .build()).build())
                .parameters(paramDetailMap)
                .build();

        given(CACHE.getNamedView(NAMED_VIEW_NAME)).willReturn(extendedNamedView);

        final OperationChain<?> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .build())
                        .build())
                .build();

        // When
        RESOLVER.preExecute(opChain, CONTEXT);
        GetElements getElements = (GetElements) opChain.getOperations().get(0);

        // Then
        assertTrue(new String(getElements.getView().toCompactJson()).contains(VALUE_JSON_STRING + 2));

        final OperationChain<?> opChain1 = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .view(new NamedView.Builder()
                                .name(NAMED_VIEW_NAME)
                                .parameterValues(paramMap)
                                .build())
                        .build())
                .build();

        // When
        RESOLVER.preExecute(opChain1, CONTEXT);
        GetElements getElements1 = (GetElements) opChain1.getOperations().get(0);

        // Then
        assertTrue(new String(getElements1.getView().toCompactJson()).contains(VALUE_JSON_STRING + 7));
    }

    private void assertNamedViewAndViewEqual(NamedView namedView, View view) throws SerialisationException {
        JsonAssert.assertEquals(JSONSerialiser.serialise(namedView.getGlobalElements()), JSONSerialiser.serialise(view.getGlobalElements()));
        JsonAssert.assertEquals(JSONSerialiser.serialise(namedView.getGlobalEntities()), JSONSerialiser.serialise(view.getGlobalEntities()));
        JsonAssert.assertEquals(JSONSerialiser.serialise(namedView.getGlobalEdges()), JSONSerialiser.serialise(view.getGlobalEdges()));
        JsonAssert.assertEquals(JSONSerialiser.serialise(namedView.getEdges()), JSONSerialiser.serialise(view.getEdges()));
        JsonAssert.assertEquals(JSONSerialiser.serialise(namedView.getEntities()), JSONSerialiser.serialise(view.getEntities()));
    }
}
