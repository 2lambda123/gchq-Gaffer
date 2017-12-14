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

package uk.gov.gchq.gaffer.data.elementdefinition.view;

import com.google.common.collect.Maps;
import org.junit.Test;

import uk.gov.gchq.gaffer.commonutil.JsonAssert;
import uk.gov.gchq.gaffer.commonutil.StringUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.IdentifierType;
import uk.gov.gchq.gaffer.data.element.function.ElementFilter;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.function.ExampleFilterFunction;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class NamedViewTest {
    private static final String TEST_VIEW_NAME = "testViewName";
    private static final String TEST_PARAM_KEY = "testParamKey";
    private static final ViewParameterDetail TEST_PARAM = new ViewParameterDetail.Builder()
            .defaultValue(1L)
            .description("Limit param")
            .valueClass(Long.class)
            .build();
    private final Map<String, ViewParameterDetail> testParameters = new HashMap<>();
    private final ViewElementDefinition edgeDef1 = new ViewElementDefinition();
    private final ViewElementDefinition entityDef1 = new ViewElementDefinition();
    private final ViewElementDefinition edgeDef2 = new ViewElementDefinition.Builder().groupBy(TestGroups.EDGE).build();
    private final ViewElementDefinition entityDef2 = new ViewElementDefinition.Builder().groupBy(TestGroups.ENTITY).build();

    @Test
    public void shouldCreateEmptyNamedViewWithBasicConstructor() {
        // When
        NamedView namedView = new NamedView();

        // Then
        assertTrue(namedView.getName().isEmpty());
        assertTrue(namedView.getParameters().isEmpty());
        assertTrue(namedView.getEdges().isEmpty());
        assertTrue(namedView.getEntities().isEmpty());
    }

    @Test
    public void shouldThrowExceptionWithNoName() {
        try {
            new NamedView.Builder().edge(TestGroups.EDGE).build();
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Name must be set"));
        }
    }

    @Test
    public void shouldCreateNewNamedViewWithEdgesAndEntities() {
        // Given
        List<String> entityGroups = new ArrayList<>();
        List<String> edgeGroups = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            entityGroups.add(TestGroups.ENTITY + i);
            edgeGroups.add(TestGroups.EDGE + i);
        }

        // When
        NamedView namedView = new NamedView.Builder()
                .name(TEST_VIEW_NAME)
                .entities(entityGroups)
                .edges(edgeGroups)
                .build();

        // Then
        assertTrue(namedView.getEntityGroups().containsAll(entityGroups));
        assertEquals(entityGroups.size(), namedView.getEntityGroups().size());
        assertTrue(namedView.getEdgeGroups().containsAll(edgeGroups));
        assertEquals(edgeGroups.size(), namedView.getEdgeGroups().size());
    }

    @Test
    public void shouldBuildFullNamedView() {
        // Given
        testParameters.put(TEST_PARAM_KEY, TEST_PARAM);

        // When
        NamedView namedView = new NamedView.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .name(TEST_VIEW_NAME)
                .parameters(testParameters)
                .build();

        // Then
        assertEquals(1, namedView.getEdges().size());
        assertSame(edgeDef1, namedView.getEdge(TestGroups.EDGE));


        assertEquals(1, namedView.getEntities().size());
        assertSame(entityDef1, namedView.getEntity(TestGroups.ENTITY));

        assertEquals(TEST_VIEW_NAME, namedView.getName());
        assertEquals(testParameters, namedView.getParameters());
    }

    @Test
    public void shouldSerialiseToJson() {
        // Given
        final NamedView namedView = new NamedView.Builder()
                .name(TEST_VIEW_NAME)
                .entity(TestGroups.ENTITY, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(TestPropertyNames.PROP_1)
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .build();

        // When
        byte[] json = namedView.toJson(true);

        // Then
        JsonAssert.assertEquals(String.format("{%n" +
                "  \"class\" : \"uk.gov.gchq.gaffer.data.elementdefinition.view.NamedView\"," +
                "  \"edges\" : { }," +
                "  \"entities\" : {\n" +
                "    \"BasicEntity\" : {\n" +
                "       \"preAggregationFilterFunctions\" : [ {\n" +
                "          \"predicate\" : {\n" +
                "             \"class\" : \"uk.gov.gchq.gaffer.function.ExampleFilterFunction\"" +
                "           }," +
                "           \"selection\" : [ \"property1\" ]" +
                "          } ]" +
                "        }" +
                "      }," +
                "      \"name\": \"testViewName\"," +
                "      \"parameters\" : { }" +
                "    }"), new String(json));
    }

    @Test
    public void shouldJsonSerialiseAndDeserialise() {
        // Given
        testParameters.put(TEST_PARAM_KEY, TEST_PARAM);
        NamedView namedView = new NamedView.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .name(TEST_VIEW_NAME)
                .parameters(testParameters)
                .build();

        // When
        byte[] json = namedView.toJson(true);
        final NamedView deserialisedView = new NamedView.Builder().json(json).build();

        // Then
        assertEquals(TEST_VIEW_NAME, deserialisedView.getName());
        assertEquals(testParameters, namedView.getParameters());
        assertEquals(1, namedView.getEdges().size());
        assertSame(edgeDef1, namedView.getEdge(TestGroups.EDGE));
        assertEquals(1, namedView.getEntities().size());
        assertSame(entityDef1, namedView.getEntity(TestGroups.ENTITY));
    }

    @Test
    public void shouldMergeNamedViews() {
        // Given / When
        testParameters.put(TEST_PARAM_KEY, TEST_PARAM);

        NamedView namedView = new NamedView.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .name(TEST_VIEW_NAME)
                .parameters(testParameters)
                .build();

        NamedView namedView2 = new NamedView.Builder()
                .edge(TestGroups.EDGE, edgeDef2)
                .entity(TestGroups.ENTITY, entityDef2)
                .name(TEST_VIEW_NAME + 2)
                .parameters(new HashMap<>())
                .merge(namedView)
                .build();

        // Then
        assertEquals(TEST_VIEW_NAME + 2, namedView2.getName());
        assertEquals(1, namedView2.getMergedNamedViewNames().size());
        assertTrue(namedView2.getMergedNamedViewNames().contains(TEST_VIEW_NAME));
        assertEquals(testParameters, namedView2.getParameters());
    }

    @Test
    public void shouldMergeEmptyNamedViewWithPopulatedNamedView() {
        // Given / When
        testParameters.put(TEST_PARAM_KEY, TEST_PARAM);

        NamedView namedView = new NamedView.Builder()
                .edge(TestGroups.EDGE, edgeDef1)
                .entity(TestGroups.ENTITY, entityDef1)
                .name(TEST_VIEW_NAME)
                .parameters(testParameters)
                .merge(new NamedView())
                .build();

        // Then
        assertEquals(TEST_VIEW_NAME, namedView.getName());
        assertEquals(0, namedView.getMergedNamedViewNames().size());
        assertEquals(testParameters, namedView.getParameters());
    }

    @Test
    public void shouldMultipleMergeNamedViewsCorrectly() {
        // Given
        List<String> entityGroups = new ArrayList<>();
        List<String> edgeGroups = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            entityGroups.add(TestGroups.ENTITY + i);
            edgeGroups.add(TestGroups.EDGE + i);
        }

        testParameters.put(TEST_PARAM_KEY, TEST_PARAM);

        // When
        NamedView namedView1 = new NamedView.Builder()
                .edges(edgeGroups)
                .name(TEST_VIEW_NAME + 1)
                .parameters(testParameters)
                .merge(new NamedView())
                .build();

        NamedView namedView2 = new NamedView.Builder()
                .entities(entityGroups)
                .name(TEST_VIEW_NAME + 2)
                .parameters(testParameters)
                .merge(namedView1)
                .build();

        NamedView namedView3 = new NamedView.Builder()
                .name(TEST_VIEW_NAME + 3)
                .parameters(testParameters)
                .merge(namedView2)
                .build();

        // Then
        assertEquals(TEST_VIEW_NAME + 3, namedView3.getName());
        assertEquals(2, namedView3.getMergedNamedViewNames().size());
        assertEquals(Arrays.asList(TEST_VIEW_NAME + 2, TEST_VIEW_NAME + 1), namedView3.getMergedNamedViewNames());
        assertEquals(testParameters, namedView3.getParameters());
        assertTrue(namedView3.getEntityGroups().containsAll(entityGroups));
        assertEquals(entityGroups.size(), namedView3.getEntityGroups().size());
        assertTrue(namedView3.getEdgeGroups().containsAll(edgeGroups));
        assertEquals(edgeGroups.size(), namedView3.getEdgeGroups().size());
    }

    @Test
    public void shouldMergeViewToNamedViewsCorrectly() {
        // Given
        List<String> entityGroups = new ArrayList<>();
        List<String> edgeGroups = new ArrayList<>();

        for (int i = 0; i < 4; i++) {
            entityGroups.add(TestGroups.ENTITY + i);
            edgeGroups.add(TestGroups.EDGE + i);
        }

        testParameters.put(TEST_PARAM_KEY, TEST_PARAM);

        // When
        View view = new View.Builder()
                .entities(entityGroups)
                .build();

        NamedView namedView = new NamedView.Builder()
                .name(TEST_VIEW_NAME + 3)
                .edges(edgeGroups)
                .parameters(testParameters)
                .merge(view)
                .build();

        // Then
        assertEquals(TEST_VIEW_NAME + 3, namedView.getName());
        assertEquals(0, namedView.getMergedNamedViewNames().size());
        assertEquals(testParameters, namedView.getParameters());
        assertTrue(namedView.getEntityGroups().containsAll(entityGroups));
        assertEquals(entityGroups.size(), namedView.getEntityGroups().size());
        assertTrue(namedView.getEdgeGroups().containsAll(edgeGroups));
        assertEquals(edgeGroups.size(), namedView.getEdgeGroups().size());
    }

    @Test
    public void shouldDefaultDeserialiseToView() throws SerialisationException {
        final byte[] emptyJson = StringUtil.toBytes("{}");
        View view = JSONSerialiser.deserialise(emptyJson, View.class);
        assertEquals(View.class, view.getClass());
    }

    @Test
    public void showAllowMergingOfNamedViewIntoAViewWhenNameIsEmpty() {
        //When / Then
        try {
            new View.Builder().merge(new NamedView()).build();
        } catch (final IllegalArgumentException e) {
            fail("Exception not expected");
        }
    }

    @Test
    public void shouldThrowExceptionWhenMergingNamedViewIntoAViewWhenNameIsSet() {
        // Given
        final NamedView namedView = new NamedView.Builder().name(TEST_VIEW_NAME).build();
        //When / Then
        try {
            new View.Builder().merge(namedView).build();
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("A NamedView cannot be merged into a View"));
        }
    }

    @Test
    public void shouldNotAddNameToMergedNamedViewsListIfNameIsTheSameAsTheNamedViewName() {
        final String namedViewName = "namedViewName";

        final NamedView namedViewToMerge = new NamedView.Builder()
                .name(namedViewName)
                .edge(TestGroups.EDGE)
                .build();

        final NamedView namedViewMerged = new NamedView.Builder()
                .name(namedViewName)
                .merge(namedViewToMerge)
                .build();

        assertTrue(namedViewMerged.getMergedNamedViewNames().isEmpty());
        JsonAssert.assertEquals(namedViewToMerge.toCompactJson(), namedViewMerged.toCompactJson());
    }

    @Test
    public void shouldGetNamedViewWithResolvedParameters() {
        // Given
        ViewParameterDetail viewParameterDetail = new ViewParameterDetail.Builder()
                .defaultValue(TestGroups.EDGE_2)
                .description("edge name param")
                .valueClass(String.class)
                .build();

        Map<String, ViewParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("EDGE_NAME", viewParameterDetail);

        // When - full NamedView created with params
        final NamedView extendedNamedView = new NamedView.Builder()
                .name(TEST_VIEW_NAME)
                .edge("${EDGE_NAME}", new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select(IdentifierType.VERTEX.name())
                                .execute(new ExampleFilterFunction())
                                .build())
                        .build())
                .parameters(paramDetailMap)
                .build();

        // Then - assert NamedView contains the param name and not resolved param
        assertNotNull(extendedNamedView.getElement("${EDGE_NAME}"));
        assertNull(extendedNamedView.getElement(TestGroups.EDGE));

        // When - resolve it with default params
        NamedView namedViewWithResolvedDefaultParams = extendedNamedView.getNamedView();

        // Then - assert resolved NamedView contains resolved default param and not the param name
        assertNull(namedViewWithResolvedDefaultParams.getElement("${EDGE_NAME}"));
        assertTrue(new String(namedViewWithResolvedDefaultParams.toCompactJson()).contains(TestGroups.EDGE_2));

        // When - resolve it with specified params
        Map<String, Object> paramValueMap = Maps.newHashMap();
        paramValueMap.put("EDGE_NAME", TestGroups.EDGE);
        extendedNamedView.setParameterValues(paramValueMap);
        NamedView namedViewWithResolvedParams = extendedNamedView.getNamedView();

        // Then - assert resolved NamedView contains resolved param and not the param name
        assertNotNull(namedViewWithResolvedParams.getElement(TestGroups.EDGE));
        assertNull(namedViewWithResolvedParams.getElement("${EDGE_NAME}"));
    }

    @Test
    public void shouldGetNamedViewWithResolvedParametersToMakeCompleteFilter() {
        // Given
        ViewParameterDetail param = new ViewParameterDetail.Builder()
                .defaultValue(2L)
                .description("more than filter")
                .valueClass(Long.class)
                .build();

        Map<String, ViewParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("IS_MORE_THAN_X", param);

        // When - full NamedView created with params
        final NamedView extendedNamedView = new NamedView.Builder()
                .name(TEST_VIEW_NAME)
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("count")
                                .execute(new IsMoreThan("${IS_MORE_THAN_X}"))
                                .build())
                        .build())
                .parameters(paramDetailMap)
                .build();

        // Then - assert NamedView contains the param name and not resolved param
        assertTrue(new String(extendedNamedView.toCompactJson()).contains("\"value\":\"${IS_MORE_THAN_X}\""));

        // When - resolve it with default params
        NamedView namedViewWithResolvedDefaultParams = extendedNamedView.getNamedView();

        // Then - assert resolved NamedView contains resolved default param and not the param name
        assertFalse(new String(namedViewWithResolvedDefaultParams.toCompactJson()).contains("${IS_MORE_THAN_X"));
        assertTrue(new String(namedViewWithResolvedDefaultParams.toCompactJson()).contains("\"value\":2"));

        // When - resolve it with specified params
        Map<String, Object> paramMap = Maps.newHashMap();
        paramMap.put("IS_MORE_THAN_X", 7L);
        extendedNamedView.setParameterValues(paramMap);
        NamedView namedViewWithResolvedParams = extendedNamedView.getNamedView();

        // Then - assert resolved NamedView contains resolved param and not the param name
        assertFalse(new String(namedViewWithResolvedParams.toCompactJson()).contains("${IS_MORE_THAN_X"));
        assertTrue(new String(namedViewWithResolvedParams.toCompactJson()).contains("\"value\":7"));
    }

    @Test
    public void shouldThrowExceptionWithNoParamsValueSetAndNoDefault() {
        ViewParameterDetail param = new ViewParameterDetail.Builder()
                .description("more than filter")
                .valueClass(Long.class)
                .build();

        Map<String, ViewParameterDetail> paramDetailMap = Maps.newHashMap();
        paramDetailMap.put("IS_MORE_THAN_X", param);

        // When - full NamedView created with params
        final NamedView extendedNamedView = new NamedView.Builder()
                .name(TEST_VIEW_NAME)
                .edge(TestGroups.EDGE, new ViewElementDefinition.Builder()
                        .preAggregationFilter(new ElementFilter.Builder()
                                .select("count")
                                .execute(new IsMoreThan("${IS_MORE_THAN_X}"))
                                .build())
                        .build())
                .parameters(paramDetailMap)
                .build();

        try {
            extendedNamedView.getNamedView();
            fail("Exception expected");
        } catch (final IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("Missing parameter IS_MORE_THAN_X with no default"));
        }
    }
}
