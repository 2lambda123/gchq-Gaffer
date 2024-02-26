/*
 * Copyright 2022-2024 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.util;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import uk.gov.gchq.gaffer.core.exception.GafferCheckedException;
import uk.gov.gchq.gaffer.core.exception.GafferRuntimeException;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.graph.GraphSerialisable;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.mapstore.MapStoreProperties;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.user.User;
import uk.gov.gchq.koryphe.tuple.predicate.TupleAdaptedPredicate;

import java.io.Closeable;
import java.io.IOException;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Stream;

import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.SCHEMA;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.USER;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.VIEW;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.containsUser;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getSchema;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getView;

/**
 * This class is used to address some of the issues with having the same element groups distributed amongst multiple graphs.
 * Such as the re-application of View filter or Schema Validation after the local aggregation of results from multiple graphs.
 * By default, a local in memory MapStore is used for local aggregation,
 * but a Graph or {@link GraphSerialisable} of any kind could be supplied via the {@link #context} with the key {@link #TEMP_RESULTS_GRAPH}.
 */
public class MergeElementFunction implements ContextSpecificMergeFunction<Object, Iterable<Object>, Iterable<Object>> {
    private static final Logger LOGGER = LoggerFactory.getLogger(MergeElementFunction.class);
    public static final String TEMP_RESULTS_GRAPH = "temporaryResultsGraph";
    private static final Random RANDOM = new Random();

    @JsonProperty("context")
    private Map<String, Object> context;

    public MergeElementFunction() {
    }

    @Override
    public MergeElementFunction createFunctionWithContext(final HashMap<String, Object> context) throws GafferCheckedException {
        final MergeElementFunction mergeElementFunction = new MergeElementFunction();

        try {
            validate(context);

            makeTempResultsGraphIfRequired(context);

            updateViewWithValidationFromSchema(context);

            mergeElementFunction.context = Collections.unmodifiableMap(context);
            return mergeElementFunction;
        } catch (final Exception e) {
            throw new GafferCheckedException("Unable to create TemporaryResultsGraph", e);
        }

    }

    private static void updateViewWithValidationFromSchema(final Map<String, Object> context) {
        //Only do this for MapStore, not required for other stores.
        final Optional<Graph> graph = getGraph(context);
        if (graph.isPresent() && MapStore.class.getName().equals(graph.get().getStoreProperties().getStoreClass())) {
            //Update View with
            final Optional<View> view = getView(context);
            final Schema schema = getSchema(context).orElseThrow(() -> new IllegalStateException("No Schema was found from context, which should have been validated"));
            final View.Builder editableView = new View.Builder(view);

            //getUpdatedDefs and add to new view.
            getUpdatedViewDefsFromSchemaDefs(schema.getEdges(), view)
                    .forEach(e -> editableView.edge(e.getKey(), e.getValue()));
            getUpdatedViewDefsFromSchemaDefs(schema.getEntities(), view)
                    .forEach(e -> editableView.entity(e.getKey(), e.getValue()));

            context.put(VIEW, editableView.build());
        }
    }

    private static void makeTempResultsGraphIfRequired(final HashMap<String, Object> context) {
        // Check if results graph, hasn't already be supplied, otherwise make a default results graph.
        if (!containsTempResultsGraph(context)) {
            final Graph resultsGraph = new Graph.Builder()
                    .config(new GraphConfig(String.format("%s%s%d", TEMP_RESULTS_GRAPH, MergeElementFunction.class.getSimpleName(), RANDOM.nextInt(Integer.MAX_VALUE))))
                    .addSchema((Schema) context.get(SCHEMA))
                    //MapStore easy in memory Store. Large results size may not be suitable, a graph could be provided via Context.
                    .addStoreProperties(new MapStoreProperties())
                    .build();

            LOGGER.debug("A Temporary results graph named:{} is being made with schema:{}", resultsGraph.getGraphId(), resultsGraph.getSchema());

            context.put(TEMP_RESULTS_GRAPH, resultsGraph);
        }
    }

    /**
     * Validates the supplied context to ensure we have everything needed to run the Function
     *
     * @param context The context e.g. view, schema and user
     */
    public static void validate(final Map<String, Object> context) {
        if (!containsUser(context)) {
            throw new IllegalArgumentException("Error: context invalid, requires a User");
        }

        if (!containsTempResultsGraph(context)) {
            if (!containsValidView(context)) {
                throw new UnsupportedOperationException("Error: context invalid: can not use this function with a POST AGGREGATION TRANSFORM VIEW, " +
                        "because transformation may have created items that does not exist in the schema. " +
                        "The re-applying of the View to the collected federated results would not be be possible. " +
                        "Try a simple concat merge that doesn't require the re-application of view");
                // Solution is to derive and use the "Transformed schema" from the uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition.
            }

            if (!containsValidSchema(context)) {
                throw new IllegalArgumentException("Error: context invalid, requires a populated schema.");
            }

        } else if (!containsValidTempResultsGraph(context)) {
            throw new IllegalArgumentException(String.format("Error: context invalid, value for %s was not a Graph, found: %s", TEMP_RESULTS_GRAPH, context.get(TEMP_RESULTS_GRAPH)));
        }
    }

    private static boolean containsValidTempResultsGraph(final Map<String, Object> context) {
        final Object o = context.get(TEMP_RESULTS_GRAPH);
        return o instanceof Graph || o instanceof GraphSerialisable;
    }

    private static boolean containsTempResultsGraph(final Map<String, Object> context) {
        return context.containsKey(TEMP_RESULTS_GRAPH);
    }

    private static boolean containsValidSchema(final Map<String, Object> context) {
        final Optional<Schema> schema = getSchema(context);
        return schema.isPresent() && (schema.get()).hasGroups();
    }

    private static boolean containsValidView(final Map<String, Object> context) {
        final Optional<View> view = getView(context);
        return view.isEmpty() || !view.get().hasTransform();
    }

    private static Stream<Map.Entry<String, ViewElementDefinition>> getUpdatedViewDefsFromSchemaDefs(final Map<String, ? extends SchemaElementDefinition> groupDefs, final Optional<View> view) {
        return groupDefs.entrySet().stream()
                .map(e -> new AbstractMap.SimpleImmutableEntry<>(e.getKey(), getUpdatedViewDefFromSchemaDef(e.getKey(), e.getValue(), view)));
    }

    private static ViewElementDefinition getUpdatedViewDefFromSchemaDef(final String groupName, final SchemaElementDefinition schemaElementDef, final Optional<View> view) {
        final ViewElementDefinition.Builder updatePreAggregationFilter;
        final ArrayList<TupleAdaptedPredicate<String, ?>> updatedFilterFunctions = new ArrayList<>();

        //Add Schema Validation
        if (schemaElementDef.hasValidation()) {
            updatedFilterFunctions.addAll(schemaElementDef.getValidator().getComponents());
        }

        if (view.isPresent()) {
            final ViewElementDefinition viewElementDef = view.get().getElement(groupName);
            //Add View Validation
            if (viewElementDef != null && viewElementDef.hasPostAggregationFilters()) {
                updatedFilterFunctions.addAll(viewElementDef.getPostAggregationFilter().getComponents());
            }
            //Init Builder with contents of the view.
            updatePreAggregationFilter = new ViewElementDefinition.Builder(viewElementDef);
        } else {
            updatePreAggregationFilter = new ViewElementDefinition.Builder();
        }

        //override
        updatePreAggregationFilter.postAggregationFilterFunctions(updatedFilterFunctions);

        return updatePreAggregationFilter.build();
    }

    @Override
    @JsonIgnore
    public Set<String> getRequiredContextValues() {
        return Collections.unmodifiableSet(new HashSet<>(Arrays.asList(VIEW, SCHEMA, USER)));
    }

    @Override
    public Iterable<Object> apply(final Object update, final Iterable<Object> state) {
        if (state instanceof Closeable) {
            Closeable closeable = (Closeable) state;
            try {
                closeable.close();
            } catch (final IOException e) {
                LOGGER.error("Error closing looped iterable", e);
            }
        }

        final Graph resultsGraph;
        final Context userContext = new Context((User) context.get(USER));

        try {
            resultsGraph = getGraph(context).orElseThrow(() -> new GafferCheckedException("No results Graph available for merging "));
            // The update object might be a lazy AccumuloElementRetriever and might be MASSIVE.
            resultsGraph.execute(new AddElements.Builder().input((Iterable<Element>) update).build(), userContext);
        } catch (final GafferCheckedException e) {
            throw new GafferRuntimeException("Error adding elements to temporary results graph, due to:" + e.getMessage(), e);
        }

        try {
            return (Iterable) resultsGraph.execute(new GetAllElements.Builder().view((View) context.get(VIEW)).build(), userContext);
        } catch (final OperationException e) {
            throw new GafferRuntimeException("Error getting all elements from temporary graph, due to:" + e.getMessage(), e);
        }
    }

    private static Optional<Graph> getGraph(final Map<String, Object> context) {
        final Object o = context.get(TEMP_RESULTS_GRAPH);
        final Optional<Graph> resultsGraph;
        if (o instanceof GraphSerialisable) {
            resultsGraph = Optional.of(((GraphSerialisable) o).getGraph());
        } else if (o instanceof Graph) {
            resultsGraph = Optional.of((Graph) o);
        } else {
            resultsGraph = Optional.empty();
        }
        return resultsGraph;
    }
}
