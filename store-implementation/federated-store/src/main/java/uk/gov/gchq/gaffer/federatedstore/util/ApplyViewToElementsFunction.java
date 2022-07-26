/*
 * Copyright 2022 Crown Copyright
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

import org.apache.commons.lang3.builder.HashCodeBuilder;

import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.elementdefinition.view.View;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.util.HashMap;
import java.util.Iterator;
import java.util.function.BiFunction;

import static java.util.Objects.nonNull;
import static uk.gov.gchq.gaffer.mapstore.impl.GetElementsUtil.applyView;

public class ApplyViewToElementsFunction implements BiFunction<Object, Iterable<Object>, Iterable<Object>>, ContextSpecificMergeFunction<Object, Iterable<Object>, Iterable<Object>> {

    public static final String MISSING_S = "Context is not complete for %s missing: %s";
    HashMap<String, Object> context;

    public ApplyViewToElementsFunction() {
        this(null);
    }

    public ApplyViewToElementsFunction(final HashMap<String, Object> context) {
        this.context = context;
    }

    @Override
    public ApplyViewToElementsFunction createFunctionWithContext(final HashMap<String, Object> context) {

        final View view = (View) context.get("view");
        if (view == null) {
            throw new IllegalArgumentException(String.format(MISSING_S, ApplyViewToElementsFunction.class.getCanonicalName(), view));
        } else if (view.hasTransform()) {
            throw new UnsupportedOperationException("Error: can not use the default merge function with a POST AGGREGATION TRANSFORM VIEW, " +
                    "because transformation may have create items that does not exist in schema. " +
                    "The re-applying of the View to collected federated results would not be be possible. " +
                    "Try a simple concat merge that doesn't require the re-application of view");
            //Solution is to derive and use the "Transformed schema" from the uk.gov.gchq.gaffer.data.elementdefinition.view.ViewElementDefinition.
        }
        return new ApplyViewToElementsFunction(context);
    }

    @Override
    public Iterable<Object> apply(final Object first, final Iterable<Object> next) {

        final Iterable<Object> concatResults = new DefaultBestEffortsMergeFunction().apply(first, next);

        final HashMap<String, Object> clone = (HashMap<String, Object>) context.clone();
        clone.put("concatResults", concatResults);

        return new ViewFilteredIterable(clone);
    }

    @Override
    public int hashCode() {
        return new HashCodeBuilder(13, 47)
                .append(super.hashCode())
                .toHashCode();
    }

    @Override
    public boolean equals(final Object obj) {
        return nonNull(obj) && this.getClass().equals(obj.getClass());
    }

    private static class ViewFilteredIterable implements Iterable<Object> {
        Iterable<Object> concatResults;
        View view;
        Schema schema;

        ViewFilteredIterable(final HashMap context) {
            this.view = (View) context.get("view");
            this.schema = (Schema) context.get("schema");
            this.concatResults = (Iterable<Object>) context.get("concatResults");
            if (view == null || schema == null || concatResults == null) {
                throw new IllegalArgumentException(String.format(MISSING_S, ApplyViewToElementsFunction.class.getCanonicalName(), view == null ? "view" : schema == null ? "schema" : "concatResults"));
            }
        }

        @Override
        public Iterator<Object> iterator() {
            //TODO FS test.
            return applyView(Streams.toStream(concatResults).map(o -> (Element) o), schema, view).map(o -> (Object) o).iterator();
        }
    }
}
