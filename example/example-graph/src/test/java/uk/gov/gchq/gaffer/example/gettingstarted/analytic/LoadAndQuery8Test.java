/*
 * Copyright 2016 Crown Copyright
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

package uk.gov.gchq.gaffer.example.gettingstarted.analytic;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.google.common.collect.Lists;
import org.hamcrest.core.IsCollectionContaining;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.commonutil.iterable.CloseableIterable;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.get.GetAllElements;
import uk.gov.gchq.gaffer.user.User;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

public class LoadAndQuery8Test {
    private static final Date JAN_01_16_END;

    static {
        try {
            JAN_01_16_END = new SimpleDateFormat("dd/MM/yy/hh/mm/ss/SSS").parse("01/01/16/23/59/59/999");
        } catch (final ParseException e) {
            throw new IllegalArgumentException("Unable to parse date", e);
        }
    }

    private static final String RESOURCE_PREFIX = "/example/gettingstarted/";
    private static final String RESOURCE_EXAMPLE_PREFIX = RESOURCE_PREFIX + "8/";
    public static final String COUNT = "count";
    public static final String VISIBILITY = "visibility";
    public static final String START_DATE = "startDate";
    public static final String END_DATE = "endDate";
    public static final String EDGE_GROUP = "data";

    @Test
    public void shouldReturnExpectedEdges() throws OperationException {
        // Given
        final LoadAndQuery8 query = new LoadAndQuery8();

        // When
        final CloseableIterable<? extends Element> results = query.run();

        // Then
        verifyResults(results);
    }

    @Test
    public void shouldReturnExpectedStringsViaJson() throws OperationException, SerialisationException {
        // Given
        final User publicUser = new User.Builder()
                .userId("public user")
                .dataAuths("public")
                .build();

        final JSONSerialiser serialiser = new JSONSerialiser();
        final OperationChain<?> addOpChain = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/load.json"), OperationChain.class);
        final GetAllElements getAllEdges = serialiser.deserialise(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "json/query.json"), GetAllElements.class);

        // Setup graph
        final Graph graph = new Graph.Builder()
                .storeProperties(StreamUtil.openStream(LoadAndQuery.class, RESOURCE_PREFIX + "mockaccumulostore.properties"))
                .addSchemas(StreamUtil.openStreams(LoadAndQuery.class, RESOURCE_EXAMPLE_PREFIX + "schema"))
                .build();

        // When
        graph.execute(addOpChain, publicUser); // Execute the add operation chain on the graph
        final CloseableIterable<? extends Element> results = graph.execute(getAllEdges, publicUser); // Execute the query operation on the graph.

        // Then
        verifyResults(results);
    }

    private void verifyResults(final CloseableIterable<? extends Element> resultsItr) {
        final Edge[] expectedResults = {
                new Edge.Builder()
                        .source("1")
                        .destination("2")
                        .directed(true)
                        .group(EDGE_GROUP)
                        .property(COUNT, 4L)
                        .property(VISIBILITY, "public")
                        .property(START_DATE, LoadAndQuery8.JAN_01_16)
                        .property(END_DATE, JAN_01_16_END)
                        .build(),
                new Edge.Builder()
                        .source("1")
                        .destination("3")
                        .directed(true)
                        .group(EDGE_GROUP)
                        .property(COUNT, 2L)
                        .property(VISIBILITY, "public")
                        .property(START_DATE, LoadAndQuery8.JAN_01_16)
                        .property(END_DATE, JAN_01_16_END)
                        .build()
        };

        final List<Element> results = Lists.newArrayList(resultsItr);
        assertEquals(expectedResults.length, results.size());
        assertThat(results, IsCollectionContaining.hasItems(expectedResults));
    }
}