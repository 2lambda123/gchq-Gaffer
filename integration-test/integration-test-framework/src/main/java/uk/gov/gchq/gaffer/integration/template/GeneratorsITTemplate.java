/*
 * Copyright 2016-2020 Crown Copyright
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
package uk.gov.gchq.gaffer.integration.template;

import com.google.common.collect.Lists;

import uk.gov.gchq.gaffer.commonutil.CollectionUtil;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.util.ElementUtil;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.integration.AbstractStoreIT;
import uk.gov.gchq.gaffer.integration.GafferTest;
import uk.gov.gchq.gaffer.integration.domain.DomainObject;
import uk.gov.gchq.gaffer.integration.domain.EdgeDomainObject;
import uk.gov.gchq.gaffer.integration.domain.EntityDomainObject;
import uk.gov.gchq.gaffer.integration.extensions.GafferTestCase;
import uk.gov.gchq.gaffer.integration.generators.BasicElementGenerator;
import uk.gov.gchq.gaffer.integration.generators.BasicObjectGenerator;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.data.EdgeSeed;
import uk.gov.gchq.gaffer.operation.data.EntitySeed;
import uk.gov.gchq.gaffer.operation.impl.add.AddElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateElements;
import uk.gov.gchq.gaffer.operation.impl.generate.GenerateObjects;
import uk.gov.gchq.gaffer.operation.impl.get.GetElements;
import uk.gov.gchq.gaffer.user.User;

import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.DEST_1;
import static uk.gov.gchq.gaffer.integration.util.TestUtil.SOURCE_1;

public class GeneratorsITTemplate extends AbstractStoreIT {
    private static final String NEW_SOURCE = "newSource";
    private static final String NEW_DEST = "newDest";
    private static final String NEW_VERTEX = "newVertex";

    @GafferTest
    public void shouldConvertToDomainObjects(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final OperationChain<Iterable<? extends DomainObject>> opChain = new OperationChain.Builder()
                .first(new GetElements.Builder()
                        .input(new EntitySeed(SOURCE_1))
                        .build())
                .then(new GenerateObjects.Builder<DomainObject>()
                        .generator(new BasicObjectGenerator())
                        .build())
                .build();

        // When
        final List<DomainObject> results = Lists.newArrayList(graph.execute(opChain, new User()));

        final EntityDomainObject entityDomainObject = new EntityDomainObject(SOURCE_1, "3", null);
        final EdgeDomainObject edgeDomainObject = new EdgeDomainObject(SOURCE_1, DEST_1, false, 1, 1L);

        // Then
        assertNotNull(results);
        assertEquals(2, results.size());
        assertTrue(results.containsAll(Lists.newArrayList(entityDomainObject, edgeDomainObject)));
    }

    @GafferTest
    public void shouldConvertFromDomainObjects(final GafferTestCase testCase) throws OperationException {
        // Given
        Graph graph = testCase.getPopulatedGraph();
        final OperationChain<Void> opChain = new OperationChain.Builder()
                .first(new GenerateElements.Builder<DomainObject>()
                        .generator(new BasicElementGenerator())
                        .input(new EntityDomainObject(NEW_VERTEX, "1", null),
                                new EdgeDomainObject(NEW_SOURCE, NEW_DEST, false, 1, 1L))
                        .build())
                .then(new AddElements())
                .build();

        // When - add
        graph.execute(opChain, new User());

        // Then - check they were added correctly
        final List<Element> results = Lists.newArrayList(graph.execute(new GetElements.Builder()
                .input(new EntitySeed(NEW_VERTEX), new EdgeSeed(NEW_SOURCE, NEW_DEST, false))
                .build(), new User()));

        final Edge expectedEdge = new Edge.Builder()
                .group(TestGroups.EDGE)
                .source(NEW_SOURCE)
                .dest(NEW_DEST)
                .directed(false)
                .matchedVertex(EdgeId.MatchedVertex.DESTINATION)
                .build();
        expectedEdge.putProperty(TestPropertyNames.INT, 1);
        expectedEdge.putProperty(TestPropertyNames.COUNT, 1L);

        final Entity expectedEntity = new Entity(TestGroups.ENTITY, NEW_VERTEX);
        expectedEntity.putProperty(TestPropertyNames.SET, CollectionUtil.treeSet("1"));

        ElementUtil.assertElementEquals(Arrays.asList(expectedEntity, expectedEdge), results);
    }
}
