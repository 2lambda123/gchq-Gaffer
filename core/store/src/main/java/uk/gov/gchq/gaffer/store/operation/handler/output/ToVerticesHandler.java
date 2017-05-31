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
package uk.gov.gchq.gaffer.store.operation.handler.output;

import uk.gov.gchq.gaffer.commonutil.iterable.StreamIterable;
import uk.gov.gchq.gaffer.commonutil.stream.Streams;
import uk.gov.gchq.gaffer.data.element.id.EdgeId;
import uk.gov.gchq.gaffer.data.element.id.ElementId;
import uk.gov.gchq.gaffer.data.element.id.EntityId;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices;
import uk.gov.gchq.gaffer.operation.impl.output.ToVertices.EdgeVertices;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import java.util.function.Function;
import java.util.stream.Stream;

public class ToVerticesHandler implements OutputOperationHandler<ToVertices, Iterable<? extends Object>> {

    @Override
    public Iterable<Object> doOperation(final ToVertices operation, final Context context, final Store store) throws OperationException {
        if (null == operation.getInput()) {
            return null;
        }

        return new StreamIterable<>(Streams.toStream(operation.getInput())
                                           .flatMap(elementIdsToVertices(operation)));
    }

    private Function<ElementId, Stream<Object>> elementIdsToVertices(final ToVertices operation) {
        return e -> {
            Stream<Object> vertices = Stream.empty();

            if (e instanceof EdgeId) {
                final EdgeId edgeId = (EdgeId) e;
                if (operation.getEdgeVertices() != EdgeVertices.NONE) {
                    switch (operation.getEdgeVertices()) {
                        case BOTH:
                            vertices = Stream.of(edgeId.getSource(), edgeId.getDestination());
                            break;
                        case SOURCE:
                            vertices = Stream.of(edgeId.getSource());
                            break;
                        case DESTINATION:
                            vertices = Stream.of(edgeId.getDestination());
                            break;
                        default:
                            break;
                    }
                }
            } else {
                vertices = Stream.of(((EntityId) e).getVertex());
            }
            return vertices;
        };
    }
}
