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
package gaffer.example.operation;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Element;
import gaffer.data.element.function.ElementFilter;
import gaffer.data.elementdefinition.view.View;
import gaffer.data.elementdefinition.view.ViewElementDefinition;
import gaffer.function.simple.filter.IsMoreThan;
import gaffer.operation.data.EdgeSeed;
import gaffer.operation.data.EntitySeed;
import gaffer.operation.impl.get.GetElementsBySeed;

public class GetElementsBySeedExample extends OperationExample {
    public static void main(final String[] args) {
        new GetElementsBySeedExample().run();
    }

    public GetElementsBySeedExample() {
        super(GetElementsBySeed.class);
    }

    public void runExamples() {
        getEntitiesAndEdgesByEntitySeed2AndEdgeSeed2to3();
        getEntitiesAndEdgesByEntitySeed2AndEdgeSeed2to3WithCountGreaterThan1();
    }

    public CloseableIterable<Element> getEntitiesAndEdgesByEntitySeed2AndEdgeSeed2to3() {
        final String opJava = "new GetElementsBySeed.Builder<>()\n"
                + "                .addSeed(new EntitySeed(2))\n"
                + "                .addSeed(new EdgeSeed(2, 3, true))\n"
                + "                .build();";
        return runExample(new GetElementsBySeed.Builder<>()
                .addSeed(new EntitySeed(2))
                .addSeed(new EdgeSeed(2, 3, true))
                .build(), opJava);
    }

    public CloseableIterable<Element> getEntitiesAndEdgesByEntitySeed2AndEdgeSeed2to3WithCountGreaterThan1() {
        final String opJava = "new GetElementsBySeed.Builder<>()\n"
                + "                .addSeed(new EntitySeed(2))\n"
                + "                .addSeed(new EdgeSeed(2, 3, true))\n"
                + "                .view(new View.Builder()\n"
                + "                        .entity(\"entity\", new ViewElementDefinition.Builder()\n"
                + "                                .filter(new ElementFilter.Builder()\n"
                + "                                        .select(\"count\")\n"
                + "                                        .execute(new IsMoreThan(1))\n"
                + "                                        .build())\n"
                + "                                .build())\n"
                + "                        .edge(\"edge\", new ViewElementDefinition.Builder()\n"
                + "                                .filter(new ElementFilter.Builder()\n"
                + "                                        .select(\"count\")\n"
                + "                                        .execute(new IsMoreThan(1))\n"
                + "                                        .build())\n"
                + "                                .build())\n"
                + "                        .build())\n"
                + "                .build();";
        return runExample(new GetElementsBySeed.Builder<>()
                .addSeed(new EntitySeed(2))
                .addSeed(new EdgeSeed(2, 3, true))
                .view(new View.Builder()
                        .entity("entity", new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .edge("edge", new ViewElementDefinition.Builder()
                                .filter(new ElementFilter.Builder()
                                        .select("count")
                                        .execute(new IsMoreThan(1))
                                        .build())
                                .build())
                        .build())
                .build(), opJava);
    }
}
