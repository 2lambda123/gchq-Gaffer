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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.scalardd;

import org.apache.hadoop.conf.Configuration;
import org.apache.spark.rdd.RDD;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.rules.TemporaryFolder;
import scala.collection.mutable.ArrayBuffer;
import scala.reflect.ClassTag;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.TestGroups;
import uk.gov.gchq.gaffer.commonutil.TestPropertyNames;
import uk.gov.gchq.gaffer.data.element.Edge;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.element.Entity;
import uk.gov.gchq.gaffer.graph.Graph;
import uk.gov.gchq.gaffer.graph.GraphConfig;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.spark.SparkSessionProvider;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ClassTagConstants;
import uk.gov.gchq.gaffer.spark.operation.scalardd.GetRDDOfAllElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.ImportRDDOfElements;
import uk.gov.gchq.gaffer.spark.operation.scalardd.SplitStoreFromRDDOfElements;
import uk.gov.gchq.gaffer.sparkaccumulo.AbstractPropertiesDrivenTest;
import uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.AbstractGetRDDHandler;
import uk.gov.gchq.gaffer.user.User;

import java.io.IOException;
import java.util.HashSet;
import java.util.Set;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public class SplitStoreFromRDDOfElementsHandlerTest extends AbstractPropertiesDrivenTest {

    private static final ClassTag<Element> ELEMENT_CLASS_TAG = ClassTagConstants.ELEMENT_CLASS_TAG;

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @Rule
    public final ExpectedException expectedException = ExpectedException.none();

    private final User user = new User();

    private String outputPath;
    private String failurePath;
    private Graph graph;
    private ArrayBuffer<Element> elements;
    private RDD<Element> rdd;
    private String configurationString;

    @ClassRule
    public static TemporaryFolder storeBaseFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    @BeforeClass
    public static void setup() {
        setUpBeforeClass("/store.properties", storeBaseFolder);
    }

    @AfterClass
    public static void teardown() {
        tearDownAfterClass();
    }

    @Before
    public void setUp() throws IOException {

        graph = createGraph();
        elements = createElements();
        rdd = createRDDContaining(elements);
        configurationString = createConfigurationString();
        outputPath = testFolder.getRoot().getAbsolutePath() + "/output";
        failurePath = testFolder.getRoot().getAbsolutePath() + "/failure";
    }

    private Graph createGraph() {

        return new Graph.Builder()
                .config(new GraphConfig.Builder()
                        .graphId("graphId")
                        .build())
                .addSchema(getClass().getResourceAsStream("/schema/elements.json"))
                .addSchema(getClass().getResourceAsStream("/schema/types.json"))
                .addSchema(getClass().getResourceAsStream("/schema/serialisation.json"))
                .storeProperties(getStoreProperties())
                .build();
    }

    private ArrayBuffer<Element> createElements() {

        final ArrayBuffer<Element> elements = new ArrayBuffer<>();
        for (int i = 0; i < 10; i++) {
            final Entity entity = new Entity.Builder()
                    .group(TestGroups.ENTITY)
                    .vertex("" + i)
                    .build();

            final Edge edge1 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("B")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 2)
                    .build();

            final Edge edge2 = new Edge.Builder()
                    .group(TestGroups.EDGE)
                    .source("" + i)
                    .dest("C")
                    .directed(false)
                    .property(TestPropertyNames.COUNT, 4)
                    .build();

            elements.$plus$eq(edge1);
            elements.$plus$eq(edge2);
            elements.$plus$eq(entity);
        }

        return elements;
    }

    private RDD<Element> createRDDContaining(final ArrayBuffer<Element> elements) {

        return SparkSessionProvider.getSparkSession().sparkContext().parallelize(elements, 8, ELEMENT_CLASS_TAG);
    }

    private String createConfigurationString() throws IOException {

        // Create Hadoop configuration and serialise to a string
        final Configuration configuration = new Configuration();
        return AbstractGetRDDHandler.convertConfigurationToString(configuration);
    }

    @Test
    public void throwsExceptionWhenNumSplitPointsIsLessThanOne() throws OperationException {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("numSplits must be null or greater than 0");

        final SplitStoreFromRDDOfElements splitStoreHandler = new SplitStoreFromRDDOfElements.Builder()
                .input(rdd)
                .numSplits(-1)
                .build();
        graph.execute(splitStoreHandler, user);

    }

    @Test
    public void throwsExceptionWhenMaxSampleSizeIsLessThanOne() throws OperationException {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("maxSampleSize must be null or greater than 0");

        final SplitStoreFromRDDOfElements splitStoreHandler = new SplitStoreFromRDDOfElements.Builder()
                .input(rdd)
                .maxSampleSize(-1)
                .build();
        graph.execute(splitStoreHandler, user);
    }

    @Test
    public void throwsExceptionWhenFractionToSampleIsGreaterThanOne() throws OperationException {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("fractionToSample must be null or between 0 exclusive and 1 inclusive");

        final SplitStoreFromRDDOfElements splitStoreHandler = new SplitStoreFromRDDOfElements.Builder()
                .input(rdd)
                .fractionToSample(1.000000001d)
                .build();
        graph.execute(splitStoreHandler, user);
    }

    @Test
    public void throwsExceptionWhenFractionToSampleIsZero() throws OperationException {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("fractionToSample must be null or between 0 exclusive and 1 inclusive");

        final SplitStoreFromRDDOfElements splitStoreHandler = new SplitStoreFromRDDOfElements.Builder()
                .input(rdd)
                .fractionToSample(0d)
                .build();
        graph.execute(splitStoreHandler, user);
    }

    @Test
    public void throwsExceptionWhenFractionToSampleLessThanZero() throws OperationException {

        expectedException.expect(IllegalArgumentException.class);
        expectedException.expectMessage("fractionToSample must be null or between 0 exclusive and 1 inclusive");

        final SplitStoreFromRDDOfElements splitStoreHandler = new SplitStoreFromRDDOfElements.Builder()
                .input(rdd)
                .fractionToSample(-0.00000001d)
                .build();
        graph.execute(splitStoreHandler, user);
    }

    @Test
    public void canBeSuccessfullyChainedWithImport() throws Exception {

        graph.execute(new OperationChain.Builder()
                .first(new SplitStoreFromRDDOfElements.Builder()
                        .input(rdd)
                        .build())
                .then(new ImportRDDOfElements.Builder()
                        .input(rdd)
                        .option("outputPath", outputPath)
                        .option("failurePath", failurePath)
                        .build()).build(), user);

        // Check all elements were added
        final GetRDDOfAllElements rddQuery = new GetRDDOfAllElements.Builder()
                .option(AbstractGetRDDHandler.HADOOP_CONFIGURATION_KEY, configurationString)
                .build();

        final RDD<Element> rdd = graph.execute(rddQuery, user);
        if (rdd == null) {
            fail("No RDD returned");
        }
        final Set<Element> results = new HashSet<>(rdd.toJavaRDD().collect());
        assertEquals(elements.size(), results.size());
    }
}
