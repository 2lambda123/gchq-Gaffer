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
package uk.gov.gchq.gaffer.accumulostore.operation.hdfs.handler.job.factory;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.junit.Before;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;

import uk.gov.gchq.gaffer.accumulostore.AccumuloProperties;
import uk.gov.gchq.gaffer.accumulostore.SingleUseMockAccumuloStore;
import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.commonutil.StreamUtil;
import uk.gov.gchq.gaffer.hdfs.operation.MapReduce;
import uk.gov.gchq.gaffer.hdfs.operation.SampleDataForSplitPoints;
import uk.gov.gchq.gaffer.hdfs.operation.hander.job.factory.AbstractJobFactoryTest;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.factory.JobFactory;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.JsonMapperGenerator;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.StoreException;
import uk.gov.gchq.gaffer.store.schema.Schema;

import java.io.IOException;

public class AccumuloSampleDataForSplitPointsJobFactoryTest extends AbstractJobFactoryTest {
    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);

    public String inputDir;
    public String outputDir;
    public String splitsDir;
    public String splitsFile;

    @Before
    public void setup() {
        inputDir = testFolder.getRoot().getAbsolutePath() + "inputDir";
        outputDir = testFolder.getRoot().getAbsolutePath() + "/outputDir";
        splitsDir = testFolder.getRoot().getAbsolutePath() + "/splitsDir";
        splitsFile = splitsDir + "/splits";
    }

    @Override
    protected Store getStoreConfiguredWith(final Class<JSONSerialiser> jsonSerialiserClass, final String jsonSerialiserModules, final Boolean strictJson) throws IOException, StoreException {
        final SingleUseMockAccumuloStore store = new SingleUseMockAccumuloStore();
        final Schema schema = Schema.fromJson(StreamUtil.schemas(AccumuloAddElementsFromHdfsJobFactoryTest.class));
        final AccumuloProperties properties = AccumuloProperties.loadStoreProperties(StreamUtil.storeProps(AccumuloAddElementsFromHdfsJobFactoryTest.class));

        super.configureStoreProperties(properties, jsonSerialiserClass, jsonSerialiserModules, strictJson);

        store.initialise("graphId", schema, properties);

        final FileSystem fileSystem = FileSystem.getLocal(new Configuration());
        fileSystem.mkdirs(new Path(outputDir));
        fileSystem.mkdirs(new Path(splitsDir));

        return store;
    }

    @Override
    protected JobFactory getJobFactory() {
        return new AccumuloSampleDataForSplitPointsJobFactory();
    }

    @Override
    protected MapReduce getMapReduceOperation() {
        return new SampleDataForSplitPoints.Builder()
                .outputPath(outputDir)
                .addInputMapperPair(inputDir, JsonMapperGenerator.class.getName())
                .build();
    }
}
