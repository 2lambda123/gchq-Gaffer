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

package uk.gov.gchq.gaffer.flink.integration.loader;

import org.apache.commons.io.FileUtils;
import org.junit.Rule;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import uk.gov.gchq.gaffer.commonutil.CommonTestConstants;
import uk.gov.gchq.gaffer.data.element.Element;
import uk.gov.gchq.gaffer.data.generator.JsonToElementGenerator;
import uk.gov.gchq.gaffer.integration.impl.loader.AbstractStandaloneLoaderIT;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.mapstore.MapStore;
import uk.gov.gchq.gaffer.operation.impl.add.AddElementsFromFile;
import uk.gov.gchq.gaffer.store.StoreProperties;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.runners.Parameterized.Parameters;


// TODO: Run on AccumuloStore
@RunWith(Parameterized.class)
public class AddElementsFromFileLoaderIT extends AbstractStandaloneLoaderIT<AddElementsFromFile> {

    @Parameters
    public static Iterable<? extends String> instances() {
        final Object[] obj = new Object[]{};

        return Arrays.asList("mapStore.properties", "accumuloStore.properties");
    }

    @Rule
    public final TemporaryFolder testFolder = new TemporaryFolder(CommonTestConstants.TMP_DIRECTORY);
    private File file;

    private final String storePropertiesPath;

    public AddElementsFromFileLoaderIT(final String storePropertiesPath) {
        this.storePropertiesPath = storePropertiesPath;
    }

    @Override
    protected void configure(final Iterable<? extends Element> elements) throws Exception {
        if (storePropertiesPath.contains("map")) {
            MapStore.resetStaticMap();
        }

        file = testFolder.newFile("inputFile.txt");

        final List<String> lines = new ArrayList<>();

        for (final Element element : elements) {
            final String json = JSONSerialiser.createDefaultMapper().writeValueAsString(element);
            lines.add(json);
        }

        FileUtils.writeLines(file, lines);
    }

    @Override
    protected AddElementsFromFile createOperation(final Iterable<? extends Element> elements) {
        return new AddElementsFromFile.Builder()
                .filename(file.getAbsolutePath())
                .generator(JsonToElementGenerator.class)
                .validate(false)
                .skipInvalidElements(false)
                .build();
    }

    @Override
    public StoreProperties createStoreProperties() {
        final StoreProperties storeProperties = StoreProperties.loadStoreProperties(storePropertiesPath);
        storeProperties.addOperationDeclarationPaths("FlinkOperationDeclarations.json");

        return storeProperties;
    }
}
