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

package uk.gov.gchq.gaffer.flink.operation;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.configuration.Configuration;

import uk.gov.gchq.gaffer.data.element.Element;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class TestFileOutput extends RichOutputFormat<Element> implements ElementFileStore {
    private final AtomicInteger fileId = new AtomicInteger();
    private final String path;

    public TestFileOutput(final String path) {
        this.path = path;
    }

    @Override
    public String getStorePath() {
        return path;
    }

    @Override
    public void configure(final Configuration parameters) {
    }

    @Override
    public void open(final int taskNumber, final int numTasks) throws IOException {
    }

    @Override
    public void writeRecord(final Element element) throws IOException {
        writeElement(fileId.getAndIncrement(), element);
    }

    @Override
    public void close() throws IOException {
    }
}
