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

package gaffer.arrayliststore.export;

import gaffer.data.element.Element;
import gaffer.graph.Graph;
import gaffer.graph.export.GafferExporter;
import gaffer.store.StoreProperties;
import gaffer.user.User;
import java.util.Map;

public class ArrayListStoreExporter extends GafferExporter {

    @Override
    public Map<String, Graph> getGraphExports() {
        return super.getGraphExports();
    }

    @Override
    public Graph getGraphExport(final String key) {
        return super.getGraphExport(key);
    }

    @Override
    protected StoreProperties createExportStorePropsWithKey(final String key) {
        // Just return a clone of the store properties.
        // This will mean a new instance of ArrayListStore will be created so
        // the elements will be kept in a separate array list.
        return null != getStoreProperties() ? getStoreProperties().clone() : null;
    }

    @Override
    protected void validateAdd(final String key, final Iterable<Element> elements, final User user) {
        // no validation required.
    }

    @Override
    protected void validateGet(final String key, final User user, final int start, final int end) {
        // no validation required.
    }
}
