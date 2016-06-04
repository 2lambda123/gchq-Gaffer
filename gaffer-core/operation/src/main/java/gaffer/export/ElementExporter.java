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

package gaffer.export;

import gaffer.commonutil.iterable.CloseableIterable;
import gaffer.data.element.Element;
import gaffer.user.User;

public abstract class ElementExporter extends Exporter {
    @Override
    protected void _add(final String key, final Iterable<?> values, final User user) {
        addElements(key, (Iterable<Element>) values, user);
    }

    @Override
    protected CloseableIterable<?> _get(final String key, final User user,
                                        final int start, final int end) {
        return getElements(key, user, start, end);
    }

    protected abstract void addElements(final String key, final Iterable<Element> elements, final User user);

    protected abstract CloseableIterable<Element> getElements(final String key, final User user, final int start, final int end);
}
