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

package gaffer.accumulostore.retriever.impl;

import com.google.common.collect.Sets;
import gaffer.accumulostore.AccumuloStore;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.key.exception.IteratorSettingException;
import gaffer.accumulostore.retriever.RetrieverException;
import gaffer.accumulostore.utils.CloseableIterator;
import gaffer.data.element.Element;
import gaffer.operation.impl.get.GetAllElements;
import gaffer.store.StoreException;
import gaffer.user.User;
import org.apache.accumulo.core.client.BatchScanner;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;
import java.util.Set;

/**
 * This allows queries for all elements.
 */
public class AccumuloAllElementsRetriever extends AccumuloSingleIDRetriever {
    private static final Logger LOGGER = LoggerFactory.getLogger(AccumuloAllElementsRetriever.class);

    public AccumuloAllElementsRetriever(final AccumuloStore store, final GetAllElements<?> operation,
                                        final User user)
            throws IteratorSettingException, StoreException {
        this(store, operation, user,
                store.getKeyPackage().getIteratorFactory().getElementFilterIteratorSetting(operation.getView(), store),
                store.getKeyPackage().getIteratorFactory().getEdgeEntityDirectionFilterIteratorSetting(operation));
    }

    /**
     * Use of the varargs parameter here will mean the usual default iterators
     * wont be applied, (Edge Direction,Edge/Entity TypeDefinition and View Filtering) To
     * apply them pass them directly to the varargs via calling your
     * keyPackage.getIteratorFactory() and either
     * getElementFilterIteratorSetting and/Or
     * getEdgeEntityDirectionFilterIteratorSetting
     *
     * @param store            the accumulo store
     * @param operation        the get all elements operation
     * @param user             the user executing the operation
     * @param iteratorSettings the iterator settings
     * @throws StoreException if any store issues occur
     */
    public AccumuloAllElementsRetriever(final AccumuloStore store, final GetAllElements<?> operation,
                                        final User user,
                                        final IteratorSetting... iteratorSettings) throws StoreException {
        super(store, operation, user, iteratorSettings);
    }

    @Override
    public Iterator<Element> iterator() {
        try {
            iterator = new AllElementsIterator();
        } catch (final RetrieverException e) {
            LOGGER.error(e.getMessage() + " returning empty iterator", e);
            return Collections.emptyIterator();
        }

        return iterator;
    }

    protected class AllElementsIterator implements CloseableIterator<Element> {
        private BatchScanner scanner;
        private Iterator<Map.Entry<Key, Value>> scannerIterator;

        protected AllElementsIterator() throws RetrieverException {
            final Set<Range> ranges = Sets.newHashSet(new Range());
            try {
                scanner = getScanner(ranges);
            } catch (TableNotFoundException | StoreException e) {
                throw new RetrieverException(e);
            }
            scannerIterator = scanner.iterator();
        }

        @Override
        public boolean hasNext() {
            final boolean scannerHasNext = scannerIterator.hasNext();
            if (!scannerHasNext) {
                scanner.close();
            }

            return scannerHasNext;
        }

        @Override
        public Element next() {
            final Map.Entry<Key, Value> entry = scannerIterator.next();
            try {
                final Element elm = elementConverter.getFullElement(entry.getKey(), entry.getValue(),
                        operation.getOptions());
                doTransformation(elm);
                return elm;
            } catch (final AccumuloElementConversionException e) {
                LOGGER.error("Failed to re-create an element from a key value entry set returning next element as null",
                        e);
                return null;
            }
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException("Unable to remove elements from this iterator");
        }

        @Override
        public void close() {
            if (scanner != null) {
                scanner.close();
            }
        }
    }
}
