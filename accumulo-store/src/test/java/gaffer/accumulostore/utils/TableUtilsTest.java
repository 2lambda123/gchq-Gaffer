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

package gaffer.accumulostore.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import gaffer.accumulostore.AccumuloProperties;
import gaffer.accumulostore.MockAccumuloStore;
import gaffer.accumulostore.key.core.impl.byteEntity.ByteEntityAccumuloElementConverter;
import gaffer.accumulostore.key.impl.AggregatorIterator;
import gaffer.accumulostore.key.impl.ValidatorFilter;
import gaffer.commonutil.PathUtil;
import gaffer.data.elementdefinition.schema.DataEdgeDefinition;
import gaffer.data.elementdefinition.schema.DataSchema;
import gaffer.store.schema.StoreElementDefinition;
import gaffer.store.schema.StoreSchema;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.conf.Property;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.junit.Test;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;

public class TableUtilsTest {
    public static final String TABLE_NAME = "table1";

    @Test
    public void shouldCreateTableWithAllRequiredIterators() throws Exception {
        // Given
        final MockAccumuloStore store = new MockAccumuloStore();
        final DataSchema dataSchema = new DataSchema.Builder()
                .edge("BasicEdge", new DataEdgeDefinition.Builder()
                        .source(String.class)
                        .destination(String.class)
                        .directed(Boolean.class)
                        .build())
                .build();
        final StoreSchema storeSchema = new StoreSchema.Builder()
                .edge("BasicEdge", new StoreElementDefinition.Builder()
                        .build())
                .build();

        final AccumuloProperties props = new AccumuloProperties(PathUtil.storeProps(TableUtilsTest.class));
        store.initialise(dataSchema, storeSchema, props);

        // When
        TableUtils.createTable(store);

        // Then
        final Map<String, EnumSet<IteratorScope>> itrs = store.getConnection().tableOperations().listIterators(TABLE_NAME);
        assertEquals(2, itrs.size());

        final EnumSet<IteratorScope> validator = itrs.get(AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME);
        assertEquals(EnumSet.allOf(IteratorScope.class), validator);
        final IteratorSetting validatorSetting = store.getConnection().tableOperations().getIteratorSetting(TABLE_NAME, AccumuloStoreConstants.VALIDATOR_ITERATOR_NAME, IteratorScope.majc);
        assertEquals(AccumuloStoreConstants.VALIDATOR_ITERATOR_PRIORITY, validatorSetting.getPriority());
        assertEquals(ValidatorFilter.class.getName(), validatorSetting.getIteratorClass());
        final Map<String, String> validatorOptions = validatorSetting.getOptions();
        assertNotNull(DataSchema.fromJson(validatorOptions.get(AccumuloStoreConstants.DATA_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET)).getEdge("BasicEdge"));
        assertNotNull(StoreSchema.fromJson(validatorOptions.get(AccumuloStoreConstants.STORE_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET)).getEdge("BasicEdge"));
        assertEquals(ByteEntityAccumuloElementConverter.class.getName(), validatorOptions.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));

        final EnumSet<IteratorScope> aggregator = itrs.get(AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME);
        assertEquals(EnumSet.allOf(IteratorScope.class), aggregator);
        final IteratorSetting aggregatorSetting = store.getConnection().tableOperations().getIteratorSetting(TABLE_NAME, AccumuloStoreConstants.AGGREGATOR_ITERATOR_NAME, IteratorScope.majc);
        assertEquals(AccumuloStoreConstants.AGGREGATOR_ITERATOR_PRIORITY, aggregatorSetting.getPriority());
        assertEquals(AggregatorIterator.class.getName(), aggregatorSetting.getIteratorClass());
        final Map<String, String> aggregatorOptions = aggregatorSetting.getOptions();
        assertNotNull(DataSchema.fromJson(aggregatorOptions.get(AccumuloStoreConstants.DATA_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET)).getEdge("BasicEdge"));
        assertNotNull(StoreSchema.fromJson(aggregatorOptions.get(AccumuloStoreConstants.STORE_SCHEMA).getBytes(AccumuloStoreConstants.UTF_8_CHARSET)).getEdge("BasicEdge"));
        assertEquals(ByteEntityAccumuloElementConverter.class.getName(), aggregatorOptions.get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS));


        final Map<String, String> tableProps = new HashMap<>();
        for (Entry<String, String> entry : store.getConnection().tableOperations().getProperties(TABLE_NAME)) {
            tableProps.put(entry.getKey(), entry.getValue());
        }

        assertEquals(2, Integer.parseInt(tableProps.get(Property.TABLE_FILE_REPLICATION.getKey())));
    }
}
