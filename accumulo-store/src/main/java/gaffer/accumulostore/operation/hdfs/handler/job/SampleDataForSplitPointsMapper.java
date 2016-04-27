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
package gaffer.accumulostore.operation.hdfs.handler.job;

import gaffer.accumulostore.key.AccumuloElementConverter;
import gaffer.accumulostore.key.exception.AccumuloElementConversionException;
import gaffer.accumulostore.utils.AccumuloStoreConstants;
import gaffer.accumulostore.utils.Pair;
import gaffer.commonutil.CommonConstants;
import gaffer.data.element.Element;
import gaffer.data.elementdefinition.exception.SchemaException;
import gaffer.store.schema.Schema;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.hadoop.mapreduce.Mapper;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.InvocationTargetException;

/**
* Mapper class used for estimating the split points to ensure even distribution of
* data in Accumulo after initial insert.
*/
public class SampleDataForSplitPointsMapper<KEY_IN, VALUE_IN> extends Mapper<KEY_IN, VALUE_IN, Key, Value> {

    private float proportionToSample;
    private AccumuloElementConverter elementConverter;

    protected void setup(final Context context) {
       proportionToSample = context.getConfiguration().getFloat(SampleDataForSplitPointsJobFactory.PROPORTION_TO_SAMPLE, 0.001f);
       final Schema schema;
       try {
           schema = Schema.fromJson(context.getConfiguration()
                   .get(SampleDataForSplitPointsJobFactory.SCHEMA).getBytes(CommonConstants.UTF_8));
       } catch (final UnsupportedEncodingException e) {
           throw new SchemaException("Unable to deserialise Store Schema from JSON");
       }

       final String converterClass = context.getConfiguration().get(AccumuloStoreConstants.ACCUMULO_ELEMENT_CONVERTER_CLASS);
       try {
           final Class<?> elementConverterClass = Class.forName(converterClass);
           elementConverter = (AccumuloElementConverter) elementConverterClass.getConstructor(Schema.class)
                   .newInstance(schema);
       } catch (ClassNotFoundException | InstantiationException | IllegalAccessException | IllegalArgumentException
               | InvocationTargetException | NoSuchMethodException | SecurityException e) {
           throw new IllegalArgumentException("Element converter could not be created: " + converterClass, e);
       }
   }

   protected void map(final Element element, final Context context) throws IOException, InterruptedException {
       if (Math.random() < proportionToSample) {
           context.getCounter("Split points", "Number sampled").increment(1L);
           final Pair<Key> keyPair;
           try {
               keyPair = elementConverter.getKeysFromElement(element);
           } catch (final AccumuloElementConversionException e) {
               throw new IllegalArgumentException(e.getMessage(), e);
           }

           final Value value;
           try {
               value = elementConverter.getValueFromElement(element);
           } catch (final AccumuloElementConversionException e) {
               throw new IllegalArgumentException(e.getMessage(), e);
           }
           context.write(keyPair.getFirst(), value);
           if (keyPair.getSecond() != null) {
               context.write(keyPair.getSecond(), value);
           }
       } else {
           context.getCounter("Split points", "Number not sampled").increment(1L);
       }
   }

}
