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
package uk.gov.gchq.gaffer.sparkaccumulo.operation.handler.dataframe;

import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.DataTypes;
import scala.collection.JavaConverters;
import uk.gov.gchq.gaffer.spark.operation.dataframe.ConversionException;
import uk.gov.gchq.gaffer.spark.operation.dataframe.Converter;
import uk.gov.gchq.gaffer.types.FreqMap;

/**
 * A {@link Converter} that converts a {@link FreqMap} into a Scala map that is suitable for inclusion
 * in a Dataframe.
 */
public class FreqMapConverter implements Converter {
    private static final long serialVersionUID = -1247681579101863145L;

    @Override
    public boolean canHandle(final Class clazz) {
        return FreqMap.class.equals(clazz);
    }

    @Override
    public DataType convertedType() {
        return DataTypes.createMapType(DataTypes.StringType, DataTypes.LongType, true);
    }

    @Override
    public Object convert(final Object object) throws ConversionException {
        return JavaConverters.mapAsScalaMapConverter((FreqMap) object).asScala();
    }
}
