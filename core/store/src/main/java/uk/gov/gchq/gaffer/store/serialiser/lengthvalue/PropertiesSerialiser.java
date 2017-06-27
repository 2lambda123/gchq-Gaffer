/*
 * Copyright 2016-2017 Crown Copyright
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

package uk.gov.gchq.gaffer.store.serialiser.lengthvalue;

import uk.gov.gchq.gaffer.data.element.Properties;
import uk.gov.gchq.gaffer.exception.SerialisationException;
import uk.gov.gchq.gaffer.serialisation.ToBytesSerialiser;
import uk.gov.gchq.gaffer.serialisation.util.LengthValueBytesSerialiserUtil;
import uk.gov.gchq.gaffer.store.schema.Schema;
import uk.gov.gchq.gaffer.store.schema.SchemaElementDefinition;
import uk.gov.gchq.gaffer.store.schema.TypeDefinition;
import java.io.ByteArrayOutputStream;
import java.util.Iterator;

public abstract class PropertiesSerialiser {
    protected Schema schema;

    // Required for serialisation
    PropertiesSerialiser() {
    }

    protected PropertiesSerialiser(final Schema schema) {
        updateSchema(schema);
    }

    public void updateSchema(final Schema schema) {
        this.schema = schema;
    }

    public boolean canHandle(final Class clazz) {
        return Properties.class.isAssignableFrom(clazz);
    }

    public boolean preservesObjectOrdering() {
        return false;
    }

    protected void serialiseProperties(final Properties properties, final SchemaElementDefinition elementDefinition, final ByteArrayOutputStream out) throws SerialisationException {
        for (final String propertyName : elementDefinition.getProperties()) {
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final ToBytesSerialiser<Object> serialiser = (typeDefinition != null) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;
            LengthValueBytesSerialiserUtil.serialise(serialiser, properties.get(propertyName), out);
        }
    }

    protected void deserialiseProperties(final byte[] bytes, final Properties properties, final SchemaElementDefinition elementDefinition, final int[] delimiter) throws SerialisationException {
        final int arrayLength = bytes.length;
        final Iterator<String> propertyNames = elementDefinition.getProperties().iterator();
        while (propertyNames.hasNext() && delimiter[0] < arrayLength) {
            final String propertyName = propertyNames.next();
            final TypeDefinition typeDefinition = elementDefinition.getPropertyTypeDef(propertyName);
            final ToBytesSerialiser<Object> serialiser = (typeDefinition != null) ? (ToBytesSerialiser) typeDefinition.getSerialiser() : null;

            final Object property = LengthValueBytesSerialiserUtil.deserialise(serialiser, bytes, delimiter);
            properties.put(propertyName, property);
        }
    }
}

