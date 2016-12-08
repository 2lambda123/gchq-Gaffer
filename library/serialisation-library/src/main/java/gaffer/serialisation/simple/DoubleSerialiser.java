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
package gaffer.serialisation.simple;

import gaffer.commonutil.CommonConstants;
import gaffer.exception.SerialisationException;
import gaffer.serialisation.AbstractSerialisation;
import java.io.UnsupportedEncodingException;

/**
 * @deprecated this is not very efficient and should only be used for compatibility
 * reasons. For new properties use {@link gaffer.serialisation.implementation.raw.RawDoubleSerialiser}
 * instead.
 */
@Deprecated
public class DoubleSerialiser extends AbstractSerialisation<Double> {
    private static final long serialVersionUID = 5647756843689779437L;

    @Override
    public boolean canHandle(final Class clazz) {
        return Double.class.equals(clazz);
    }

    @Override
    public byte[] serialise(final Double value) throws SerialisationException {
        try {
            return value.toString().getBytes(CommonConstants.ISO_8859_1_ENCODING);
        } catch (UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Double deserialise(final byte[] bytes) throws SerialisationException {
        try {
            return Double.parseDouble(new String(bytes, CommonConstants.ISO_8859_1_ENCODING));
        } catch (NumberFormatException | UnsupportedEncodingException e) {
            throw new SerialisationException(e.getMessage(), e);
        }
    }

    @Override
    public Double deserialiseEmptyBytes() {
        return null;
    }

    @Override
    public boolean isByteOrderPreserved() {
        return true;
    }
}
