/*
 * Copyright 2017-2023 Crown Copyright
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

package uk.gov.gchq.gaffer.spark.serialisation.kryo.impl.datasketches.sampling;

import org.apache.datasketches.sampling.ReservoirLongsSketch;

import uk.gov.gchq.gaffer.sketches.datasketches.sampling.serialisation.ReservoirLongsSketchSerialiser;
import uk.gov.gchq.gaffer.spark.serialisation.kryo.WrappedKryoSerializer;

/**
 * A {@code ReservoirLongsSketchKryoSerializer} is a {@link com.esotericsoftware.kryo.Kryo} {@link com.esotericsoftware.kryo.Serializer} for
 * a {@link ReservoirLongsSketch}
 */
public class ReservoirLongsSketchKryoSerializer extends WrappedKryoSerializer<ReservoirLongsSketchSerialiser, ReservoirLongsSketch> {

    public ReservoirLongsSketchKryoSerializer() {
        super(new ReservoirLongsSketchSerialiser());
    }
}
