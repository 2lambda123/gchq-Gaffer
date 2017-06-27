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
package uk.gov.gchq.gaffer.hdfs.operation;

import com.fasterxml.jackson.annotation.JsonSetter;
import org.apache.hadoop.mapreduce.Partitioner;
import uk.gov.gchq.gaffer.commonutil.Required;
import uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser;
import uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.MapperGenerator;
import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.Options;
import java.util.List;
import java.util.Map;

/**
 * An <code>AddElementsFromHdfs</code> operation is for adding {@link uk.gov.gchq.gaffer.data.element.Element}s from HDFS.
 * This operation requires an input, output and failure path.
 * It order to be generic and deal with any type of input file you also need to provide a
 * {@link MapperGenerator} class name and a
 * {@link uk.gov.gchq.gaffer.hdfs.operation.handler.job.initialiser.JobInitialiser}.
 * <p>
 * For normal operation handlers the operation {@link uk.gov.gchq.gaffer.data.elementdefinition.view.View} will be ignored.
 * </p>
 * <b>NOTE</b> - currently this job has to be run as a hadoop job.
 *
 * @see Builder
 */
public class AddElementsFromHdfs implements
        Operation,
        MapReduce,
        Options {
    @Required
    private String failurePath;

    private boolean validate = true;

    /**
     * Used to generate elements from the Hdfs files.
     * For Avro data see {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.AvroMapperGenerator}.
     * For Text data see {@link uk.gov.gchq.gaffer.hdfs.operation.mapper.generator.TextMapperGenerator}.
     */
    @Required
    private String mapperGeneratorClassName;

    @Required
    private List<String> inputPaths;

    @Required
    private String outputPath;

    @Required
    private JobInitialiser jobInitialiser;

    private Integer numMapTasks;
    private Integer numReduceTasks;
    private Class<? extends Partitioner> partitioner;
    private Map<String, String> options;

    public String getFailurePath() {
        return failurePath;
    }

    public void setFailurePath(final String failurePath) {
        this.failurePath = failurePath;
    }

    public boolean isValidate() {
        return validate;
    }

    public void setValidate(final boolean validate) {
        this.validate = validate;
    }

    public String getMapperGeneratorClassName() {
        return mapperGeneratorClassName;
    }

    @JsonSetter(value = "mapperGeneratorClassName")
    public void setMapperGeneratorClassName(final String mapperGeneratorClassName) {
        this.mapperGeneratorClassName = mapperGeneratorClassName;
    }

    public void setMapperGeneratorClassName(final Class<? extends MapperGenerator> mapperGeneratorClass) {
        this.mapperGeneratorClassName = mapperGeneratorClass.getName();
    }

    @Override
    public List<String> getInputPaths() {
        return inputPaths;
    }

    @Override
    public void setInputPaths(final List<String> inputPaths) {
        this.inputPaths = inputPaths;
    }

    @Override
    public String getOutputPath() {
        return outputPath;
    }

    @Override
    public void setOutputPath(final String outputPath) {
        this.outputPath = outputPath;
    }

    @Override
    public JobInitialiser getJobInitialiser() {
        return jobInitialiser;
    }

    @Override
    public void setJobInitialiser(final JobInitialiser jobInitialiser) {
        this.jobInitialiser = jobInitialiser;
    }

    @Override
    public Integer getNumMapTasks() {
        return numMapTasks;
    }

    @Override
    public void setNumMapTasks(final Integer numMapTasks) {
        this.numMapTasks = numMapTasks;
    }

    @Override
    public Integer getNumReduceTasks() {
        return numReduceTasks;
    }

    @Override
    public void setNumReduceTasks(final Integer numReduceTasks) {
        this.numReduceTasks = numReduceTasks;
    }

    @Override
    public Class<? extends Partitioner> getPartitioner() {
        return partitioner;
    }

    @Override
    public void setPartitioner(final Class<? extends Partitioner> partitioner) {
        this.partitioner = partitioner;
    }

    @Override
    public Map<String, String> getOptions() {
        return options;
    }

    @Override
    public void setOptions(final Map<String, String> options) {
        this.options = options;
    }

    public interface IBuilder<OP extends AddElementsFromHdfs, B extends AddElementsFromHdfs.IBuilder<OP, ?>> extends Operation.Builder<OP, B> {
        default B validate(final boolean validate) {
            _getOp().setValidate(validate);
            return _self();
        }

        default B mapperGenerator(final Class<? extends MapperGenerator> mapperGeneratorClass) {
            _getOp().setMapperGeneratorClassName(mapperGeneratorClass);
            return _self();
        }

        default B failurePath(final String failurePath) {
            _getOp().setFailurePath(failurePath);
            return _self();
        }
    }

    public static final class Builder extends Operation.BaseBuilder<AddElementsFromHdfs, Builder>
            implements IBuilder<AddElementsFromHdfs, Builder>,
            MapReduce.Builder<AddElementsFromHdfs, Builder>,
            Options.Builder<AddElementsFromHdfs, Builder> {
        public Builder() {
            super(new AddElementsFromHdfs());
        }
    }
}
