/*
 * Copyright 2017-2018 Crown Copyright
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
package uk.gov.gchq.gaffer.store.operation.handler;

import uk.gov.gchq.gaffer.operation.Operation;
import uk.gov.gchq.gaffer.operation.OperationChain;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Input;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.OperationValidation;
import uk.gov.gchq.gaffer.store.operation.OperationValidator;
import uk.gov.gchq.gaffer.store.optimiser.OperationChainOptimiser;
import uk.gov.gchq.gaffer.store.schema.ViewValidator;
import uk.gov.gchq.koryphe.ValidationResult;

import java.util.ArrayList;
import java.util.List;

/**
 * A {@code OperationChainHandler} handles {@link OperationChain}s.
 *
 * @param <OUT> the output type of the operation chain
 */
public class OperationChainHandler<OUT> implements OutputOperationHandler<OperationChain<OUT>, OUT>, OperationValidation<OperationChain<OUT>> {
    private OperationValidator opValidator =
            new OperationValidator(new ViewValidator());
    private List<OperationChainOptimiser> opChainOptimisers = new ArrayList<>();

    @Override
    public OUT doOperation(final OperationChain<OUT> operationChain, final Context context, final Store store) throws OperationException {
        Object result = null;
        for (final Operation op : operationChain.getOperations()) {
            updateOperationInput(op, result);
            result = store.handleOperation(op, context);
        }

        return (OUT) result;
    }

    @Override
    public OperationChain<OUT> prepareOperation(final OperationChain<OUT> operation,
                                                final Context context, final Store store) {
        final ValidationResult validationResult = opValidator.validate(operation, context.getUser(), store);
        if (!validationResult.isValid()) {
            throw new IllegalArgumentException("Operation chain is invalid. " + validationResult
                    .getErrorString());
        }

        OperationChain<OUT> optimisedOperationChain = operation;
        for (final OperationChainOptimiser opChainOptimiser : opChainOptimisers) {
            optimisedOperationChain = opChainOptimiser.optimise(optimisedOperationChain);
        }

        return optimisedOperationChain;
    }

    protected void updateOperationInput(final Operation op, final Object result) {
        if (null != result) {
            if (op instanceof OperationChain) {
                if (!((OperationChain) op).getOperations().isEmpty()) {
                    final Operation firstOp = (Operation) ((OperationChain) op).getOperations()
                            .get(0);
                    if (firstOp instanceof Input) {
                        setOperationInput(firstOp, result);
                    }
                }
            } else if (op instanceof Input) {
                setOperationInput(op, result);
            }
        }
    }

    public OperationChainHandler() {

    }

    /**
     * @param opValidator       the OperationValidator instance
     * @param opChainOptimisers A list of OperationChainOptimisers
     * @deprecated OperationChainValidator and OpChainOptimisers are now
     * supplied by the OperationChainHandler class.  These should not be
     * supplied and if the validation should be different
     * OperationChainHandler should be extended and the prepareOperation
     * method can be overridden.
     */
    public OperationChainHandler(final OperationValidator opValidator, final List<OperationChainOptimiser> opChainOptimisers) {
        this.opValidator = opValidator;
        this.opChainOptimisers = opChainOptimisers;
    }

    private void setOperationInput(final Operation op, final Object result) {
        if (null == ((Input) op).getInput()) {
            ((Input) op).setInput(result);
        }
    }

    /**
     * @return the OperationValidator instance
     * @see OperationChainHandler constructor doc
     * @deprecated This field should not be used.  It will be supplied within
     * the OperationChainHandlerClass.
     */
    protected OperationValidator getOpChainValidator() {
        return opValidator;
    }

    /**
     * @return a list of OperationChainOptimisers
     * @see OperationChainHandler constructor doc
     * @deprecated This field should not be used.  It will be supplied within
     * the OperationChainHandlerClass.
     */
    protected List<OperationChainOptimiser> getOpChainOptimisers() {
        return opChainOptimisers;
    }
}
