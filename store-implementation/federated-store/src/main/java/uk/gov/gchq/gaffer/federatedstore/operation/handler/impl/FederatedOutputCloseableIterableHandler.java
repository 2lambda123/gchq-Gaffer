/*
 * Copyright 2017-2021 Crown Copyright
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

package uk.gov.gchq.gaffer.federatedstore.operation.handler.impl;

import uk.gov.gchq.gaffer.commonutil.iterable.EmptyIterable;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.InputOutput;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;

import static java.util.Objects.isNull;
import static uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil.getFederatedOperation;

/**
 * Handler for the federation of an PAYLOAD operation with an expected return type CloseableIterable/<ITERABLE_ELEMENTS/>
 *
 * @param <PAYLOAD>           The operation to be federated and executed by delegate graphs.
 * @param <ITERABLE_ELEMENTS> the type of elements returned by the Output Iterable
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class FederatedOutputCloseableIterableHandler<PAYLOAD extends Output<? extends Iterable<? extends ITERABLE_ELEMENTS>>, ITERABLE_ELEMENTS>
        implements OutputOperationHandler<PAYLOAD, Iterable<? extends ITERABLE_ELEMENTS>> {

    @Override
    public Iterable<? extends ITERABLE_ELEMENTS> doOperation(final PAYLOAD operation, final Context context, final Store store) throws OperationException {

        Iterable<? extends ITERABLE_ELEMENTS> results;

        FederatedOperation federatedOperation = getFederatedOperation(operation instanceof InputOutput ? (InputOutput) operation : (Output) operation);
        Object execute = store.execute(federatedOperation, context);
        try {
            results = (Iterable<? extends ITERABLE_ELEMENTS>) execute;
        } catch (final ClassCastException e) {
            throw new OperationException(String.format("Could not cast execution result. Expected:%s Found:%s", Iterable.class, execute.getClass().toString()), e);
        }
        operation.setOptions(federatedOperation.getOptions());

        return isNull(results) ? new EmptyIterable<>() : results;
    }
}
