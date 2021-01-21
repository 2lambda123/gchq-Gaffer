/*
 * Copyright 2017-2020 Crown Copyright
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

import uk.gov.gchq.gaffer.commonutil.iterable.ChainedIterable;
import uk.gov.gchq.gaffer.commonutil.iterable.EmptyClosableIterable;
import uk.gov.gchq.gaffer.federatedstore.operation.FederatedOperation;
import uk.gov.gchq.gaffer.federatedstore.util.FederatedStoreUtil;
import uk.gov.gchq.gaffer.operation.OperationException;
import uk.gov.gchq.gaffer.operation.io.Output;
import uk.gov.gchq.gaffer.store.Context;
import uk.gov.gchq.gaffer.store.Store;
import uk.gov.gchq.gaffer.store.operation.handler.OutputOperationHandler;
import uk.gov.gchq.koryphe.binaryoperator.KorypheBinaryOperator;

import static java.util.Objects.nonNull;

/**
 * A handler for GetElements operation for the FederatedStore.
 *
 * @see uk.gov.gchq.gaffer.store.operation.handler.OperationHandler
 * @see uk.gov.gchq.gaffer.federatedstore.FederatedStore
 * @see uk.gov.gchq.gaffer.operation.impl.get.GetElements
 */
public class FederatedOutputIterableHandler<PAYLOAD extends Output<? extends Iterable<? extends O>>, O>
        extends FederationHandler<PAYLOAD, Iterable<? extends O>, PAYLOAD>
        implements OutputOperationHandler<PAYLOAD, Iterable<? extends O>> {


    @Override
    public Iterable<? extends O> doOperation(final PAYLOAD operation, final Context context, final Store store) throws OperationException {

        FederatedOperation fedOp = FederatedStoreUtil.getFederatedOperation(operation);

        O results = new FederatedOperationHandler<PAYLOAD, O>().doOperation(fedOp, context, store);

        operation.setOptions(fedOp.getOptions());

        return nonNull(results) && results instanceof Iterable
                ? new ChainedIterable<O>((Iterable) results)
                : new EmptyClosableIterable<O>();
    }

    @Override
    KorypheBinaryOperator getMergeFunction(final PAYLOAD ignore) {
        throw new IllegalStateException();
    }

    @Override
    PAYLOAD getPayloadOperation(final PAYLOAD operation) {
        throw new IllegalStateException();
    }

    @Override
    String getGraphIdsCsv(final PAYLOAD ignore) {
        throw new IllegalStateException();
    }

}
