package gaffer.accumulostore.operation.impl;


import gaffer.accumulostore.utils.AccumuloTestData;
import gaffer.accumulostore.utils.Pair;
import gaffer.data.element.Edge;
import gaffer.data.elementdefinition.view.View;
import gaffer.exception.SerialisationException;
import gaffer.jsonserialisation.JSONSerialiser;
import gaffer.operation.GetOperation;
import gaffer.operation.OperationTest;
import gaffer.operation.data.EntitySeed;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import static org.junit.Assert.*;

public class GetElementsInRangesTest implements OperationTest {
    private static final JSONSerialiser serialiser = new JSONSerialiser();

    @Test
    @Override
    public void shouldSerialiseAndDeserialiseOperation() throws SerialisationException {
        // Given
        final List<Pair<EntitySeed>> pairList = new ArrayList<>();
        final Pair<EntitySeed> pair1 = new Pair<>(AccumuloTestData.SEED_SOURCE_1, AccumuloTestData.SEED_DESTINATION_1);
        final Pair<EntitySeed> pair2 = new Pair<>(AccumuloTestData.SEED_SOURCE_2, AccumuloTestData.SEED_DESTINATION_2);
        pairList.add(pair1);
        pairList.add(pair2);
        final GetElementsInRanges<Pair<EntitySeed>, Edge> op = new GetElementsInRanges<>(pairList);
        // When
        byte[] json = serialiser.serialise(op, true);

        final GetElementsInRanges<Pair<EntitySeed>, Edge> deserialisedOp = serialiser.deserialise(json, GetElementsInRanges.class);

        // Then
        final Iterator itrPairs = deserialisedOp.getSeeds().iterator();
        assertEquals(pair1, itrPairs.next());
        assertEquals(pair2, itrPairs.next());
        assertFalse(itrPairs.hasNext());

    }

    @Test
    @Override
    public void builderShouldCreatePopulatedOperation() {
        final Pair<EntitySeed> seed = new Pair<>(AccumuloTestData.SEED_A, AccumuloTestData.SEED_B);
        final GetElementsInRanges getElementsInRanges = new GetElementsInRanges.Builder<>()
                .inOutType(GetOperation.IncludeIncomingOutgoingType.BOTH)
                .addSeed(seed)
                .includeEdges(GetOperation.IncludeEdgeType.UNDIRECTED)
                .includeEntities(false)
                .option(AccumuloTestData.TEST_OPTION_PROPERTY_KEY, "true")
                .populateProperties(true)
                .summarise(true)
                .view(new View.Builder().edge("testEdgeGroup").build())
                .build();
        assertEquals("true", getElementsInRanges.getOption(AccumuloTestData.TEST_OPTION_PROPERTY_KEY));
        assertFalse(getElementsInRanges.isIncludeEntities());
        assertEquals(GetOperation.IncludeIncomingOutgoingType.BOTH, getElementsInRanges.getIncludeIncomingOutGoing());
        assertEquals(GetOperation.IncludeEdgeType.UNDIRECTED, getElementsInRanges.getIncludeEdges());
        assertTrue(getElementsInRanges.isPopulateProperties());
        assertTrue(getElementsInRanges.isSummarise());
        assertEquals(seed, getElementsInRanges.getInput().iterator().next());
        assertNotNull(getElementsInRanges.getView());
    }
}
