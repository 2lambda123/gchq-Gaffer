package uk.gov.gchq.gaffer.time.function;

import org.junit.Before;
import org.junit.Test;
import uk.gov.gchq.gaffer.commonutil.CommonTimeUtil;
import uk.gov.gchq.gaffer.jsonserialisation.JSONSerialiser;
import uk.gov.gchq.gaffer.time.RBMBackedTimestampSet;
import uk.gov.gchq.koryphe.function.FunctionTest;

import java.io.IOException;
import java.time.Duration;
import java.time.Instant;
import java.util.function.Function;

import static org.junit.Assert.assertEquals;

public class MaskTimestampSetByTimeRangeTest extends FunctionTest {

    private Instant instant;
    private MaskTimestampSetByTimeRange maskTimestampSetByTimeRange = new MaskTimestampSetByTimeRange();

    @Before
    public void setup() {
        instant = Instant.now();
    }

    @Test
    public void filtersTimestampSet() {
        final RBMBackedTimestampSet timestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        timestampSet.add(instant);
        timestampSet.add(instant.plus(Duration.ofDays(100L)));
        timestampSet.add(instant.plus(Duration.ofDays(200L)));
        timestampSet.add(instant.plus(Duration.ofDays(300L)));

        maskTimestampSetByTimeRange.setTimeRangeStartEpochMilli(instant.plus(Duration.ofDays(100)).toEpochMilli());
        maskTimestampSetByTimeRange.setTimeRangeEndEpochMilli(instant.plus(Duration.ofDays(250)).toEpochMilli());

        final RBMBackedTimestampSet expectedTimestampSet = new RBMBackedTimestampSet(CommonTimeUtil.TimeBucket.MINUTE);
        expectedTimestampSet.add(instant.plus(Duration.ofDays(100L)));
        expectedTimestampSet.add(instant.plus(Duration.ofDays(200L)));

        final RBMBackedTimestampSet actualTimestampSet = maskTimestampSetByTimeRange.apply(timestampSet);

        assertEquals(expectedTimestampSet, actualTimestampSet);
    }

    @Override
    protected Function getInstance() {
        return maskTimestampSetByTimeRange;
    }

    @Override
    protected Class<? extends Function> getFunctionClass() {
        return MaskTimestampSetByTimeRange.class;
    }

    @Override
    public void shouldJsonSerialiseAndDeserialise() throws IOException {
        // Given
        final MaskTimestampSetByTimeRange maskTimestampSetByTimeRange = new MaskTimestampSetByTimeRange(1l,2l);

        // When
        final String json = new String(JSONSerialiser.serialise(maskTimestampSetByTimeRange));
        MaskTimestampSetByTimeRange deserialisedMaskTimestampSetByTimeRange = JSONSerialiser.deserialise(json, MaskTimestampSetByTimeRange.class);

        // Then
        assertEquals(maskTimestampSetByTimeRange, deserialisedMaskTimestampSetByTimeRange);
        assertEquals("{\"class\":\"uk.gov.gchq.gaffer.time.function.MaskTimestampSetByTimeRange\",\"timeRangeStartEpochMilli\":1,\"timeRangeEndEpochMilli\":2}"
                , json);
    }
}