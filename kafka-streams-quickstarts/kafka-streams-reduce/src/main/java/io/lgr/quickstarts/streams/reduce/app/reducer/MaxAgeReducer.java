package io.lgr.quickstarts.streams.reduce.app.reducer;

import io.lgr.quickstarts.avro.KafkaPerson;
import org.apache.kafka.streams.kstream.Reducer;

import java.time.LocalDate;
import java.time.ZoneOffset;

public class MaxAgeReducer implements Reducer<KafkaPerson> {
    @Override
    public KafkaPerson apply(KafkaPerson reduced, KafkaPerson toReduce) {
        LocalDate reducedAge = LocalDate.ofInstant(reduced.getBirthDate(), ZoneOffset.UTC);
        LocalDate toReduceAge = LocalDate.ofInstant(toReduce.getBirthDate(), ZoneOffset.UTC);
        return toReduceAge.isBefore(reducedAge) ? toReduce : reduced;
    }
}
