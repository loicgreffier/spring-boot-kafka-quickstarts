package io.lgr.quickstarts.streams.reduce.app.reducer;

import io.lgr.quickstarts.avro.KafkaPerson;
import org.apache.kafka.streams.kstream.Reducer;

import java.time.LocalDate;
import java.time.Period;
import java.time.ZoneOffset;

public class MaxAgeReducer implements Reducer<KafkaPerson> {
    @Override
    public KafkaPerson apply(KafkaPerson reduced, KafkaPerson toReduce) {
        int reducedAge = Period.between(LocalDate.ofInstant(reduced.getBirthDate(), ZoneOffset.UTC), LocalDate.now()).getYears();
        int toReduceAge = Period.between(LocalDate.ofInstant(toReduce.getBirthDate(), ZoneOffset.UTC), LocalDate.now()).getYears();
        return toReduceAge > reducedAge ? toReduce : reduced;
    }
}
