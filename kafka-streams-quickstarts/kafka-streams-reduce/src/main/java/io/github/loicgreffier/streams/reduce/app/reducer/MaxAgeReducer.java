package io.github.loicgreffier.streams.reduce.app.reducer;

import io.github.loicgreffier.avro.KafkaPerson;
import java.time.LocalDate;
import java.time.ZoneOffset;
import org.apache.kafka.streams.kstream.Reducer;

/**
 * This class represents a reducer that reduces the input values to the one with the maximum age.
 */
public class MaxAgeReducer implements Reducer<KafkaPerson> {
    /**
     * Reduces the input values to the one with the maximum age.
     * The age is calculated from the birth date.
     *
     * @param reduced  The reduced value.
     * @param toReduce The value to reduce.
     * @return The reduced value.
     */
    @Override
    public KafkaPerson apply(KafkaPerson reduced, KafkaPerson toReduce) {
        LocalDate reducedBirthDate = LocalDate.ofInstant(reduced.getBirthDate(), ZoneOffset.UTC);
        LocalDate toReduceBirthDate = LocalDate.ofInstant(toReduce.getBirthDate(), ZoneOffset.UTC);
        return toReduceBirthDate.isBefore(reducedBirthDate) ? toReduce : reduced;
    }
}
