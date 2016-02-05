package org.qcri.rheem.basic.operators;

import org.apache.commons.lang3.Validate;
import org.qcri.rheem.core.api.Configuration;
import org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimate;
import org.qcri.rheem.core.plan.UnaryToUnaryOperator;
import org.qcri.rheem.core.types.DataSetType;

import java.util.Optional;
import java.util.function.Predicate;


/**
 * This operator returns a new dataset after filtering by applying predicate.
 */
public class FilterOperator<Type> extends UnaryToUnaryOperator<Type, Type> {

    /**
     * Function that this operator applies to the input elements.
     */
    protected final Predicate<Type> predicate;

    /**
     * Creates a new instance.
     *
     * @param type type of the dataunit elements
     */
    public FilterOperator(DataSetType<Type> type, Predicate<Type> predicate) {

        super(type, type, null);
        this.predicate = predicate;
    }

    public Predicate<Type> getFunctionDescriptor() {
        return predicate;
    }

    @Override
    public Optional<org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator> getCardinalityEstimator(
            final int outputIndex,
            final Configuration configuration) {
        Validate.inclusiveBetween(0, this.getNumOutputs() - 1, outputIndex);
        return Optional.of(new FilterOperator.CardinalityEstimator());
    }

    /**
     * Custom {@link org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator} for {@link FilterOperator}s.
     */
    private class CardinalityEstimator implements org.qcri.rheem.core.optimizer.cardinality.CardinalityEstimator {

        public static final double DEFAULT_SELECTIVITY_CORRECTNESS = 0.9;

        @Override
        public CardinalityEstimate estimate(Configuration configuration, CardinalityEstimate... inputEstimates) {
            Validate.isTrue(inputEstimates.length == FilterOperator.this.getNumInputs());
            final CardinalityEstimate inputEstimate = inputEstimates[0];

            final Optional<Double> selectivity = configuration.getPredicateSelectivityProvider()
                    .optionallyProvideFor(FilterOperator.this.predicate.getClass());
            if (selectivity.isPresent()) {
                return new CardinalityEstimate(
                        (long) (inputEstimate.getLowerEstimate() * selectivity.get()),
                        (long) (inputEstimate.getUpperEstimate() * selectivity.get()),
                        inputEstimate.getCorrectnessProbability() * DEFAULT_SELECTIVITY_CORRECTNESS
                );
            } else {
                return new CardinalityEstimate(
                        0l,
                        inputEstimate.getUpperEstimate(),
                        inputEstimate.getCorrectnessProbability()
                );
            }
        }
    }
}
