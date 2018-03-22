package io.druid.query.experimentation;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.base.Preconditions;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import io.druid.query.Queries;
import io.druid.query.aggregation.AggregatorFactory;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ArithmeticPostAggregator;
import io.druid.query.aggregation.post.PostAggregatorIds;
import io.druid.query.cache.CacheKeyBuilder;

import java.util.*;

public class ProbabiltyOfBeatingPostAggregator implements PostAggregator {

    private final String name;
    private final PostAggregator testSuccess;
    private final PostAggregator testFailure;
    private final PostAggregator controlSuccess;
    private final PostAggregator controlFailure;

    @JsonCreator
    public ProbabiltyOfBeatingPostAggregator(@JsonProperty("name") String name,
                                             @JsonProperty("testSuccess") PostAggregator testSuccess,
                                             @JsonProperty("testFailure") PostAggregator testFailure,
                                             @JsonProperty("controlSuccess") PostAggregator controlSuccess,
                                             @JsonProperty("controlFailure") PostAggregator controlFailure
    ) {
        Preconditions.checkNotNull(name, "Must have a valid, non-null post-aggregator name");
        Preconditions.checkNotNull(testSuccess, "success count for test can not be null");
        Preconditions.checkNotNull(testFailure, "failure count for test can not null");
        Preconditions.checkNotNull(controlSuccess, "success count for control can not be null");
        Preconditions.checkNotNull(controlFailure, "failure count for control can not null");

        this.name = name;
        this.testSuccess = testSuccess;
        this.testFailure = testFailure;
        this.controlSuccess = controlSuccess;
        this.controlFailure = controlFailure;
    }

    @Override
    public Set<String> getDependentFields() {
        Set<String> dependentFields = Sets.newLinkedHashSet();
        dependentFields.addAll(testSuccess.getDependentFields());
        dependentFields.addAll(testFailure.getDependentFields());
        dependentFields.addAll(controlSuccess.getDependentFields());
        dependentFields.addAll(controlFailure.getDependentFields());

        return dependentFields;
    }

    @Override
    public Comparator getComparator() {
        return ArithmeticPostAggregator.DEFAULT_COMPARATOR;
    }

    @Override
    public Object compute(Map<String, Object> combinedAggregators) {
        return probabilityOfBeating(
                ((Number) testSuccess.compute(combinedAggregators)).doubleValue(),
                ((Number) testFailure.compute(combinedAggregators)).doubleValue(),
                ((Number) controlSuccess.compute(combinedAggregators)).doubleValue(),
                ((Number) controlFailure.compute(combinedAggregators)).doubleValue()
        );
    }

    @Override
    @JsonProperty
    public String getName() {
        return name;
    }

    @Override
    public ProbabiltyOfBeatingPostAggregator decorate(Map<String, AggregatorFactory> aggregators) {
        return new ProbabiltyOfBeatingPostAggregator(
                name,
                Iterables
                        .getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(testSuccess), aggregators)),
                Iterables.getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(testFailure), aggregators)),
                Iterables
                        .getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(controlSuccess), aggregators)),
                Iterables.getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(controlFailure), aggregators))
        );
    }


    private double probabilityOfBeating(double testSuccessCnt, double testFailureCnt, double controlSuccessCnt, double controlFailureCnt) {
        //TODO extract the aggregator values and pass it to the below module and return the result
//        val test = new Experiment(10,90)
//        val control = new Experiment(7,93)
//        return test.probabilityOfBeating(control)
        return 0;
    }

    @Override
    public byte[] getCacheKey() {
        return new CacheKeyBuilder(
                PostAggregatorIds.ZTEST)
                .appendCacheable(testSuccess)
                .appendCacheable(testFailure)
                .appendCacheable(controlSuccess)
                .appendCacheable(controlFailure)
                .build();
    }
}
