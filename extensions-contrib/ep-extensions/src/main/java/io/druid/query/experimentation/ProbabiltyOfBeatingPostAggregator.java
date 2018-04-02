/*
 * Licensed to Metamarkets Group Inc. (Metamarkets) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. Metamarkets licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package io.druid.query.experimentation;

import com.experimentation.Experiment;
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

import java.util.Collections;
import java.util.Comparator;
import java.util.Map;
import java.util.Set;

public class ProbabiltyOfBeatingPostAggregator implements PostAggregator
{

  private final String name;
  private final PostAggregator testSuccess;
  private final PostAggregator testFailure;
  private final PostAggregator controlSuccess;
  private final PostAggregator controlFailure;

  @JsonCreator
  public ProbabiltyOfBeatingPostAggregator(
      @JsonProperty("name") String name,
      @JsonProperty("controlSuccess") PostAggregator controlSuccess,
      @JsonProperty("controlFailure") PostAggregator controlFailure,
      @JsonProperty("testSuccess") PostAggregator testSuccess,
      @JsonProperty("testFailure") PostAggregator testFailure
  )
  {
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
  public Set<String> getDependentFields()
  {
    Set<String> dependentFields = Sets.newLinkedHashSet();
    dependentFields.addAll(testSuccess.getDependentFields());
    dependentFields.addAll(testFailure.getDependentFields());
    dependentFields.addAll(controlSuccess.getDependentFields());
    dependentFields.addAll(controlFailure.getDependentFields());

    return dependentFields;
  }

  @Override
  public Comparator getComparator()
  {
    return ArithmeticPostAggregator.DEFAULT_COMPARATOR;
  }

  @Override
  public Object compute(Map<String, Object> combinedAggregators)
  {
    return probabilityOfBeating(
        ((Number) controlSuccess.compute(combinedAggregators)).intValue(),
        ((Number) controlFailure.compute(combinedAggregators)).intValue(),
        ((Number) testSuccess.compute(combinedAggregators)).intValue(),
        ((Number) testFailure.compute(combinedAggregators)).intValue()
    );
  }

  @Override
  @JsonProperty
  public String getName()
  {
    return name;
  }

  @Override
  public ProbabiltyOfBeatingPostAggregator decorate(Map<String, AggregatorFactory> aggregators)
  {
    return new ProbabiltyOfBeatingPostAggregator(
        name,
        Iterables
            .getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(testSuccess), aggregators)),
        Iterables.getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(testFailure), aggregators)),
        Iterables
            .getOnlyElement(Queries.decoratePostAggregators(Collections.singletonList(controlSuccess), aggregators)),
        Iterables.getOnlyElement(Queries.decoratePostAggregators(
            Collections.singletonList(controlFailure),
            aggregators
        ))
    );
  }


  private double probabilityOfBeating(
      int controlSuccessCnt,
      int controlFailureCnt,
      int testSuccessCnt,
      int testFailureCnt
  )
  {
    return new Experiment(controlSuccessCnt, controlFailureCnt)
        .probabilityOfBeating(new Experiment(testSuccessCnt, testFailureCnt));
  }

  @Override
  public byte[] getCacheKey()
  {
    return new CacheKeyBuilder(
        PostAggregatorIds.ZTEST)
        .appendCacheable(testSuccess)
        .appendCacheable(testFailure)
        .appendCacheable(controlSuccess)
        .appendCacheable(controlFailure)
        .build();
  }
}
