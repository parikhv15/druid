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
import com.google.common.collect.ImmutableMap;
import io.druid.jackson.DefaultObjectMapper;
import io.druid.query.aggregation.PostAggregator;
import io.druid.query.aggregation.post.ConstantPostAggregator;
import org.junit.Assert;
import org.junit.Test;

public class ProbabilityOfBeatingPostAggregatorTest
{

  private ProbabiltyOfBeatingPostAggregator probabiltyOfBeatingPostAggregator;
  private PostAggregator testSuccess;
  private PostAggregator testFailure;
  private PostAggregator controlSuccess;
  private PostAggregator controlFailure;

  @Test
  public void testProbabilityOfBeating()
  {
    controlSuccess = new ConstantPostAggregator("control_success", 12);
    controlFailure = new ConstantPostAggregator("control_failure", 8);
    testSuccess = new ConstantPostAggregator("test_success", 10);
    testFailure = new ConstantPostAggregator("test_failure", 56);
    probabiltyOfBeatingPostAggregator = new ProbabiltyOfBeatingPostAggregator("pob", controlSuccess,
                                                                              controlFailure, testSuccess,
                                                                              testFailure
    );
    double result = ((Number) probabiltyOfBeatingPostAggregator.compute(ImmutableMap.of(
        "control_success", 12,
        "control_failure", 8,
        "test_success", 10,
        "test_failure", 56
    ))).doubleValue();
    Assert.assertTrue(result == 0.9999358462472524);
  }

  @Test
  public void testProbabilityOfBeatingStatSignificance()
  {
    controlSuccess = new ConstantPostAggregator("control_success", 2);
    controlFailure = new ConstantPostAggregator("control_failure", 902);
    testSuccess = new ConstantPostAggregator("test_success", 1);
    testFailure = new ConstantPostAggregator("test_failure", 23);
    probabiltyOfBeatingPostAggregator = new ProbabiltyOfBeatingPostAggregator("pob", controlSuccess,
                                                                              controlFailure, testSuccess,
                                                                              testFailure
    );
    double result = ((Number) probabiltyOfBeatingPostAggregator.compute(ImmutableMap.of(
        "control_success", 2,
        "control_failure", 902,
        "test_success", 1,
        "test_failure", 23
    ))).doubleValue();
    System.out.println(result);
    Assert.assertTrue(result == 0.004030340448577963);
  }

  @Test
  public void testSerDe() throws Exception
  {
    DefaultObjectMapper mapper = new DefaultObjectMapper();
    ProbabiltyOfBeatingPostAggregator postAggregator1 = mapper.readValue(mapper.writeValueAsString(
        probabiltyOfBeatingPostAggregator), ProbabiltyOfBeatingPostAggregator.class);

    Assert.assertEquals(probabiltyOfBeatingPostAggregator, postAggregator1);
  }

  @Test
  public void testAb()
  {
    double result = new Experiment(12, 8)
        .probabilityOfBeating(new Experiment(10, 56));
    System.out.println(result);
    assert (result == 0.9999358462472524);
  }
}
