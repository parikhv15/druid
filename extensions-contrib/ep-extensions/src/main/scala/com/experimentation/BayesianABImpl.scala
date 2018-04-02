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

package com.experimentation

import breeze.numerics._

case class BernoulliTrial(outcome: Boolean)

object Experiment {
  def apply(successes: Int, failures: Int) = new Experiment(successes, failures)
}

case class Experiment(trials: Seq[BernoulliTrial]) {

  def this(successes: Int, failures: Int) = {
    this(
      Seq.fill(successes)(BernoulliTrial(true)) ++
        Seq.fill(failures)(BernoulliTrial(false))
    )
  }

  def alpha = 1 + trials.count(_.outcome == true).toDouble

  def beta = 1 + trials.count(_.outcome == false).toDouble

  def probabilityOfBeating(otherExperiment: Experiment) = {
    (0 until alpha.toInt).map { i =>
      math.exp(
        lbeta(Array(otherExperiment.alpha + i, otherExperiment.beta + beta))
          - log(beta + i)
          - lbeta(Array(1 + i, beta))
          - lbeta(Array(otherExperiment.alpha, otherExperiment.beta))
      )
    }.sum
  }
}
