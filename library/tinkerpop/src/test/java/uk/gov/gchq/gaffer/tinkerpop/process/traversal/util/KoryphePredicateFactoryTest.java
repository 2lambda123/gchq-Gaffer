/*
 * Copyright 2024 Crown Copyright
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package uk.gov.gchq.gaffer.tinkerpop.process.traversal.util;

import org.apache.tinkerpop.gremlin.process.traversal.P;
import org.apache.tinkerpop.gremlin.process.traversal.PBiPredicate;
import org.apache.tinkerpop.gremlin.process.traversal.TextP;
import org.junit.jupiter.api.Test;

import uk.gov.gchq.koryphe.impl.predicate.And;
import uk.gov.gchq.koryphe.impl.predicate.IsEqual;
import uk.gov.gchq.koryphe.impl.predicate.IsIn;
import uk.gov.gchq.koryphe.impl.predicate.IsLessThan;
import uk.gov.gchq.koryphe.impl.predicate.IsMoreThan;
import uk.gov.gchq.koryphe.impl.predicate.Not;
import uk.gov.gchq.koryphe.impl.predicate.Or;
import uk.gov.gchq.koryphe.impl.predicate.Regex;
import uk.gov.gchq.koryphe.impl.predicate.StringContains;

import java.util.Arrays;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;

class KoryphePredicateFactoryTest {

  @Test
  void shouldReturnIsEqual() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.eq(20)))
      .isEqualTo(new IsEqual(20));
  }

  @Test
  void shouldReturnNotIsEqual() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.neq(20)))
      .isEqualTo(new Not<Object>(new IsEqual(20)));
  }

  @Test
  void shouldReturnIsMoreThan() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.gt(20)))
      .isEqualTo(new IsMoreThan(20, false));
  }

  @Test
  void shouldReturnIsMoreThanOrEqualTo() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.gte(20)))
      .isEqualTo(new IsMoreThan(20, true));
  }

  @Test
  void shouldReturnIsLessThan() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.lt(20)))
      .isEqualTo(new IsLessThan(20, false));
  }

  @Test
  void shouldReturnIsLessThanOrEqualTo() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.lte(20)))
      .isEqualTo(new IsLessThan(20, true));
  }

  @Test
  void shouldReturnAndInside() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.inside(20, 30)))
      .isEqualTo(new And<>(Arrays.asList(new IsMoreThan(20, false),
                                         new IsLessThan(30, false))));
  }

  @Test
  void shouldReturnAndBetween() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.between(20, 30)))
      .isEqualTo(new And<>(Arrays.asList(new IsMoreThan(20, true),
                                         new IsLessThan(30, false))));
  }

  @Test
  void shouldReturnOrOutside() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.outside(20, 30)))
      .isEqualTo(new Or<>(Arrays.asList(new IsLessThan(20, false),
                                         new IsMoreThan(30, false))));
  }

  @Test
  void shouldReturnIsIn() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.within("marko", "josh")))
      .isEqualTo(new IsIn("marko", "josh"));
  }

  @Test
  void shouldReturnIsNotIn() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(P.without("marko", "josh")))
      .isEqualTo(new Not<Object>(new IsIn("marko", "josh")));
  }

  @Test
  void shouldReturnRegexStartingWith() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.startingWith("m")))
      .isEqualTo(new Regex("^m.*"));
  }

  @Test
  void shouldReturnRegexNotStartingWith() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.notStartingWith("m")))
      .isEqualTo(new Regex("^(?!m).*"));
  }

  @Test
  void shouldReturnRegexEndingWith() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.endingWith("o")))
      .isEqualTo(new Regex(".*o$"));
  }

  @Test
  void shouldReturnRegexNotEndingWith() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.notEndingWith("o")))
      .isEqualTo(new Regex(".*(?<!o)$"));
  }

  @Test
  void shouldReturnStringContains() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.containing("m")))
      .isEqualTo(new StringContains("m"));
  }

  @Test
  void shouldReturnNotStringContains() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.notContaining("m")))
      .isEqualTo(new Not<>(new StringContains("m")));
  }

  @Test
  void shouldReturnRegex() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.regex("(m|j).*")))
      .isEqualTo(new Regex("(m|j).*"));
  }

  @Test
  void shouldReturnNotRegex() {
    assertThat(KoryphePredicateFactory.getKoryphePredicate(TextP.notRegex("(m|j).*")))
      .isEqualTo(new Not<>(new Regex("(m|j).*")));
  }

  @Test
  void shouldThrowWhenPredicateIsNull() {
    assertThatExceptionOfType(IllegalArgumentException.class)
    .isThrownBy(() -> KoryphePredicateFactory.getKoryphePredicate(null))
    .withMessage("Could not translate Gremlin predicate: null");
  }

  @Test
  void shouldThrowWhenPredicateIsUnknown() {
    P<?> unknownPredicate = UnknownP.unknown(10);
    assertThatExceptionOfType(IllegalArgumentException.class)
    .isThrownBy(() -> KoryphePredicateFactory.getKoryphePredicate(unknownPredicate))
    .withMessageContaining("Could not translate Gremlin predicate");
  }

}

// Test classes only
class UnknownP<V> extends P<V> {

  UnknownP(PBiPredicate<V, V> biPredicate, V value) {
    super(biPredicate, value);
  }

  public static <V> P<V> unknown(final V value) {
    return new P<V>((PBiPredicate<V, V>) new UnknownPBiPredicate<V, V>(), value);
  }

}

class UnknownPBiPredicate<T, U> implements PBiPredicate<T, U> {

  @Override
  public boolean test(T t, U u) {
    throw new UnsupportedOperationException("Unimplemented method 'test'");
  }

}

