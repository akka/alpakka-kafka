/*
 * Copyright (C) 2014 - 2016 Softwaremill <https://softwaremill.com>
 * Copyright (C) 2016 - 2025 Lightbend Inc. <https://akka.io>
 */

package docs.javadsl;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Objects;

public class SampleData {

  public final String name;
  public final int value;

  @JsonCreator
  public SampleData(@JsonProperty("name") String name, @JsonProperty("value") int value) {
    this.name = name;
    this.value = value;
  }

  @Override
  public String toString() {
    return "SampleData(name=" + name + ",value=" + value + ")";
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (o == null || getClass() != o.getClass()) return false;
    SampleData that = (SampleData) o;
    return value == that.value && Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, value);
  }
}
