package com.softwaremill.react.kafka;


import org.testng.annotations.Test;

import static junit.framework.Assert.assertNotNull;

public class JavaConstructorTest {

    @Test
    public void canConstructReactiveKafkaWithoutDefaultArgs() {
        final ReactiveKafka reactiveKafka = new ReactiveKafka();
        assertNotNull(reactiveKafka);
    }

}
