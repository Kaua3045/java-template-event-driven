package com.kaua.event.driven;

import com.kaua.event.driven.infrastructure.es.eventstore.outbox.jpa.JpaOutboxStoreEngine;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.springframework.boot.test.autoconfigure.orm.jpa.DataJpaTest;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.context.annotation.FilterType;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.ActiveProfiles;

import java.lang.annotation.*;

@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Inherited
@ActiveProfiles("test-integration")
@ComponentScan(
        basePackages = "com.kaua.event.driven",
        useDefaultFilters = false,
        includeFilters = {
                @ComponentScan.Filter(type = FilterType.REGEX, pattern = ".*JpaRepository")
        }
)
@DataJpaTest
@ExtendWith(JpaCleanUpExtension.class)
@Import({JpaOutboxStoreEngine.class})
@Tag("integrationTest")
public @interface DatabaseGatewayTest {
}
