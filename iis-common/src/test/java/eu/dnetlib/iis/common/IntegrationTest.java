package eu.dnetlib.iis.common;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.*;

/**
 * Integration test markup.
 * <p>
 * Integration tests are tests to be run using cluster environment.
 */
@Inherited
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Tag("IntegrationTest")
public @interface IntegrationTest {
}
