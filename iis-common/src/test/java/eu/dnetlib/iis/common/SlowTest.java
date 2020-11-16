package eu.dnetlib.iis.common;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.*;

/**
 * Slow test markup.
 * <p>
 * Slow tests are unit tests with long execution time.
 */
@Inherited
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Tag("SlowTest")
public @interface SlowTest {
}
