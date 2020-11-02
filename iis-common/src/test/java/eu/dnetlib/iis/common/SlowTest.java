package eu.dnetlib.iis.common;

import org.junit.jupiter.api.Tag;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * Slow test markup.
 * <p>
 * Slow tests are unit tests with long execution time.
 */
@Target({ElementType.TYPE})
@Retention(RetentionPolicy.RUNTIME)
@Tag("SlowTest")
public @interface SlowTest {
}
