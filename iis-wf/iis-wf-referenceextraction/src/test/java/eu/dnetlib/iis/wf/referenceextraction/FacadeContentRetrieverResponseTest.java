package eu.dnetlib.iis.wf.referenceextraction;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class FacadeContentRetrieverResponseTest {

    @Test
    @DisplayName("Success wraps an object")
    public void givenSuccessfulResponse_thenContentCanBeRetrieved() {
        assertEquals(FacadeContentRetrieverResponse.Success.class,
                FacadeContentRetrieverResponse.success("success").getClass());
        assertEquals("success", FacadeContentRetrieverResponse.success("success").getContent());
        assertThrows(RuntimeException.class, FacadeContentRetrieverResponse.success("success")::getException);
    }

    @Test
    @DisplayName("Persistent failure wraps an exception")
    public void givenPersistentFailure_thenExceptionCanBeRetrieved() {
        Exception exception = new Exception("failed");
        assertEquals(FacadeContentRetrieverResponse.PersistentFailure.class,
                FacadeContentRetrieverResponse.persistentFailure(exception).getClass());
        assertThrows(RuntimeException.class, FacadeContentRetrieverResponse.persistentFailure(exception)::getContent);
        assertEquals(exception, FacadeContentRetrieverResponse.persistentFailure(exception).getException());
    }

    @Test
    @DisplayName("Transient failure wraps an exception")
    public void givenTransientFailure_thenExceptionCanBeRetrieved() {
        Exception exception = new Exception("failed");
        assertEquals(FacadeContentRetrieverResponse.TransientFailure.class,
                FacadeContentRetrieverResponse.transientFailure(exception).getClass());
        assertThrows(RuntimeException.class, FacadeContentRetrieverResponse.transientFailure(exception)::getContent);
        assertEquals(exception, FacadeContentRetrieverResponse.transientFailure(exception).getException());
    }

    @Nested
    public class IsSuccessTest {

        @Test
        @DisplayName("Success is matched by isSuccess")
        public void givenASuccessResponse_whenMatched_thenTrueIsReturned() {
            assertTrue(FacadeContentRetrieverResponse.isSuccess(FacadeContentRetrieverResponse
                    .success("any content")));
        }

        @Test
        @DisplayName("Persistent failure is not matched by isSuccess")
        public void givenAPersistentFailureResponse_whenMatched_thenFalseIsReturned() {
            assertFalse(FacadeContentRetrieverResponse.isSuccess(FacadeContentRetrieverResponse
                    .persistentFailure(new Exception())));
        }

        @Test
        @DisplayName("Transient failure is not matched by isSuccess")
        public void givenATransientFailureResponse_whenMatched_thenFalseIsReturned() {
            assertFalse(FacadeContentRetrieverResponse.isSuccess(FacadeContentRetrieverResponse
                    .transientFailure(new Exception())));
        }
    }

    @Nested
    public class IsFailureTest {
        @Test
        @DisplayName("Success is not matched by isFailure")
        public void givenASuccessResponse_whenMatched_thenFalseIsReturned() {
            assertFalse(FacadeContentRetrieverResponse.isFailure(FacadeContentRetrieverResponse
                    .success("any content")));
        }

        @Test
        @DisplayName("Persistent failure is matched by isFailure")
        public void givenAPersistentFailureResponse_whenMatched_thenTrueIsReturned() {
            assertTrue(FacadeContentRetrieverResponse.isFailure(FacadeContentRetrieverResponse
                    .persistentFailure(new Exception())));
        }

        @Test
        @DisplayName("Transient failure is matched by isFailure")
        public void givenATransientFailureResponse_whenMatched_thenTrueIsReturned() {
            assertTrue(FacadeContentRetrieverResponse.isFailure(FacadeContentRetrieverResponse
                    .transientFailure(new Exception())));
        }
    }
}