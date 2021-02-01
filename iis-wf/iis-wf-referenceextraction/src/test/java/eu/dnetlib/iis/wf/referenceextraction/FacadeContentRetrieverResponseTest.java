package eu.dnetlib.iis.wf.referenceextraction;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

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
}