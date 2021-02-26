package eu.dnetlib.iis.wf.referenceextraction;

import java.io.Serializable;

/**
 * Wraps service facade response. Successful response wraps content retrieved from the service, failure wraps exception.
 * Failure can be persistent or transient. Service facade implementation must decide which failure to return upon service
 * response.
 *
 * @param <C> Type of content wrapped by a Success.
 */
public abstract class FacadeContentRetrieverResponse<C> implements Serializable {

    public abstract C getContent();

    public abstract Exception getException();

    /**
     * Creates a wrapper for successful response from the service.
     *
     * @param content Content to be wrapped.
     */
    public static <C> Success<C> success(C content) {
        return new Success<>(content);
    }

    /**
     * Creates a wrapper for persistent failure response from the service.
     *
     * @param exception Exception to be wrapped.
     */
    public static <C> Failure<C> persistentFailure(Exception exception) {
        return new PersistentFailure<>(exception);
    }

    /**
     * Creates a wrapper for transient failure response from the service.
     *
     * @param exception Exception to be wrapped.
     */
    public static <C> Failure<C> transientFailure(Exception exception) {
        return new TransientFailure<>(exception);
    }

    public static class Success<C> extends FacadeContentRetrieverResponse<C> {
        private C content;

        private Success(C content) {
            this.content = content;
        }

        @Override
        public C getContent() {
            return content;
        }

        @Override
        public Exception getException() {
            throw new RuntimeException("Success.getException");
        }
    }

    public static abstract class Failure<C> extends FacadeContentRetrieverResponse<C> {
        private Exception exception;

        protected Failure(Exception exception) {
            this.exception = exception;
        }

        @Override
        public C getContent() {
            throw new RuntimeException("Failure.getContent");
        }

        @Override
        public Exception getException() {
            return exception;
        }
    }

    public static class PersistentFailure<C> extends Failure<C> {

        private PersistentFailure(Exception exception) {
            super(exception);
        }
    }

    public static class TransientFailure<C> extends Failure<C> {

        private TransientFailure(Exception exception) {
            super(exception);
        }
    }

    public static boolean isSuccess(FacadeContentRetrieverResponse<?> response) {
        return Success.class.equals(response.getClass());
    }

    public static boolean isFailure(FacadeContentRetrieverResponse<?> response) {
        return !Success.class.equals(response.getClass());
    }
}
