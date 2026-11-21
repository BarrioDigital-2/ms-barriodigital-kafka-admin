package cl.duoc.ms_barriodigital_kafka_admin.exception;

// Se lanza cuando un consumer group no existe → 404
public class RecursoNoEncontradoException extends RuntimeException {

    public RecursoNoEncontradoException(String mensaje) {
        super(mensaje);
    }
}
