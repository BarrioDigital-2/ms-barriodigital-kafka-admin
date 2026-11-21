package cl.duoc.ms_barriodigital_kafka_admin.exception;

// Se lanza al intentar modificar un topic interno de Kafka (empiezan con "_") → 400
public class OperacionNoPermitidaException extends RuntimeException {

    public OperacionNoPermitidaException(String mensaje) {
        super(mensaje);
    }
}
