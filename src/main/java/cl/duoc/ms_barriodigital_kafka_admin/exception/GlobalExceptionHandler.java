package cl.duoc.ms_barriodigital_kafka_admin.exception;

import org.apache.kafka.common.errors.InvalidConfigurationException;
import org.apache.kafka.common.errors.InvalidPartitionsException;
import org.apache.kafka.common.errors.InvalidReplicationFactorException;
import org.apache.kafka.common.errors.InvalidTopicException;
import org.apache.kafka.common.errors.TimeoutException;
import org.apache.kafka.common.errors.TopicExistsException;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.util.HashMap;
import java.util.Map;

// Convierte las excepciones (nuestras y de Kafka) en respuestas HTTP
@RestControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler({RecursoNoEncontradoException.class, UnknownTopicOrPartitionException.class})
    public ResponseEntity<Map<String, String>> manejarNoEncontrado(RuntimeException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of("error", ex.getMessage()));
    }

    @ExceptionHandler(TopicExistsException.class)
    public ResponseEntity<Map<String, String>> manejarDuplicado(TopicExistsException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", ex.getMessage()));
    }

    // Datos que Kafka rechaza (ej. más réplicas que brokers) o topics internos protegidos
    @ExceptionHandler({InvalidReplicationFactorException.class, InvalidPartitionsException.class,
            InvalidConfigurationException.class, InvalidTopicException.class, OperacionNoPermitidaException.class})
    public ResponseEntity<Map<String, String>> manejarInvalido(RuntimeException ex) {
        return ResponseEntity.badRequest().body(Map.of("error", ex.getMessage()));
    }

    // Kafka apagado o sin responder
    @ExceptionHandler(TimeoutException.class)
    public ResponseEntity<Map<String, String>> manejarSinConexion(TimeoutException ex) {
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE)
                .body(Map.of("error", "Kafka no responde: " + ex.getMessage()));
    }

    // Errores de @Valid en los DTOs: cada campo con su mensaje
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, String>> manejarValidacion(MethodArgumentNotValidException ex) {
        Map<String, String> errores = new HashMap<>();
        ex.getBindingResult().getFieldErrors()
                .forEach(error -> errores.put(error.getField(), error.getDefaultMessage()));
        return ResponseEntity.badRequest().body(errores);
    }
}
