package cl.duoc.ms_barriodigital_kafka_admin.dto;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicRequest {

    @NotBlank(message = "El nombre del topic es obligatorio")
    private String nombre;

    @NotNull(message = "Las particiones son obligatorias")
    @Min(value = 1, message = "Debe tener al menos 1 partición")
    private Integer particiones;

    // En local hay un solo broker, así que el máximo es 1
    @NotNull(message = "Las réplicas son obligatorias")
    @Min(value = 1, message = "Debe tener al menos 1 réplica")
    private Integer replicas;

    // Opcionales: si no vienen, Kafka usa sus valores por defecto
    private String cleanupPolicy;   // "delete" o "compact,delete"
    private Long retencionMs;       // ej. 604800000 = 7 días
}
