package cl.duoc.ms_barriodigital_kafka_admin.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.Map;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class TopicResponse {

    private String nombre;
    private int particiones;
    private int replicas;

    // Solo se llena al pedir el detalle de un topic (cleanup.policy, retention.ms)
    private Map<String, String> configuracion;
}
