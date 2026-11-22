package cl.duoc.ms_barriodigital_kafka_admin.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ClusterResponse {

    private String clusterId;
    private String controlador;      // broker que coordina el cluster
    private List<String> brokers;    // ej. "1 - kafka:9093"
}
