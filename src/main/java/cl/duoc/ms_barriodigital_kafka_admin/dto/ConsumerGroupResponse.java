package cl.duoc.ms_barriodigital_kafka_admin.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class ConsumerGroupResponse {

    private String groupId;   // ej. auditoria-processor
    private String estado;    // STABLE = consumiendo, EMPTY = sin consumidores conectados
}
