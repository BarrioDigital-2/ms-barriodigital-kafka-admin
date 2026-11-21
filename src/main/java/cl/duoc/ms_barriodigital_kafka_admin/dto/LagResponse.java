package cl.duoc.ms_barriodigital_kafka_admin.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

// Una fila por partición: cuánto ha leído el grupo vs. cuánto hay en el topic
@Data
@NoArgsConstructor
@AllArgsConstructor
public class LagResponse {

    private String topic;
    private int particion;
    private long offsetLeido;    // hasta dónde leyó el grupo
    private long ultimoOffset;   // último mensaje que hay en la partición
    private long lag;            // mensajes pendientes = ultimoOffset - offsetLeido
}
