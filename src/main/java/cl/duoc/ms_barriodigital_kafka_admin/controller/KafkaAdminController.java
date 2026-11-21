package cl.duoc.ms_barriodigital_kafka_admin.controller;

import cl.duoc.ms_barriodigital_kafka_admin.dto.ClusterResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.ConsumerGroupResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.LagResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.TopicRequest;
import cl.duoc.ms_barriodigital_kafka_admin.dto.TopicResponse;
import cl.duoc.ms_barriodigital_kafka_admin.service.KafkaAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/api/v1/kafka-admin")
public class KafkaAdminController {

    private final KafkaAdminService service;

    public KafkaAdminController(KafkaAdminService service) {
        this.service = service;
    }

    // ========== TOPICS ==========

    @GetMapping("/topics")
    public ResponseEntity<List<TopicResponse>> listarTopics() {
        return ResponseEntity.ok(service.listarTopics());
    }

    @GetMapping("/topics/{nombre}")
    public ResponseEntity<TopicResponse> obtenerTopic(@PathVariable String nombre) {
        return ResponseEntity.ok(service.obtenerTopic(nombre));
    }

    @PostMapping("/topics")
    public ResponseEntity<TopicResponse> crearTopic(@Valid @RequestBody TopicRequest request) {
        return ResponseEntity.status(HttpStatus.CREATED).body(service.crearTopic(request));
    }

    // 204 No Content: se borró y no hay nada que devolver
    @DeleteMapping("/topics/{nombre}")
    public ResponseEntity<Void> eliminarTopic(@PathVariable String nombre) {
        service.eliminarTopic(nombre);
        return ResponseEntity.noContent().build();
    }

    // ========== CONSUMER GROUPS ==========

    @GetMapping("/consumer-groups")
    public ResponseEntity<List<ConsumerGroupResponse>> listarConsumerGroups() {
        return ResponseEntity.ok(service.listarConsumerGroups());
    }

    @GetMapping("/consumer-groups/{groupId}/lag")
    public ResponseEntity<List<LagResponse>> obtenerLag(@PathVariable String groupId) {
        return ResponseEntity.ok(service.obtenerLag(groupId));
    }

    // ========== CLUSTER ==========

    @GetMapping("/cluster")
    public ResponseEntity<ClusterResponse> obtenerCluster() {
        return ResponseEntity.ok(service.obtenerCluster());
    }
}
