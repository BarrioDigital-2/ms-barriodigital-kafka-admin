package cl.duoc.ms_barriodigital_kafka_admin.service.impl;

import cl.duoc.ms_barriodigital_kafka_admin.dto.ClusterResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.ConsumerGroupResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.LagResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.TopicRequest;
import cl.duoc.ms_barriodigital_kafka_admin.dto.TopicResponse;
import cl.duoc.ms_barriodigital_kafka_admin.exception.OperacionNoPermitidaException;
import cl.duoc.ms_barriodigital_kafka_admin.exception.RecursoNoEncontradoException;
import cl.duoc.ms_barriodigital_kafka_admin.service.KafkaAdminService;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.Config;
import org.apache.kafka.clients.admin.DescribeClusterResult;
import org.apache.kafka.clients.admin.ListGroupsOptions;
import org.apache.kafka.clients.admin.ListOffsetsResult;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.Node;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.config.ConfigResource;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;

@Service
public class KafkaAdminServiceImpl implements KafkaAdminService {

    private final AdminClient adminClient;

    public KafkaAdminServiceImpl(AdminClient adminClient) {
        this.adminClient = adminClient;
    }

    @Override
    public List<TopicResponse> listarTopics() {
        // names() no incluye los topics internos (__consumer_offsets)
        var nombres = esperar(adminClient.listTopics().names());
        return esperar(adminClient.describeTopics(nombres).allTopicNames()).values().stream()
                .map(topic -> new TopicResponse(topic.name(), topic.partitions().size(), contarReplicas(topic), null))
                .sorted(Comparator.comparing(TopicResponse::getNombre))
                .toList();
    }

    @Override
    public TopicResponse obtenerTopic(String nombre) {
        // Si no existe, Kafka lanza UnknownTopicOrPartitionException → 404
        TopicDescription topic = esperar(adminClient.describeTopics(List.of(nombre)).allTopicNames()).get(nombre);

        ConfigResource recurso = new ConfigResource(ConfigResource.Type.TOPIC, nombre);
        Config config = esperar(adminClient.describeConfigs(List.of(recurso)).all()).get(recurso);
        Map<String, String> configuracion = Map.of(
                "cleanup.policy", config.get("cleanup.policy").value(),
                "retention.ms", config.get("retention.ms").value());

        return new TopicResponse(nombre, topic.partitions().size(), contarReplicas(topic), configuracion);
    }

    @Override
    public TopicResponse crearTopic(TopicRequest request) {
        validarNoInterno(request.getNombre());

        // Solo se envían las configuraciones que vienen; el resto usa los valores por defecto de Kafka
        Map<String, String> configuracion = new HashMap<>();
        if (request.getCleanupPolicy() != null) {
            configuracion.put("cleanup.policy", request.getCleanupPolicy());
        }
        if (request.getRetencionMs() != null) {
            configuracion.put("retention.ms", String.valueOf(request.getRetencionMs()));
        }

        NewTopic topic = new NewTopic(request.getNombre(), request.getParticiones(), request.getReplicas().shortValue())
                .configs(configuracion);
        // Si ya existe → TopicExistsException (409); si hay más réplicas que brokers → 400
        esperar(adminClient.createTopics(List.of(topic)).all());

        return new TopicResponse(request.getNombre(), request.getParticiones(), request.getReplicas(), configuracion);
    }

    @Override
    public void eliminarTopic(String nombre) {
        validarNoInterno(nombre);
        esperar(adminClient.deleteTopics(List.of(nombre)).all());
    }

    @Override
    public List<ConsumerGroupResponse> listarConsumerGroups() {
        return esperar(adminClient.listGroups(ListGroupsOptions.forConsumerGroups()).all()).stream()
                .map(grupo -> new ConsumerGroupResponse(
                        grupo.groupId(),
                        grupo.groupState().map(Enum::name).orElse("DESCONOCIDO")))
                .toList();
    }

    @Override
    public List<LagResponse> obtenerLag(String groupId) {
        boolean existe = listarConsumerGroups().stream().anyMatch(g -> g.getGroupId().equals(groupId));
        if (!existe) {
            throw new RecursoNoEncontradoException("No existe el consumer group " + groupId);
        }

        // 1. Hasta dónde leyó el grupo en cada partición
        Map<TopicPartition, OffsetAndMetadata> leidos =
                esperar(adminClient.listConsumerGroupOffsets(groupId).partitionsToOffsetAndMetadata());

        // 2. Último offset de esas mismas particiones
        Map<TopicPartition, OffsetSpec> consulta = new HashMap<>();
        leidos.keySet().forEach(particion -> consulta.put(particion, OffsetSpec.latest()));
        Map<TopicPartition, ListOffsetsResult.ListOffsetsResultInfo> ultimos =
                esperar(adminClient.listOffsets(consulta).all());

        // 3. Lag = último - leído
        return leidos.entrySet().stream()
                .filter(e -> e.getValue() != null)
                .map(e -> {
                    long leido = e.getValue().offset();
                    long ultimo = ultimos.get(e.getKey()).offset();
                    return new LagResponse(e.getKey().topic(), e.getKey().partition(), leido, ultimo, ultimo - leido);
                })
                .sorted(Comparator.comparing(LagResponse::getTopic).thenComparing(LagResponse::getParticion))
                .toList();
    }

    @Override
    public ClusterResponse obtenerCluster() {
        DescribeClusterResult cluster = adminClient.describeCluster();
        Node controlador = esperar(cluster.controller());
        List<String> brokers = esperar(cluster.nodes()).stream()
                .map(nodo -> nodo.id() + " - " + nodo.host() + ":" + nodo.port())
                .toList();
        return new ClusterResponse(esperar(cluster.clusterId()), controlador.host() + ":" + controlador.port(), brokers);
    }

    // AdminClient responde con "futuros": get() espera la respuesta y saca la excepción real de Kafka
    // del envoltorio ExecutionException, para que GlobalExceptionHandler la reconozca
    private <T> T esperar(KafkaFuture<T> futuro) {
        try {
            return futuro.get();
        } catch (ExecutionException e) {
            if (e.getCause() instanceof RuntimeException causa) {
                throw causa;
            }
            throw new IllegalStateException(e.getCause());
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Operación interrumpida", e);
        }
    }

    // Todas las particiones tienen las mismas réplicas; se toma la primera
    private int contarReplicas(TopicDescription topic) {
        return topic.partitions().get(0).replicas().size();
    }

    // Los topics que empiezan con "_" son internos de Kafka (ej. __consumer_offsets)
    private void validarNoInterno(String nombre) {
        if (nombre.startsWith("_")) {
            throw new OperacionNoPermitidaException("No se permite modificar topics internos de Kafka: " + nombre);
        }
    }
}
