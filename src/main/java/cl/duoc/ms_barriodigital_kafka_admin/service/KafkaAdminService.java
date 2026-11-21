package cl.duoc.ms_barriodigital_kafka_admin.service;

import cl.duoc.ms_barriodigital_kafka_admin.dto.ClusterResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.ConsumerGroupResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.LagResponse;
import cl.duoc.ms_barriodigital_kafka_admin.dto.TopicRequest;
import cl.duoc.ms_barriodigital_kafka_admin.dto.TopicResponse;

import java.util.List;

public interface KafkaAdminService {

    List<TopicResponse> listarTopics();

    TopicResponse obtenerTopic(String nombre);

    TopicResponse crearTopic(TopicRequest request);

    void eliminarTopic(String nombre);

    List<ConsumerGroupResponse> listarConsumerGroups();

    List<LagResponse> obtenerLag(String groupId);

    ClusterResponse obtenerCluster();
}
