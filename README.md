# ms-barriodigital-kafka-admin

API REST para **administrar Kafka**: topics, consumer groups y cluster.
No envía ni lee mensajes: le pide a Kafka (vía `AdminClient`) que cree, liste o elimine, y devuelve las respuestas en JSON.
Equivalente a `ms-barriodigital-rabbit-admin`, pero para Kafka.

| Dato | Valor |
|------|-------|
| Puerto | `8086` |
| Stack | Spring Boot 4.1.2 · Java 17 · Maven · kafka-clients 4.2 |

## Cómo levantar (local)

```powershell
# 1. Infraestructura (Kafka en localhost:9092), desde ms-barriodigital-notifications
docker compose up -d

# 2. Este microservicio
.\mvnw spring-boot:run "-Dmaven.test.skip=true"
```

| Variable | Por defecto | En Docker |
|----------|-------------|-----------|
| `KAFKA_BOOTSTRAP_SERVERS` | `localhost:9092` | `kafka:9093` |

## Endpoints

Base: `/api/v1/kafka-admin`

| Método | Ruta | Descripción | Respuestas |
|--------|------|-------------|------------|
| GET | `/topics` | Listar topics (sin los internos) | 200 |
| GET | `/topics/{nombre}` | Detalle con `cleanup.policy` y `retention.ms` | 200 · 404 |
| POST | `/topics` | Crear topic | 201 · 400 · 409 |
| DELETE | `/topics/{nombre}` | Eliminar topic (los internos `_*` están protegidos) | 204 · 400 · 404 |
| GET | `/consumer-groups` | Listar consumer groups y su estado | 200 |
| GET | `/consumer-groups/{groupId}/lag` | Mensajes pendientes por partición | 200 · 404 |
| GET | `/cluster` | Cluster id, controlador y brokers | 200 |

Si Kafka no responde, la API devuelve `503`.

## Crear los topics del caso

Cuerpo de `POST /topics`. En local se usa `replicas: 1` (hay un solo broker); en producción, `3`.

```json
{ "nombre": "requests.events", "particiones": 3, "replicas": 1, "cleanupPolicy": "delete", "retencionMs": 604800000 }
```
```json
{ "nombre": "audit.timeline", "particiones": 3, "replicas": 1, "cleanupPolicy": "compact,delete", "retencionMs": 2592000000 }
```
```json
{ "nombre": "report.DLT", "particiones": 3, "replicas": 1, "cleanupPolicy": "delete", "retencionMs": 1209600000 }
```

| Retención | `retencionMs` |
|-----------|---------------|
| 1 día | `86400000` |
| 7 días | `604800000` |
| 14 días | `1209600000` |
| 30 días | `2592000000` |

## Lag

Cuántos mensajes le faltan por leer a un consumer group. Lag `0` = al día.

```json
[
  { "topic": "solicitudes-eventos", "particion": 0, "offsetLeido": 4, "ultimoOffset": 6, "lag": 2 }
]
```

## Cómo probar

```powershell
curl.exe -s http://localhost:8086/api/v1/kafka-admin/cluster
curl.exe -s http://localhost:8086/api/v1/kafka-admin/topics
curl.exe -s http://localhost:8086/api/v1/kafka-admin/consumer-groups/auditoria-processor/lag
```

En PowerShell, los `POST` con JSON se envían por tubería para que las comillas lleguen bien:

```powershell
'{"nombre":"prueba.topic","particiones":3,"replicas":1}' | curl.exe -s -X POST http://localhost:8086/api/v1/kafka-admin/topics -H "Content-Type: application/json" --data-binary "@-"
```
