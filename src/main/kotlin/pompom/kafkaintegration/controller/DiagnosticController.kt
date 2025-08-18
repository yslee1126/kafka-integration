package pompom.kafkaintegration.controller

import org.apache.kafka.clients.admin.AdminClient
import org.apache.kafka.clients.admin.AdminClientConfig
import org.apache.kafka.clients.admin.NewTopic
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.RequestParam
import org.springframework.web.bind.annotation.RestController
import java.time.Duration
import java.util.*


import org.slf4j.LoggerFactory
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.web.bind.annotation.*
import org.springframework.http.ResponseEntity

@RestController
@RequestMapping("/api/kafka")
class KafkaTestController(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val kafkaListenerEndpointRegistry: KafkaListenerEndpointRegistry
) {

    private val logger = LoggerFactory.getLogger(KafkaTestController::class.java)

    /**
     * 테스트용 메시지 발송
     */
    @PostMapping("/send")
    fun sendMessage(
        @RequestParam topic: String,
        @RequestParam message: String,
        @RequestParam(required = false) key: String? = null
    ): ResponseEntity<out Map<String, Any?>?> {

        return try {
            val result = if (key != null) {
                kafkaTemplate.send(topic, key, message)
            } else {
                kafkaTemplate.send(topic, message)
            }

            logger.info("📤 메시지 발송 완료 - Topic: {}, Key: {}, Message: {}", topic, key, message)

            ResponseEntity.ok(mapOf(
                "success" to true,
                "message" to "메시지가 성공적으로 발송되었습니다",
                "topic" to topic,
                "key" to key,
                "content" to message
            ))
        } catch (e: Exception) {
            logger.error("❌ 메시지 발송 실패: {}", e.message, e)
            ResponseEntity.badRequest().body(mapOf(
                "success" to false,
                "error" to e.message
            ))
        }
    }

    /**
     * 파일 도착 이벤트 시뮬레이션
     */
    @PostMapping("/simulate-file-arrival")
    fun simulateFileArrival(
        @RequestParam fileName: String,
        @RequestParam(defaultValue = "/data/files") filePath: String
    ): ResponseEntity<out Map<String, Any?>?> {

        val fileArrivalMessage = """
            {
                "eventType": "FILE_ARRIVED",
                "fileName": "$fileName",
                "filePath": "$filePath",
                "timestamp": "${System.currentTimeMillis()}",
                "fileSize": ${(1000..100000).random()},
                "source": "test-controller"
            }
        """.trimIndent()

        return sendMessage("file-arrival-events", fileArrivalMessage, fileName)
    }

    /**
     * 리스너 상태 조회
     */
    @GetMapping("/listeners/status")
    fun getListenersStatus(): ResponseEntity<out Map<String, Any?>?> {
        return try {
            val listeners = kafkaListenerEndpointRegistry.listenerContainers
            val listenerStatus = listeners.map { container ->
                mapOf(
                    "id" to container.listenerId,
                    "running" to container.isRunning,
                    "paused" to container.isPauseRequested,
                    "topics" to (container.containerProperties?.topics?.joinToString(", ") ?: "unknown")
                )
            }

            ResponseEntity.ok(mapOf(
                "success" to true,
                "totalListeners" to listeners.size,
                "listeners" to listenerStatus
            ))
        } catch (e: Exception) {
            logger.error("❌ 리스너 상태 조회 실패: {}", e.message, e)
            ResponseEntity.badRequest().body(mapOf(
                "success" to false,
                "error" to e.message
            ))
        }
    }

    /**
     * 특정 리스너 제어 (시작/정지/재시작)
     */
    @PostMapping("/listeners/{listenerId}/control")
    fun controlListener(
        @PathVariable listenerId: String,
        @RequestParam action: String
    ): ResponseEntity<out Map<String, Any?>?> {

        return try {
            val container = kafkaListenerEndpointRegistry.getListenerContainer(listenerId)

            if (container == null) {
                return ResponseEntity.badRequest().body(mapOf(
                    "success" to false,
                    "error" to "리스너를 찾을 수 없습니다: $listenerId"
                ))
            }

            when (action.uppercase()) {
                "START" -> {
                    if (!container.isRunning) {
                        container.start()
                        logger.info("🟢 리스너 시작: {}", listenerId)
                    }
                }
                "STOP" -> {
                    if (container.isRunning) {
                        container.stop()
                        logger.info("🔴 리스너 정지: {}", listenerId)
                    }
                }
                "RESTART" -> {
                    container.stop()
                    Thread.sleep(1000) // 잠시 대기
                    container.start()
                    logger.info("🔄 리스너 재시작: {}", listenerId)
                }
                "PAUSE" -> {
                    container.pause()
                    logger.info("⏸️ 리스너 일시정지: {}", listenerId)
                }
                "RESUME" -> {
                    container.resume()
                    logger.info("▶️ 리스너 재개: {}", listenerId)
                }
                else -> {
                    return ResponseEntity.badRequest().body(mapOf(
                        "success" to false,
                        "error" to "지원하지 않는 액션입니다: $action (START, STOP, RESTART, PAUSE, RESUME 중 하나)"
                    ))
                }
            }

            ResponseEntity.ok(mapOf(
                "success" to true,
                "message" to "리스너 제어 완료: $action",
                "listenerId" to listenerId,
                "action" to action,
                "currentStatus" to mapOf(
                    "running" to container.isRunning,
                    "paused" to container.isPauseRequested
                )
            ))

        } catch (e: Exception) {
            logger.error("❌ 리스너 제어 실패: {}", e.message, e)
            ResponseEntity.badRequest().body(mapOf(
                "success" to false,
                "error" to e.message
            ))
        }
    }

    /**
     * 헬스체크
     */
    @GetMapping("/health")
    fun healthCheck(): ResponseEntity<Map<String, Any>> {
        val listeners = kafkaListenerEndpointRegistry.listenerContainers
        val runningListeners = listeners.count { it.isRunning }

        return ResponseEntity.ok(mapOf(
            "status" to "UP",
            "kafka" to mapOf(
                "totalListeners" to listeners.size,
                "runningListeners" to runningListeners,
                "healthy" to (runningListeners == listeners.size)
            ),
            "timestamp" to System.currentTimeMillis()
        ))
    }
}