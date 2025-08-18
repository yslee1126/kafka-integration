package pompom.kafkaintegration.consumer

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component
import java.time.LocalDateTime

@Component
class XmlFileConsumer (
    private val objectMapper: ObjectMapper
) {

    private val logger = LoggerFactory.getLogger(XmlFileConsumer::class.java)

    @PostConstruct
    fun init() {
        logger.info("🎧 XmlFileConsumer 초기화 완료!")
    }

    @KafkaListener(
        topics = ["xml-file-notifications"], 
        groupId = "file-arrival-monitor-group",
        containerFactory = "kafkaListenerContainerFactory"
    )
    fun handleFileArrival(@Payload message: String) {
        try {
            logger.info("message 도착 : {}", message)
            val jsonNode = objectMapper.readTree(message)

            val fileName = jsonNode.get("file_name")?.asText() ?: "unknown"
            val action = jsonNode.get("action")?.asText() ?: "unknown"
            val eventType = jsonNode.get("event_type")?.asText() ?: "unknown"
            val timestamp = jsonNode.get("timestamp")?.asText() ?: LocalDateTime.now().toString()

            // 파일 정보 (존재하는 경우에만)
            val filePath = jsonNode.get("file_path")?.asText()
            val fileSize = jsonNode.get("file_size")?.asLong()
            val lastModified = jsonNode.get("last_modified")?.asLong()

            // 액션별 다른 로그 메시지
            when (action) {
                "CREATED" -> {
                    logger.info("🆕 ===== XML 파일 생성 알림 =====")
                    logger.info("📁 파일명: {}", fileName)
                    filePath?.let { logger.info("📂 전체경로: {}", it) }
                    fileSize?.let { logger.info("📊 크기: {} bytes", it) }
                    logger.info("⏰ 생성시간: {}", timestamp)
                    logger.info("🏷️ 이벤트: {}", eventType)
                    logger.info("================================")
                }

                "MODIFIED" -> {
                    logger.info("📝 ===== XML 파일 수정 알림 =====")
                    logger.info("📁 파일명: {}", fileName)
                    filePath?.let { logger.info("📂 전체경로: {}", it) }
                    fileSize?.let { logger.info("📊 크기: {} bytes", it) }
                    logger.info("⏰ 수정시간: {}", timestamp)
                    logger.info("================================")
                }

                "DELETED" -> {
                    logger.info("🗑️ ===== XML 파일 삭제 알림 =====")
                    logger.info("📁 파일명: {}", fileName)
                    logger.info("⏰ 삭제시간: {}", timestamp)
                    logger.info("================================")
                }

                else -> {
                    logger.info("❓ ===== XML 파일 이벤트 =====")
                    logger.info("📁 파일명: {}", fileName)
                    logger.info("🔄 액션: {}", action)
                    logger.info("⏰ 시간: {}", timestamp)
                    logger.info("===============================")
                }
            }

        } catch (e: Exception) {
            logger.error("파일 이벤트 처리 실패: {}", e.message, e)
            logger.debug("원본 메시지: {}", message)
        }
    }


}