package pompom.kafkaintegration.consumer

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.stereotype.Component

@Component
class SimpleConsumer {
    private val logger = LoggerFactory.getLogger(SimpleConsumer::class.java)

    @PostConstruct
    fun init() {
        logger.info("💡 SimpleConsumer 초기화 완료")
    }

    @KafkaListener(
        topics = ["xml-file-notifications"], 
        groupId = "simple-group",
        autoStartup = "true",
        id = "simpleConsumer"
    )
    fun processMessage(message: String) {
        logger.info("📩 단순 컨슈머 메시지 수신: {}", message)
    }
}
