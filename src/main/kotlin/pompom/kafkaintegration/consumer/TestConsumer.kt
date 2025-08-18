package pompom.kafkaintegration.consumer

import jakarta.annotation.PostConstruct
import org.slf4j.LoggerFactory
import org.springframework.kafka.annotation.KafkaListener
import org.springframework.messaging.handler.annotation.Payload
import org.springframework.stereotype.Component

@Component
class TestConsumer {
    private val logger = LoggerFactory.getLogger(TestConsumer::class.java)

    @PostConstruct
    fun init() {
        logger.info("🔍 TestConsumer 초기화 완료")
    }

    @KafkaListener(
        topics = ["xml-file-notifications"],
        groupId = "test-consumer-group",
        containerFactory = "kafkaListenerContainerFactory",
        autoStartup = "true",
        id = "testConsumer"
    )
    fun listen(@Payload message: String) {
        logger.info("🧪 테스트 컨슈머 메시지 수신: {}", message)
    }
}
