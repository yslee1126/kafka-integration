package pompom.kafkaintegration.config

import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.annotation.EnableKafka
import jakarta.annotation.PostConstruct
import org.apache.kafka.clients.consumer.ConsumerConfig
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.core.env.Environment
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory
import org.springframework.kafka.config.KafkaListenerEndpointRegistry
import org.springframework.kafka.core.ConsumerFactory
import org.springframework.kafka.core.DefaultKafkaConsumerFactory
import org.springframework.kafka.listener.ContainerProperties
import org.springframework.scheduling.annotation.Scheduled

@Configuration
@EnableKafka
class KafkaListenerConfig (
    private val env: Environment
) {

    private val logger = LoggerFactory.getLogger(KafkaListenerConfig::class.java)

    @PostConstruct
    fun init() {
        logger.info("🔄 Kafka 리스너 설정 초기화 완료")

        // 중요: 리스너 초기화 상태 확인
        logger.info("👇 현재 설정 값들 👇")
        logger.info("bootstrap.servers: {}", env.getProperty("spring.kafka.bootstrap-servers"))
        logger.info("group.id: {}", env.getProperty("spring.kafka.consumer.group-id"))
        logger.info("auto.offset.reset: {}", env.getProperty("spring.kafka.consumer.auto-offset-reset"))
        logger.info("listener.missing-topics-fatal: {}", env.getProperty("spring.kafka.listener.missing-topics-fatal"))
        logger.info("listener.ack-mode: {}", env.getProperty("spring.kafka.listener.ack-mode"))
    }

    @Bean
    fun kafkaListenerEndpointRegistry(): KafkaListenerEndpointRegistry {
        return KafkaListenerEndpointRegistry()
    }


    @Bean
    fun consumerConfig(): Map<String, Any> {
        val props: MutableMap<String, Any> = HashMap()

        // 필수 기본 설정
        props[ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG] = env.getProperty("spring.kafka.bootstrap-servers", "localhost:29092")
        props[ConsumerConfig.GROUP_ID_CONFIG] = env.getProperty("spring.kafka.consumer.group-id", "file-arrival-monitor-group")
        props[ConsumerConfig.AUTO_OFFSET_RESET_CONFIG] = env.getProperty("spring.kafka.consumer.auto-offset-reset", "earliest")
        props[ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java
        props[ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG] = StringDeserializer::class.java

        // 추가 중요 설정
        props[ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG] = false
        props[ConsumerConfig.MAX_POLL_RECORDS_CONFIG] = 10
        props[ConsumerConfig.FETCH_MIN_BYTES_CONFIG] = 1
        props[ConsumerConfig.FETCH_MAX_WAIT_MS_CONFIG] = 100

        logger.info("✅ 컨슈머 설정 완료")
        return props
    }

    @Bean
    fun consumerFactory(): ConsumerFactory<String, String> {
        return DefaultKafkaConsumerFactory(this.consumerConfig())
    }

    @Bean
    fun kafkaListenerContainerFactory(): ConcurrentKafkaListenerContainerFactory<String, String> {
        logger.info("🔧 KafkaListenerContainerFactory 생성 중...")
        val factory = ConcurrentKafkaListenerContainerFactory<String, String>()
        factory.consumerFactory = consumerFactory()
        factory.setConcurrency(1)
        factory.containerProperties.pollTimeout = 1000
        factory.containerProperties.ackMode = ContainerProperties.AckMode.RECORD
        factory.setAutoStartup(true)

        // 중요: 명시적으로 비활성화
        factory.setMissingTopicsFatal(false)

        logger.info("✅ KafkaListenerContainerFactory 설정 완료")
        return factory
    }

    @Scheduled(fixedRate = 60000) // 1분마다 실행
    fun checkListeners() {
        try {
            val listeners = kafkaListenerEndpointRegistry().listenerContainers
            logger.debug("🔍 Kafka 리스너 상태 확인 중 ({}개)", listeners.size)

            listeners.forEach { container ->
                logger.debug("리스너: {}, 실행중: {}, 일시정지: {}", 
                    container.listenerId,
                    container.isRunning,
                    container.isPauseRequested
                )

                if (!container.isRunning) {
                    logger.warn("⚠️ 리스너가 실행중이 아님: {}", container.listenerId)
                    try {
                        logger.info("🔄 리스너 재시작 시도: {}", container.listenerId)
                        container.start()
                    } catch (e: Exception) {
                        logger.error("❌ 리스너 재시작 실패: {}", e.message, e)
                    }
                }
            }

        } catch (e: Exception) {
            logger.error("❌ 리스너 상태 확인 중 오류: {}", e.message, e)
        }
    }

}
