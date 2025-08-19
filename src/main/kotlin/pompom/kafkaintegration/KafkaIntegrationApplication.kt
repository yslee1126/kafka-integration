package pompom.kafkaintegration

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.kafka.annotation.EnableKafka
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.annotation.EnableScheduling

@SpringBootApplication
class KafkaIntegrationApplication

fun main(args: Array<String>) {
    runApplication<KafkaIntegrationApplication>(*args)
}
