package pompom.kafkaintegration

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class KafkaIntegrationApplication

fun main(args: Array<String>) {
    runApplication<KafkaIntegrationApplication>(*args)
}
