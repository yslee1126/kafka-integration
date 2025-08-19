package pompom.kafkaintegration.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.annotation.EnableAsync
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor
import java.util.concurrent.Executor
import java.util.concurrent.ThreadPoolExecutor

@Configuration
@EnableAsync
class ThreadPoolConfig {

    @Bean(name = ["taskExecutor"])
    fun taskExecutor(): Executor {
        val executor = ThreadPoolTaskExecutor()
        executor.corePoolSize = 5
        executor.maxPoolSize = 10
        executor.queueCapacity = 25
        executor.setThreadNamePrefix("app-async-")
        // 종료 시 대기 큐에 있는 작업 처리 방식 설정
        executor.setRejectedExecutionHandler(ThreadPoolExecutor.CallerRunsPolicy())
        // 스레드 풀 종료 시 대기 작업 처리 여부 설정
        executor.setWaitForTasksToCompleteOnShutdown(true)
        // 종료 대기 시간 설정 (초 단위)
        executor.setAwaitTerminationSeconds(3)
        executor.initialize()
        return executor
    }

    @Bean(name = ["fileWatcherExecutor"])
    fun fileWatcherExecutor(): ThreadPoolTaskExecutor {
        val executor = ThreadPoolTaskExecutor()
        executor.corePoolSize = 1
        executor.maxPoolSize = 1
        executor.queueCapacity = 1
        executor.setThreadNamePrefix("file-watcher-")
        // 종료 시 대기 큐에 있는 작업 처리 방식 설정
        executor.setRejectedExecutionHandler(ThreadPoolExecutor.DiscardPolicy())
        // 스레드 풀 종료 시 대기 작업 즉시 취소 (기본값은 false)
        executor.setWaitForTasksToCompleteOnShutdown(false)
        // 종료 대기 시간 설정 (초 단위)
        executor.setAwaitTerminationSeconds(1)
        executor.initialize()
        return executor
    }
}
