package pompom.kafkaintegration.sender

import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PostConstruct
import jakarta.annotation.PreDestroy
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.kafka.core.KafkaTemplate
import org.springframework.stereotype.Service
import java.io.File
import java.nio.file.ClosedWatchServiceException
import java.nio.file.FileSystems
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardWatchEventKinds
import java.nio.file.WatchEvent
import java.nio.file.WatchKey
import java.nio.file.WatchService
import java.time.LocalDateTime
import java.time.format.DateTimeFormatter
import java.util.concurrent.CompletableFuture
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

@Service
class FileWatcherService(
    private val kafkaTemplate: KafkaTemplate<String, String>,
    private val objectMapper: ObjectMapper
) {

    private val logger = LoggerFactory.getLogger(FileWatcherService::class.java)

    @Value("\${file.watch.directory}")
    private lateinit var watchDirectory: String

    @Value("\${file.watch.pattern}")
    private lateinit var filePattern: String

    private var watchService: WatchService? = null
    private val isRunning = AtomicBoolean(false)
    // watchForFileChanges() 실행 시 현재 스레드 기억
    private var watcherThread: Thread? = null

    @PostConstruct
    fun startWatching() {
        try {
            val watchPath = Paths.get(watchDirectory)

            watchService = FileSystems.getDefault().newWatchService()

            // 디렉토리를 WatchService에 등록
            watchPath.register(
                watchService!!,
                StandardWatchEventKinds.ENTRY_CREATE,
                StandardWatchEventKinds.ENTRY_MODIFY,
                StandardWatchEventKinds.ENTRY_DELETE
            )

            logger.info("🔍 파일 감시 시작: {} (패턴: {})", watchDirectory, filePattern)
            logger.info("📡 이벤트 기반 실시간 감시 모드 (폴링 아님!)")

            CompletableFuture.runAsync { watchForFileChanges() }


        } catch (e: Exception) {
            logger.error("파일 감시 서비스 시작 실패: {}", e.message, e)
        }
    }

    fun watchForFileChanges() {
        isRunning.set(true)

        try {
            // 현재 스레드 참조 저장
            watcherThread = Thread.currentThread()
            logger.info("🚀 파일 감시 스레드 시작: {}", watcherThread)

            while (isRunning.get()) {
                try {
                    // ⭐ 진짜 이벤트 기반: take()는 이벤트가 발생할 때까지 블로킹
                    // 하지만 우아한 종료를 위해 poll() 사용
                    val key = watchService?.poll(200, TimeUnit.MILLISECONDS)

                    if (key == null) {
                        // 타임아웃 - 종료 체크 후 계속
                        continue
                    }

                    // 📨 이벤트 처리 - 여기서만 CPU 사용
                    processWatchEvents(key)

                    // WatchKey 리셋
                    val valid = key.reset()
                    if (!valid) {
                        logger.warn("WatchKey가 더 이상 유효하지 않음. 감시 중단.")
                        break
                    }

                } catch (e: ClosedWatchServiceException) {
                    logger.info("WatchService 가 닫혀서 감시 루프 종료", e)
                    break
                } catch (e: Exception) {
                    logger.error("이벤트 처리 중 오류: {}", e.message, e)
                    Thread.sleep(1000) // 오류 시 잠깐 대기
                }
            }

        } catch (e: InterruptedException) {
            logger.info("파일 감시 스레드 중단됨", e)
            Thread.currentThread().interrupt()
        } catch (e: Exception) {
            logger.error("파일 감시 중 치명적 오류: {}", e.message, e)
        } finally {
            logger.info("🛑 파일 감시 스레드 종료: {}", Thread.currentThread().name)
            isRunning.set(false)
        }
    }

    private fun processWatchEvents(key: WatchKey) {
        for (event in key.pollEvents()) {
            val kind = event.kind()

            // 오버플로우 이벤트는 무시
            if (kind === StandardWatchEventKinds.OVERFLOW) {
                logger.warn("⚠️ 이벤트 오버플로우 발생 (너무 많은 파일 이벤트)")
                continue
            }

            @Suppress("UNCHECKED_CAST")
            val watchEvent = event as WatchEvent<Path>
            val fileName = watchEvent.context().toString()

            // XML 파일만 처리
            if (isTargetFile(fileName)) {
                logger.debug("🎯 타겟 파일 이벤트: {} - {}", kind.name(), fileName)

                when (kind) {
                    StandardWatchEventKinds.ENTRY_CREATE -> handleFileCreated(fileName)
                    StandardWatchEventKinds.ENTRY_MODIFY -> handleFileModified(fileName)
                    StandardWatchEventKinds.ENTRY_DELETE -> handleFileDeleted(fileName)
                }
            } else {
                logger.trace("⏭️ 무시된 파일: {} (패턴: {})", fileName, filePattern)
            }
        }
    }

    private fun isTargetFile(fileName: String): Boolean {
        return fileName.matches(Regex(filePattern.replace("*", ".*")))
    }

    private fun handleFileCreated(fileName: String) {
        logger.info("🆕 파일 생성 감지: {} (스레드: {})", fileName, Thread.currentThread().name)

        val file = File(watchDirectory, fileName)
        if (waitForFileStability(file)) {
            sendFileNotification(fileName, "CREATED", file)
        } else {
            logger.warn("⚠️ 생성된 파일에 접근할 수 없음: {}", fileName)
        }
    }

    private fun waitForFileStability(file: File, maxWaitMs: Long = 3000): Boolean {
        if (!file.exists()) {
            logger.debug("파일이 존재하지 않음: {}", file.name)
            return false
        }

        var lastSize = -1L
        var lastModified = -1L
        var stableCount = 0
        val requiredStableChecks = 3  // 연속 3번 동일해야 안정으로 판단

        val startTime = System.currentTimeMillis()

        while (System.currentTimeMillis() - startTime < maxWaitMs) {
            try {
                val currentSize = file.length()
                val currentModified = file.lastModified()

                if (currentSize == lastSize && currentModified == lastModified) {
                    stableCount++
                    if (stableCount >= requiredStableChecks) {
                        logger.debug("✅ 파일 안정성 확인: {} ({}ms 소요)",
                            file.name, System.currentTimeMillis() - startTime)
                        return true
                    }
                } else {
                    stableCount = 0  // 변경 감지되면 카운트 리셋
                    lastSize = currentSize
                    lastModified = currentModified
                }

                Thread.sleep(50)  // 50ms 간격으로 체크

            } catch (e: Exception) {
                logger.debug("파일 안정성 체크 중 오류: {} - {}", file.name, e.message)
                Thread.sleep(100)
            }
        }

        logger.warn("⏰ 파일 안정성 대기 타임아웃: {} ({}ms)", file.name, maxWaitMs)
        return file.exists() && file.canRead()  // 타임아웃되어도 파일이 있으면 처리
    }


    private fun handleFileModified(fileName: String) {
        logger.debug("📝 파일 수정 감지: {} (스레드: {})", fileName, Thread.currentThread().name)

        val file = File(watchDirectory, fileName)
        if (file.exists() && file.canRead()) {
            sendFileNotification(fileName, "MODIFIED", file)
        }
    }

    private fun handleFileDeleted(fileName: String) {
        logger.info("🗑️ 파일 삭제 감지: {} (스레드: {})", fileName, Thread.currentThread().name)
        sendFileNotification(fileName, "DELETED", null)
    }

    private fun sendFileNotification(fileName: String, eventType: String, file: File?) {
        try {
            val notification = mutableMapOf<String, Any>(
                "file_name" to fileName,
                "event_type" to "xml_file_${eventType.lowercase()}",
                "action" to eventType,
                "timestamp" to LocalDateTime.now().format(DateTimeFormatter.ISO_LOCAL_DATE_TIME),
                "detected_at" to System.currentTimeMillis(),
                "processing_thread" to Thread.currentThread().name,
                "watch_method" to "OS_NATIVE_EVENT"  // 진짜 이벤트 기반임을 명시
            )

            if (file != null && file.exists()) {
                notification.apply {
                    put("file_path", file.absolutePath)
                    put("file_size", file.length())
                    put("last_modified", file.lastModified())
                    put("readable", file.canRead())
                }
            }

            val jsonMessage = objectMapper.writeValueAsString(notification)

            kafkaTemplate.send("xml-file-notifications", jsonMessage)
                .whenComplete { result, ex ->
                    if (ex == null) {
                        logger.debug("✅ 파일 알림 전송 성공: {} ({})", fileName, eventType)
                    } else {
                        logger.error("❌ 파일 알림 전송 실패: {} - {}", fileName, ex.message)
                    }
                }

        } catch (e: Exception) {
            logger.error("파일 알림 생성 실패: {} - {}", fileName, e.message, e)
        }
    }

    @PreDestroy
    fun stopWatching() {
        try {
            logger.info("🛑 파일 감시 서비스 종료 중...")

            // 먼저 실행 중단 플래그 설정
            isRunning.set(false)

            watchService?.close()

            watcherThread?.interrupt() // poll()을 바로 깨어나게 함

            logger.info("✅ 파일 감시 서비스 종료 완료")
        } catch (e: Exception) {
            logger.error("파일 감시 서비스 종료 중 오류: {}", e.message, e)
        }
    }

}
