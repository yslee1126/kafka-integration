# Getting Started

## 개발환경 설정 

### Kafka Docker 사용하는 경우 
```
# 설치 및 기동 
~/docker> docker compose up -d

# 테스트 토픽 생성 및 확인 호스트에서 접속은 localhost:29092 사용
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --create --topic xml-file-notifications --partitions 1 --replication-factor 1
docker exec -it kafka kafka-topics --bootstrap-server localhost:29092 --list

# kafdrop 접속 (테스트토큰 보이면 정상) 
http://localhost:9000

# Kafka가 정상 기동했는지
docker logs -f kafka

# Kafdrop이 브로커에 붙었는지
docker logs -f kafdrop

# 현재 스택 정리 (볼륨까지 정리해 새로 시작하고 싶다면 -v 옵션)
docker compose down -v
```

### Mac Kafka 설치 
```
>brew install kafka
>cd /usr/local/etc/kafka
// log.dir 경로 수정, listener 포트 9092 아닌 29092 로 2군데 수정 
>vi server.properties 
>cd /usr/local/opt/kafka/bin
>./kafka-storage random-uuid
>./kafka-storage format \
  --config /usr/local/etc/kafka/server.properties \
  --cluster-id uh05ZR-2SCeR8WSBksMxJg \
  --standalone
>vi start.sh
#!/bin/bash

KAFKA_BIN=/usr/local/opt/kafka/bin
CONFIG_FILE=/usr/local/etc/kafka/server.properties
LOG_DIR=/usr/local/var/lib/kafka-logs
LOG_FILE=$LOG_DIR/kafka.log
PID_FILE=$LOG_DIR/kafka.pid

# Kafka 실행
echo "Starting Kafka..."
$KAFKA_BIN/kafka-server-start $CONFIG_FILE > $LOG_FILE 2>&1 &

# 백그라운드 PID 저장
echo $! > $PID_FILE
echo "Kafka started with PID $(cat $PID_FILE)"
>vi stop.sh
#!/bin/bash

LOG_DIR=/usr/local/var/lib/kafka-logs
PID_FILE=$LOG_DIR/kafka.pid

if [ -f $PID_FILE ]; then
  PID=$(cat $PID_FILE)
  echo "Stopping Kafka (PID $PID)..."
  kill $PID
  rm $PID_FILE
  echo "Kafka stopped."
else
  echo "No Kafka PID file found. Kafka might not be running."
fi
>vi tail.sh
#!/bin/bash

# 로그 파일 경로
LOG_FILE=/usr/local/var/lib/kafka-logs/kafka.log

if [ ! -f "$LOG_FILE" ]; then
  echo "Kafka 로그 파일이 없습니다: $LOG_FILE"
  exit 1
fi

# 옵션 처리
if [ -z "$1" ]; then
  # 옵션 없으면 실시간 로그
  tail -f "$LOG_FILE"
elif [[ "$1" =~ ^-?[0-9]+$ ]]; then
  # 숫자 옵션이면 최근 N라인 출력
  tail -n "$1" "$LOG_FILE"
else
  echo "사용법: $0 [라인수]"
  echo "  옵션 없으면 실시간 로그(-f)"
  echo "  숫자 옵션: 최근 N라인 출력 (예: -5000)"
  exit 1
fi

```

### 어플리케이션 빌드 및 실행  
```
# 우선 개발 환경에 맞춰서 모니터링 폴더를 생성합니다. 
# 사용가능한 파라메터와 함께 실행, 서버 실행할때 로깅 경로 필요 
>./gradlew bootRun -Dfile.watch.dir=/Users/username/documents/files -Dspring.profiles.active=dev
```

### xml 생성 테스트 
```
>echo '<?xml version="1.0"?><test>consumer test</test>' > consumer-test.xml
```