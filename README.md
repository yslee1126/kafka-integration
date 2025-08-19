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

### xml 생성 테스트 
```
>echo '<?xml version="1.0"?><test>consumer test</test>' > consumer-test.xml
```




