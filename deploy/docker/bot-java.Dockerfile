# build from the repository root:  docker build -f deploy/docker/bot-java.Dockerfile -t maroof-street/java-bot .
FROM gradle:8.9-jdk17 AS builder
# the Gradle build reads ../../../proto, so keep the repository layout
WORKDIR /src/services/bots/java
COPY proto /src/proto
COPY services/bots/java .
RUN gradle shadowJar --no-daemon

FROM eclipse-temurin:17-jre
COPY --from=builder /src/services/bots/java/build/libs/java-twap-bot-1.0-all.jar /app/java-twap-bot.jar
ENTRYPOINT ["java", "-jar", "/app/java-twap-bot.jar"]
