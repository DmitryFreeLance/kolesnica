FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /app
COPY pom.xml .
RUN mvn -q -DskipTests dependency:go-offline
COPY src ./src
RUN mvn -q -DskipTests package

FROM eclipse-temurin:21-jre
WORKDIR /opt/bot
ENV BOT_DB_PATH=/opt/bot/data/bot.db
COPY --from=build /app/target/max-bot-kolesnica-1.0.0.jar /opt/bot/bot.jar
RUN mkdir -p /opt/bot/data
ENTRYPOINT ["java","-jar","/opt/bot/bot.jar"]
