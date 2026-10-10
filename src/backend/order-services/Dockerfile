FROM maven:3.9-eclipse-temurin-21 AS build

WORKDIR /workspace
COPY src/backend/order-services/pom.xml ./pom.xml
COPY src/backend/order-services/src ./src
COPY src/frontend/order-ui/ ./src/main/resources/static/
RUN mvn --batch-mode --no-transfer-progress -DskipTests package

FROM eclipse-temurin:21-jre

WORKDIR /app
RUN useradd --system --uid 10001 --home-dir /app appuser
COPY --from=build --chown=10001:10001 \
    /workspace/target/order-service-0.0.1-SNAPSHOT.jar /app/app.jar
USER 10001:10001

EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
