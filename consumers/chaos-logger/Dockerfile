# Stage 1 — build
FROM maven:3.9-eclipse-temurin-21-alpine AS builder
WORKDIR /build

# Copy parent POM (now reachable from consumers/ context)
COPY pom.xml .

# Copy this module
COPY chaos-logger/pom.xml chaos-logger/
COPY chaos-logger/src chaos-logger/src/

WORKDIR /build/chaos-logger
RUN mvn package -DskipTests --no-transfer-progress

# Stage 2 — runtime
FROM eclipse-temurin:21-jre-alpine
WORKDIR /app
COPY --from=builder /build/chaos-logger/target/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
