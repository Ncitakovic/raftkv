# syntax=docker/dockerfile:1

# ---- build ----
FROM maven:3.9-eclipse-temurin-21 AS build
WORKDIR /src
COPY pom.xml .
RUN mvn -B -q dependency:go-offline
COPY src ./src
RUN mvn -B -q package -DskipTests

# ---- runtime ----
FROM eclipse-temurin:21-jre
# Run as an unprivileged user; the data directory is the only writable path.
RUN groupadd --system raftkv && useradd --system --gid raftkv --no-create-home raftkv \
    && mkdir -p /var/lib/raftkv && chown raftkv:raftkv /var/lib/raftkv
USER raftkv
WORKDIR /app
COPY --from=build /src/target/raftkv.jar /app/raftkv.jar

ENV RAFTKV_PORT=50051 \
    RAFTKV_DATA_DIR=/var/lib/raftkv
EXPOSE 50051
VOLUME ["/var/lib/raftkv"]

ENTRYPOINT ["java", "-XX:MaxRAMPercentage=75", "-jar", "/app/raftkv.jar"]
