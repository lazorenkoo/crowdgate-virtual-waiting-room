FROM maven:3.9.11-eclipse-temurin-21 AS build
WORKDIR /workspace
COPY pom.xml ./
COPY src ./src
RUN mvn -B -DskipTests install
COPY demo-backend/pom.xml ./demo-backend/pom.xml
COPY demo-backend/src ./demo-backend/src
RUN mvn -B -f demo-backend/pom.xml -DskipTests package

FROM eclipse-temurin:21-jre-jammy AS crowdgate
WORKDIR /app
COPY --from=build --chown=10001:10001 /workspace/target/crowdgate-0.0.1-SNAPSHOT.jar app.jar
USER 10001:10001
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "/app/app.jar"]

FROM eclipse-temurin:21-jre-jammy AS demo
WORKDIR /app
COPY --from=build --chown=10001:10001 /workspace/demo-backend/target/crowdgate-demo-backend-0.0.1-SNAPSHOT.jar app.jar
USER 10001:10001
ENV SERVER_ADDRESS=0.0.0.0
EXPOSE 58081
ENTRYPOINT ["java", "-jar", "/app/app.jar"]
