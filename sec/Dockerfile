FROM eclipse-temurin:25-jdk AS build
WORKDIR /workspace
COPY gradlew settings.gradle build.gradle gradle.properties ./
COPY gradle gradle
COPY core/build.gradle core/build.gradle
COPY master/build.gradle master/build.gradle
COPY sec/build.gradle sec/build.gradle
COPY core/src core/src
COPY sec/src sec/src
RUN ./gradlew :sec:bootJar --no-daemon

FROM eclipse-temurin:25-jre
WORKDIR /app
COPY --from=build /workspace/sec/build/libs/*.jar app.jar
EXPOSE 8080
ENTRYPOINT ["java", "-jar", "app.jar"]
