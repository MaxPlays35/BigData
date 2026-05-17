FROM sbtscala/scala-sbt:eclipse-temurin-alpine-25.0.1_8_1.12.11_3.8.3 as build

WORKDIR /app
COPY build.sbt .
COPY project/plugins.sbt project/plugins.sbt
COPY src src
RUN sbt assembly
#ENTRYPOINT ["top", "-b"]

FROM apache/spark:4.0.0
COPY --from=build /app/target/scala-3.8.3/lab2-assembly-0.1.0-SNAPSHOT.jar job.jar
ENTRYPOINT ["/opt/spark/bin/spark-submit", "--class", "main", "--master", "spark://spark-master:7077", "job.jar"]