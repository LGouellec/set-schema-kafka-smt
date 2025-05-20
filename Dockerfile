FROM maven:3.9.9-eclipse-temurin-17 AS build

RUN mkdir -p /app
COPY pom.xml /app
RUN ls /app
WORKDIR /app
RUN mvn dependency:copy-dependencies -DexcludeTransitive=true -DoutputDirectory=.
COPY target/kafka-connect-transform-schema-1.0-SNAPSHOT.jar /app
RUN rm /app/pom.xml
RUN ls /app



FROM confluentinc/cp-server-connect:7.9.0

USER root
RUN confluent-hub install --no-prompt confluentinc/kafka-connect-s3-source:2.6.11
RUN mkdir -p /usr/share/custom-plugin
COPY --from=build /app /usr/share/custom-plugin
USER appuser

# FOR DEBUGING ENV JAVA_TOOL_OPTIONS="-agentlib:jdwp=transport=dt_socket,server=y,suspend=n,address=*:5005"