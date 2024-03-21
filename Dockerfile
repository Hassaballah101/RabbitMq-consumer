FROM jelastic/maven:3.9.5-openjdk-21 AS mvn_img
COPY src /usr/src/app/src
COPY pom.xml /usr/src/app
RUN mvn -f /usr/src/app/pom.xml clean package -Dmaven.test.skip=true

FROM openjdk:21-slim
COPY --from=mvn_img /usr/src/app/target/*.jar app.jar

ENTRYPOINT ["java","-jar","/app.jar"]

