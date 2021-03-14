From tomcat:9.0-jdk15-openjdk-oracle
RUN rm -rf /usr/local/tomcat/webapps/*
ADD ./target/streaming-engine.war /usr/local/tomcat/webapps/test.war