#!/usr/bin/env bash
set -e
SPARK_LIB=/home/hadoop/extrajars/
mkdir -p ${SPARK_LIB}
cd ${SPARK_LIB}
wget http://central.maven.org/maven2/org/apache/logging/log4j/log4j-api/2.11.1/log4j-api-2.11.1.jar
wget http://central.maven.org/maven2/org/apache/logging/log4j/log4j-core/2.11.1/log4j-core-2.11.1.jar
wget http://central.maven.org/maven2/org/apache/logging/log4j/log4j-1.2-api/2.11.1/log4j-1.2-api-2.11.1.jar
cat > log4j2.xml <<EOL
<?xml version="1.0" encoding="UTF-8"?>
<Configuration status="WARN">
    <Appenders>
        <Console name="Console" target="SYSTEM_OUT">
            <PatternLayout pattern="%d{HH:mm:ss.SSS} [%t] %-5level %logger{36} - %msg%n"/>
        </Console>
    </Appenders>
    <Loggers>
        <Root level="INFO">
            <AppenderRef ref="Console"/>
        </Root>
    </Loggers>
</Configuration>
EOL
