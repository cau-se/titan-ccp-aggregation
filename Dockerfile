FROM openjdk:11-slim

ADD build/distributions/titanccp-aggregation.tar /

EXPOSE 80

CMD JAVA_OPTS="$JAVA_OPTS -Dorg.slf4j.simpleLogger.defaultLogLevel=$LOG_LEVEL" \
    /titanccp-aggregation/bin/titanccp-aggregation