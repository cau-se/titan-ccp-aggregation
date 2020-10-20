FROM openjdk:11-slim

ADD build/distributions/titan-ccp-aggregation.tar /

EXPOSE 80

CMD JAVA_OPTS="$JAVA_OPTS -Dorg.slf4j.simpleLogger.defaultLogLevel=$LOG_LEVEL" \
    /titan-ccp-aggregation/bin/titan-ccp-aggregation
