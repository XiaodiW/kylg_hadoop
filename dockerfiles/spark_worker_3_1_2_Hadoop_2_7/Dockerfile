FROM alpine:3.8
ADD bash /bin/
CMD ["/bin/sh"]
MAINTAINER Xiaodi Wang <xiaodi@kth.se>
ENV ENABLE_INIT_DAEMON=true
ENV INIT_DAEMON_BASE_URI=http://identifier/init-daemon
ENV INIT_DAEMON_STEP=spark_master_init
ENV SPARK_VERSION=3.1.2
ENV HADOOP_VERSION=2.7
COPY execute-step.sh  / 
COPY finish-step.sh /
COPY wait-for-step.sh  /
RUN apk add --no-cache curl bash openjdk8-jre python3=3.6.9-r1 py-pip       \
    && chmod +x *.sh       \
    && wget https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz       \
    && tar -xvzf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz       \
    && mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} spark       \
    && rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz       \
    && cd /
# Setup JAVA_HOME 
ENV JAVA_HOME /usr/lib/jvm/java-1.8-openjdk/jre
RUN export JAVA_HOME
RUN chmod a+x /wait-for-step.sh \
    && chmod a+x /execute-step.sh \
    && chmod a+x /finish-step.sh
ENV PYTHONHASHSEED=1
COPY worker.sh /
ENV SPARK_WORKER_WEBUI_PORT=8081
ENV SPARK_WORKER_LOG=/spark/logs
ENV SPARK_MASTER=spark://spark-master:7077
EXPOSE 8081
CMD ["/worker.sh"]