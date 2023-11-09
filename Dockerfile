FROM ubuntu
WORKDIR /pubsub
RUN apt-get update
RUN apt-get install -y net-tools netcat tcpdump inetutils-ping python3
COPY ./FrameSamples ./
COPY ./AudioSample.m4v ./
COPY ./ SaveImages ./
CMD ["/bin/bash"]
