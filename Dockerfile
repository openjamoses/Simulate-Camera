#to build on docker host first: docker build --no-cache=true -f Dockerfile https://github.com/brianr82/Emulated-Camera.git -t brianr82/Emulated-Camera:latest
#usage example: docker run -it -e camera_id='IoT_Camera_01' -e cassandra_ip='10.12.7.5' img_quality='5' transmit_delay ='1'--name emucamera01 emucamera
FROM python:3.6-slim
RUN mkdir app
WORKDIR app
COPY / /app
RUN mkdir imagesout


RUN apt-get update && apt-get install -y build-essential \
    cmake \
    wget \
    git \
    libgtk2.0-dev \
    && apt-get -y clean all \
    && rm -rf /var/lib/apt/lists/*

RUN pip install opencv-contrib-python-headless
RUN pip install cassandra-driver
RUN pip install confluent-kafka


#ENV camera_id = 'IOT_camera_x'


ENTRYPOINT ["python3", "/app/main.py"]

