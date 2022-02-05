FROM ubuntu:latest

# Install OpenJDK 8
RUN \
  apt-get update && \
  apt-get install -y openjdk-8-jdk && \
  rm -rf /var/lib/apt/lists/*

# Install Python
RUN \
    apt-get update && \
    apt-get install -y python python-dev python3-pip python3-virtualenv && \
    rm -rf /var/lib/apt/lists/*

# Install PySpark and Numpy
RUN \
    pip install --upgrade pip && \
    pip install numpy && \
    pip install pyspark


RUN apt-get update && \
    apt-get -y install sudo && \
    rm -rf /var/lib/apt/lists/*
    
# RUN adduser --disabled-password --gecos '' admin
# RUN adduser admin sudo
# RUN echo '%sudo ALL=(ALL) NOPASSWD:ALL' >> /etc/sudoers

# USER admin

RUN mkdir /work
#Copy source directories to container
ADD sparkify/ /work/
#RUN sudo chown daemon:daemon -R /home/admin/work/
RUN pip install -r /work/requirements.txt

# Define default command
WORKDIR /work
ENTRYPOINT ["python3"]
CMD ["sparkify.py"]


