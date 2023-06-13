FROM nvidia/cuda:11.8.0-base-ubuntu22.04

ENV NVIDIA_DRIVER_CAPABILITIES=all

WORKDIR /project

RUN apt-get update && \
    apt-get install -y --no-install-recommends  ca-certificates python3-setuptools python3-dev python3-wheel python3-pip build-essential libmariadb-dev unattended-upgrades && \
    unattended-upgrade

COPY . .

RUN python3 -m pip install -r requirements.txt

CMD ["python3", "server.py"]