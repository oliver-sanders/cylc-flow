FROM ubuntu:latest

SHELL ["/bin/bash", "-c"]

RUN apt-get update && \
    apt-get install -y -qq wget bzip2 && \
    wget -qO- \
        https://micromamba.snakepit.net/api/micromamba/linux-64/latest \
        | tar -xvj bin/micromamba && \
    rm /root/.bashrc && \
    touch /root/.bashrc && \
    ./bin/micromamba shell init -s bash -p ~/micromamba

ARG CYLC_FLOW_DIR="./"
COPY "$CYLC_FLOW_DIR" "cylc"

RUN . /root/.bashrc && \
    micromamba activate && \
    micromamba install -y cylc-flow -c conda-forge && \
    pip install ./cylc && \
    cylc version
