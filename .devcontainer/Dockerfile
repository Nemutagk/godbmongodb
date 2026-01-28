FROM golang:1.25-bookworm

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update && apt-get upgrade -y && \
    apt-get install -y git curl wget unzip sudo && \
    adduser --disabled-password --gecos "" vscode && \
    echo "vscode ALL=(ALL) NOPASSWD: ALL" > /etc/sudoers.d/vscode && \
    apt-get clean && rm -rf /var/lib/apt/lists/*

RUN go install golang.org/x/tools/gopls@latest

RUN mkdir -p /go/pkg && \
    chown -R vscode:vscode /go/pkg

WORKDIR /workspaces

USER vscode

