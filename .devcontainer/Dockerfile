FROM mcr.microsoft.com/devcontainers/java:17

RUN apt-get update && apt-get install -y --no-install-recommends \
    openssh-server \
    wget \
    curl \
    vim \
    sudo \
    && rm -rf /var/lib/apt/lists/*

# SSH 配置（Hadoop 需要免密）
RUN mkdir /var/run/sshd && echo 'root:admin' | chpasswd && \
    sed -i 's/#PermitRootLogin prohibit-password/PermitRootLogin yes/' /etc/ssh/sshd_config && \
    sed -i 's/#PasswordAuthentication yes/PasswordAuthentication yes/' /etc/ssh/sshd_config
EXPOSE 22
