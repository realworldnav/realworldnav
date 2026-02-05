FROM ubuntu:22.04

ENV DEBIAN_FRONTEND=noninteractive

# Install base tools + Python
RUN apt-get update && apt-get install -y \
    git \
    curl \
    ca-certificates \
    build-essential \
    python3 \
    python3-pip \
    python3-venv \
  && rm -rf /var/lib/apt/lists/*

# Install Node.js 20 LTS (Ubuntu 22.04's default Node is too old for Claude Code)
RUN curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
  && apt-get install -y nodejs \
  && rm -rf /var/lib/apt/lists/*

# Install Claude Code CLI globally
RUN npm install -g @anthropic-ai/claude-code

# Create non-root user
RUN useradd -m -u 1000 dev

# Install Python dependencies as root so they're available system-wide
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Switch to non-root user
USER dev

WORKDIR /workspace

CMD ["bash"]
