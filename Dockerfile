# BlazingHill Express Reporting — Railway Dockerfile
# Node.js 20 + Python 3 for report engine (matplotlib/numpy)

FROM node:20-bookworm-slim

# Install Python 3, pip, build tools (for better-sqlite3), and native libs for matplotlib
RUN apt-get update && apt-get install -y --no-install-recommends \
    python3 \
    python3-pip \
    python3-dev \
    build-essential \
    libstdc++6 \
    libfreetype6-dev \
    libpng-dev \
    pkg-config \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Copy package files and install Node.js dependencies
COPY package*.json ./
RUN npm install --production

# Copy Python requirements and install
COPY engine/requirements.txt ./engine/requirements.txt
RUN pip3 install --no-cache-dir --break-system-packages -r engine/requirements.txt

# Copy the rest of the application
COPY . .

# Create data and reports directories
RUN mkdir -p data reports

# Expose the port (Railway sets PORT env var)
EXPOSE 8000

# Start the server
CMD ["node", "server/index.js"]
