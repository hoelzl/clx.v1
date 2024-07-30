FROM python:3.11-slim

# Install drawio-desktop and dependencies
COPY drawio-amd64-24.7.5.deb .
RUN apt-get update && apt-get install -y \
    dbus-x11 \
    nodejs \
    npm \
    libasound2 \
    libgbm1 \
    xvfb \
    x11-xkb-utils \
    xfonts-75dpi \
    xfonts-100dpi \
    fonts-liberation \
    fonts-noto \
    fonts-noto-cjk \
    && apt-get -y -f install ./drawio-amd64-24.7.5.deb \
    && rm drawio-amd64-24.7.5.deb \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Install svgo
RUN npm install -g svgo

WORKDIR /app

# Install Architect's Daughter font
COPY ArchitectsDaughter-Regular.ttf /usr/local/share/fonts/
RUN fc-cache -f -v

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY drawio_converter.py .
COPY config.yaml .

# Set up the entrypoint script
COPY entrypoint.sh /entrypoint.sh
RUN chmod +x /entrypoint.sh

ENTRYPOINT ["/entrypoint.sh"]
