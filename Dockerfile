# First, specify the base Docker image.
# You can see the Docker images from Apify at https://hub.docker.com/r/apify/
# Using playwright image which includes browser dependencies
FROM apify/actor-python:3.14

# Switch to root temporarily to install system-level browser dependencies
USER root
RUN apt-get update && apt-get install -y \
    libnss3 \
    libatk-bridge2.0-0 \
    libdrm2 \
    libxkbcommon0 \
    libgbm1 \
    libasound2 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libpango-1.0-0 \
    libcairo2 \
    libcups2 \
    && rm -rf /var/lib/apt/lists/*
USER myuser

# Copy and install Python dependencies
COPY --chown=myuser:myuser requirements.txt ./

RUN echo "Python version:" \
 && python --version \
 && echo "Pip version:" \
 && pip --version \
 && echo "Installing dependencies:" \
 && pip install -r requirements.txt \
 && echo "Installing Playwright browsers:" \
 && python -m playwright install chromium \
 && echo "All installed Python packages:" \
 && pip freeze

# Copy the remaining source files
COPY --chown=myuser:myuser . ./

# Compile Python source for early syntax error detection
RUN python3 -m compileall -q src/

# Launch the Actor
CMD ["python3", "-m", "src"]
