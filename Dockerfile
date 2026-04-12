# Apify-compatible Python 3.11 actor image
# See: https://hub.docker.com/r/apify/actor-python

FROM apify/actor-python:3.11

# Upgrade pip and install wheel first for faster builds
RUN pip install --no-cache-dir --upgrade pip wheel

# Install dependencies before copying source to leverage Docker layer cache
COPY requirements.txt ./
RUN pip install --no-cache-dir -r requirements.txt

# Copy all project files
COPY . ./

# Apify actors run as non-root by default; files are owned by myuser
USER myuser

# Run the actor
CMD ["python", "main.py"]
