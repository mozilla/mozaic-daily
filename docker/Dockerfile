# syntax=docker/dockerfile:1

# Builder stage for mozaic (no dependencies)

FROM --platform=linux/amd64 python:3.10.11-bullseye AS builder
RUN mkdir -p /wheel

COPY docker/requirements.outerbounds.txt requirements.outerbounds.txt
RUN python -m pip install --no-cache-dir -r requirements.outerbounds.txt
RUN python -m pip install --no-cache-dir -U pip setuptools wheel

# Compile CmdStan once in builder. We copy the resulting ~/.cmdstan into runtime.
RUN python -m pip install --no-cache-dir cmdstanpy && \
    python -c "import cmdstanpy; cmdstanpy.install_cmdstan()"

RUN python -m pip wheel --no-cache-dir --wheel-dir /wheel \
      --no-binary prophet prophet

RUN git clone https://github.com/brendanwells-moz/mozaic-forecasting /src && \
    cd /src && git checkout main && \
    git rev-parse HEAD > /wheel/mozaic_commit.txt && \
    pip wheel --no-deps /src --wheel-dir /wheel

# Runtime stage

# For outerbounds, you should specify the platform as shown, even though that is not recommended online,
# or else flows run into issues (especially when images are built on Macs and then run on Outerbounds's remote node pool machines).
ARG TARGETPLATFORM
ARG TARGETARCH
# Changed from slim-bullseye to bullseye for build tools and broader compatibility
# Prophet/Stan is compiled from source (see below) to avoid segfaults with precompiled binaries
# REVERT NOTE: Could try slim-bullseye again if build tools are installed for Prophet compilation
FROM --platform=linux/amd64 python:3.10.11-slim-bullseye
RUN echo "building for $TARGETPLATFORM ($TARGETARCH)"

# Keeps Python from buffering stdout and stderr to avoid situations where
# the application crashes without emitting any logs due to buffering.
ENV PYTHONUNBUFFERED=1

# Puts the dependencies from this requirement list into the Docker environment for installation
COPY docker/requirements.outerbounds.txt requirements.outerbounds.txt

# Installs aforementioned dependencies
RUN python -m pip install --no-cache-dir -r requirements.outerbounds.txt

COPY --from=builder /wheel /wheel

# Set up CmdStan
COPY --from=builder /root/.cmdstan /root/.cmdstan

# Install Mozaic and Prophet from /wheel with no dependencies (they're bloated)
RUN mv /wheel/mozaic_commit.txt /mozaic_commit.txt
RUN pip install --no-cache-dir --no-deps /wheel/* && rm -rf /wheel

# Install build tools needed to compile Prophet/Stan from source
# This fixes Stan segfault issues with precompiled Prophet binaries
RUN apt-get update && apt-get install -y --no-install-recommends \
      build-essential \
      g++ \
      make \
      wget \
      libtbb2 \
      && rm -rf /var/lib/apt/lists/*

# Install actual dependencies
# Prophet is built from source (--no-binary) to ensure Stan compatibility
RUN pip install --no-cache-dir \
      numpy \
      pandas \
      scipy \
      plotly \
      holidays \
      python-dateutil \
      tzdata

# Install cmdstanpy first, then compile CmdStan from source
# This ensures Stan binaries are compiled for this exact environment
RUN pip install --no-cache-dir cmdstanpy && \
    python -c "import cmdstanpy; cmdstanpy.install_cmdstan()"

# Now install Prophet (will use the cmdstan we just compiled)
RUN pip install --no-cache-dir --no-binary prophet prophet

# Remove build tools to reduce image size
RUN apt-get purge -y --auto-remove build-essential g++ make wget

# Cleanup
RUN pip uninstall -y matplotlib fonttools wandb botocore kubernetes

RUN rm -rf /root/.cache/pip

# Copy package source code
COPY src/ /src/

# Copy test script for easy Docker validation
COPY docker/test_docker.py /test_docker.py

# Set PYTHONPATH so mozaic_daily package can be imported
ENV PYTHONPATH=/src

# Run a quick import test
RUN python -c "import mozmlops; print('mozmlops import OK')"
RUN python -c "import numpy, pandas, scipy, prophet, plotly, holidays; print('mozaic stack import #OK')"
RUN python -c "import mozaic; print('mozaic imports OK')"

# Test new package imports (PYTHONPATH is set above)
RUN python -c "import mozaic_daily; print('mozaic_daily package import OK')"
RUN python -c "from mozaic_daily.validation import validate_output_dataframe; print('validation import OK')"
RUN python -c "from mozaic_daily import main; print('main import OK')"

