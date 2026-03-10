FROM postgres:16-bookworm

# Build arguments for extension versions
ARG PGVECTOR_VERSION=0.8.0
ARG AGE_TAG=PG16/v1.5.0-rc0

# Install build dependencies (including CA certs for git clone over HTTPS)
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    ca-certificates \
    git \
    postgresql-server-dev-16 \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    && rm -rf /var/lib/apt/lists/*

# Build and install pgvector
RUN git clone --branch v${PGVECTOR_VERSION} https://github.com/pgvector/pgvector.git /tmp/pgvector \
    && cd /tmp/pgvector \
    && make OPTFLAGS="" \
    && make install \
    && rm -rf /tmp/pgvector

# Build and install Apache AGE
RUN git clone --branch ${AGE_TAG} https://github.com/apache/age.git /tmp/age \
    && cd /tmp/age \
    && make \
    && make install \
    && rm -rf /tmp/age

# Clean up build dependencies to reduce image size
RUN apt-get purge -y --auto-remove \
    build-essential \
    git \
    postgresql-server-dev-16 \
    libreadline-dev \
    zlib1g-dev \
    flex \
    bison \
    && rm -rf /var/lib/apt/lists/*

# Copy custom PostgreSQL configuration
COPY config/postgresql.conf /etc/postgresql/custom.conf

# Copy initialization scripts (run once on first startup)
COPY init/ /docker-entrypoint-initdb.d/

# Expose PostgreSQL port
EXPOSE 5432

# Health check
HEALTHCHECK --interval=30s --timeout=10s --retries=3 \
    CMD pg_isready -U ${POSTGRES_USER:-context_graph} -d ${POSTGRES_DB:-context_graph_db} || exit 1
