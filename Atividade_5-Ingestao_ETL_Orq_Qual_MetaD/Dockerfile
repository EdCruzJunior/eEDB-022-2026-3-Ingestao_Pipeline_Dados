FROM apache/airflow:2.10.5-python3.11

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends \
        curl \
        ca-certificates \
    && rm -rf /var/lib/apt/lists/*

USER airflow

COPY requirements.txt /requirements.txt

RUN python -m pip install --no-cache-dir -r /requirements.txt

# Workaround: pip normalizes some jaraco.* dist-info folders with underscores
# (jaraco_context, jaraco_functools), but keyring's metadata requires them by
# their dotted name. Legacy pkg_resources doesn't equate '_' and '.', so
# pkg_resources.require("openmetadata-ingestion") raises DistributionNotFound
# on every query. Duplicate the dist-info dirs under the dotted name as a fix.
RUN SITE=$(python -c "import site; print(site.getusersitepackages())") && \
    for d in "$SITE"/jaraco_context-*.dist-info "$SITE"/jaraco_functools-*.dist-info; do \
        if [ -d "$d" ]; then \
            base=$(basename "$d"); \
            newname=$(echo "$base" | sed 's/^jaraco_/jaraco./'); \
            cp -r "$d" "$SITE/$newname"; \
        fi; \
    done

ENV PYTHONPATH=/opt/airflow