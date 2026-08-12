
FROM ubuntu:24.04

RUN apt-get update && apt-get install -y ca-certificates curl && \
    rm -rf /var/lib/apt/lists/*

# Binary is copied to root by CI
COPY rzbridge /opt/rzbridge/rzbridge

RUN chmod +x /opt/rzbridge/rzbridge

EXPOSE 8777 3443

CMD ["/opt/rzbridge/rzbridge"]