FROM scratch
ARG TARGETPLATFORM
COPY $TARGETPLATFORM/cacheprog /cacheprog
ENTRYPOINT ["/cacheprog"]
