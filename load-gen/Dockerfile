FROM alpine:3.19
RUN wget -qO /tmp/grpcurl.tar.gz \
    https://github.com/fullstorydev/grpcurl/releases/download/v1.9.1/grpcurl_1.9.1_linux_x86_64.tar.gz && \
    tar -xzf /tmp/grpcurl.tar.gz -C /usr/local/bin/ && \
    rm /tmp/grpcurl.tar.gz
