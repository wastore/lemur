FROM golang:1.15.0-buster AS build

# Move to working directory /build
WORKDIR /build

# Copy the code into the container
COPY . .

#COPY .gitignore /dist/

RUN git clone --branch 2.12.5 git://git.whamcloud.com/fs/lustre-release.git

ENV CGO_CFLAGS "-I/build/lustre-release/lustre/include -I/build/lustre-release/lustre/include/uapi"
ENV LUSTRE_SRC "/build/lustre-release"
#ENV GOCACHE "/build/.cache-go/go-build"
#ENV GOPATH "/build/go"

RUN go env

# Build the application
RUN go mod vendor
RUN go build -v -i -ldflags "-X 'main.version=0.6.0_56_g286df59_dirty'" -o lhsm-plugin-az ./cmd/lhsm-plugin-az

