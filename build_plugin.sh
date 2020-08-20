export CGO_CFLAGS='-I/usr/src/lustre-2.12.5/lustre/include -I/usr/src/lustre-2.12.5/lustre/include/uapi'
export GOPATH=/go
go build -v -i -ldflags "-X 'main.version=0.6.0_56_g286df59_dirty'" -o dist/lhsm-plugin-az ./cmd/lhsm-plugin-az