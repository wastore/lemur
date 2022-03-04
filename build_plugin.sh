go build -v -i -ldflags "-X 'main.version=0.6.0_56_g286df59_dirty'" -o dist/lhsm-plugin-az ./cmd/lhsm-plugin-az
go build -v -i -ldflags "-X 'main.version=0.6.0_56_g286df59_dirty'" -o dist/lhsmd ./cmd/lhsmd
