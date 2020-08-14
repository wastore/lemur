# clean up the existing dist folder if it is present
if [ -d "$PWD/dist" ]; then rm -Rf $PWD/dist; fi

# build the binaries
docker build --tag lemur-compile .

# copy the artifacts out
docker create -ti --name dummy lemur-compile bash
docker cp dummy:/dist $PWD

# clean up the image that we just created
docker rm -f dummy