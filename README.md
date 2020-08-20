## How to build executable for CBLD environment and run tests

This doc briefly goes over the steps involved in building the plugin for testing inside a deployed Lustre cluster.

### Preparation

- Install Docker
- Run the following commands:
  - az login
  - az acr login --name jgalaasocr
    - This is to allow us to pull images from a private registry called `jgalaasocr`.
  - docker pull jgalaasocr.azurecr.io/copytoolbuildimage/gobuild

### To build the executable

- docker run --name gobuild -v /{path-to-your-lemur-project-code}:/usr/src/lemur -it jgalaasocr.azurecr.io/copytoolbuildimage/gobuild:latest
  - Explanation:
    - We are starting a container that has all the right dependencies built (especially Lustre itself)
    - The -v flag is mapping a path on your machine (the lemur project) into a specific path on the container, so that the source code shows up inside the container.
    - Any changes you make in /usr/src/lemur on the laptop OR in the container should persist.
- cd /usr/src/lemur
  - You should see your lemur project's source code now
- ./build_plugin.sh
  - This step should run for a while, and the output will be in the `dist` folder

### To clean up 

- Break out of the container with ‘exit’.
- docker rm gobuild
  - This gets rid of the container so that you can `docker run` the next time, otherwise the name will be occupied.
 
### How to debug if something goes wrong with compilation

- The environment variables are set appropriately already, but you can examine them with ‘set’ command.
- The most important dependencies to check are:
  - The CGO_CFLAGS which specifies where to look for the C header files
  - The Lustre binaries themselves have to be available to the C compiler, check to make sure
- Check with Joe to see if the image has changed in unexpected ways.

### How to test the compiled executable

- Deploy a cluster as the guide specifies
- SSH into the HSM VM (the name starts with 'APRI') by looking up its private IP address in the portal
- Transfer your newly compiled build to that machine (you can do so via a storage container)
- See if the current lhsmd is running:
  - ps -A | grep lhsmd
- Stop the current lhsmd:
  - sudo systemctl stop lhsmd
- Move your new build into /usr/laaso/bin/ with a new name
- Edit the agent config: sudo vi /etc/lhsmd/agent
  - point it to use the new build instead
- Add a new config for the new build: cp /etc/lhsmd/lhsm-plugin-az /etc/lhsmd/lhsm-plugin-new-name
- Either:
  - restart the lhsmd with: sudo systemctl start lhsmd
  - Or better yet run it directly in a separate window: /usr/laaso/bin/lhsmd -config /etc/lhsmd/agent
    - This allows you to see the output directly
- cd /lustre/client
- Exercise the copy tool by triggering restore operations 


### Extra notes

- The lhsmd config is located at: sudo vi /etc/systemd/system/lhsmd.service
- The plugin config is located at: sudo vi /etc/lhsmd/lhsm-plugin-az
  - The name of the config is the same as the plugin.
  - If you've added new fields to the plugin config, you need to update this file before running lhsmd.
- lhsmd is the parent that starts the plugin, so stopping lhsmd also kills the copy tool
- The sys logs are located at /var/log/daemon.log
- You can trigger multiple file restores:
  - Example:
    ```bash
    IFS=$'\n'; set -f
    for f in $(find /lustre/ST0202 -type f); do lfs hsm_restore "$f"; done
    unset IFS; set +f
    ```
 