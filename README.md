# Azure HSM Agent and Data Movers for Lustre

This has been updated for Azure to provide a copy tool back to BLOB storage.

RPMS are available here for this initial version:

__Lustre 2.10__

* https://azurehpc.azureedge.net/rpms/lemur-azure-hsm-agent-1.0.0-lustre_2.10.x86_64.rpm
* https://azurehpc.azureedge.net/rpms/lemur-azure-data-movers-1.0.0-lustre_2.10.x86_64.rpm

__Lustre 2.12__

* https://azurehpc.azureedge.net/rpms/lemur-azure-hsm-agent-1.0.0-lustre_2.12.x86_64.rpm
* https://azurehpc.azureedge.net/rpms/lemur-azure-data-movers-1.0.0-lustre_2.12.x86_64.rpm


## Building

Commands used to build RPMS:

```
wget https://dl.google.com/go/go1.12.1.linux-amd64.tar.gz
sudo tar -C /usr/local -xzf go1.12.1.linux-amd64.tar.gz
export PATH=/usr/local/go/bin:$PATH
sudo yum install -y git gcc rpmdevtools rpmlint

git clone https://github.com/edwardsp/lemur.git
cd lemur
go mod init
go mod vendor
make local-rpm
```

> Note: Lustre 2.12 has an API change in the HSM so the go-lustre needs patching.  Change the `int` to `enum_changelog_rec_flages`.
