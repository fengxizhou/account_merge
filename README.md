# account_merge

## Purpose of the scripts
The account merge scripts (located at /usr/local/hpc/scripts/merge_accounts.py) merges the accounts on the master node to a compute or service node. The script is created to solve the issue that simple sync teh accounts file (/etc/passwd, /etc/group, and /etc/shadow) may overwrite some system accounts local to the compute or service node.

## How to use the scripts
The following cookbook synchronizes the account files from master to a new server node newnode.
1. Create a working directory /root/accounts on newnode.
1. Synchronize the accounts file from master to newnode. 

On master, run:

```
ssh newnode mkdir -p /root/accounts/20200227
rsync /etc/passwd newnode:/root/accounts/20200227/
rsync /etc/group newnode:/root/accounts/20200227/
rsync /etc/shadow newnode:/root/accounts/20200227/
rsync /usr/local/hpc/scripts/merge_accounts.py newnode:/root/accounts/
```

On newnode, run:

```
cd /root/accounts
python merge_accounts.py
```
