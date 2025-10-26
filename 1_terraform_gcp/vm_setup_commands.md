
This file is to act as a step by step of what I did to setup everything on a GCP VM. Saving here for posterity

# 1. generate ssh key
for pairing to the VM
docs: https://cloud.google.com/compute/docs/connect/create-ssh-keys

In a terminal:
ssh-keygen -t rsa -f C:\Users\Ali\.ssh\ -C ali
upload the public key (.pub) to gcp -> compute engine -> metadata -> sshkeys

Next create a vm:
Change boot image to ubuntu 22.04 LTS. x86/64 if you selected an intel based cpu for the vm
Change disk to ~30GB

# 2. lets ssh into our vm now
We can do this in VSCode directly. 

install remote ssh extention (microsoft is the publisher iirc)

if you do use vscode to create the ssh config file, you might need to open the config and fix the path to ssh key.

ssh -i C:\\Users\\Ali\\.ssh\\de25 ali@34.55.173.25

now ssh into the vm in a new window
