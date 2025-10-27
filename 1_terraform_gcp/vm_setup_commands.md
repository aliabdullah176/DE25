<!-- 
This file is to act as a step by step of what I did to setup everything on a GCP VM. Saving here for posterity -->

# 1. generate ssh key
<!-- for pairing to the VM
docs: https://cloud.google.com/compute/docs/connect/create-ssh-keys

In a terminal: -->
ssh-keygen -t rsa -f C:\Users\Ali\.ssh\ -C ali
<!-- upload the public key (.pub) to gcp -> compute engine -> metadata -> sshkeys

Next create a vm:
Change boot image to ubuntu 22.04 LTS. x86/64 if you selected an intel based cpu for the vm
Change disk to ~30GB -->

# 2. lets ssh into our vm now
<!-- We can do this in VSCode directly. 

install remote ssh extention (microsoft is the publisher iirc)

if you do use vscode to create the ssh config file, you might need to open the config and fix the path to ssh key. -->

ssh -i C:\\Users\\Ali\\.ssh\\de25 ali@34.55.173.25

<!-- now ssh into the vm in a new window -->

### now we are using the vm to write these instructions

# 3. setup python venv
<!-- check if you have python already installed -->

which python
which python3

<!-- I already had python 3.12.x installed on my vm
Anaconda is also not free for commercial use anymore so I will depart from the project instructions to just use base python to manage packages
Poetry is a decent package / env manager as well, but i want to try out the base venv

I mapped python3 to python just for ease of use
add the following line to .bashrc, then source .bashrc
alias python=python3 -->

# create venv
python -m venv ./.venv
# activate venv
. .venv/bin/activate

# pip install your packages.
# pandas, numpy etc

# I should create a requirements.txt file at some point

# 4. get docker
sudo apt-get update

sudo apt-get install docker.io

# to remove sudo required for every docker run
sudo groupadd docker
sudo gpasswd -a $USER docker
newgrp docker

docker run hello-world

docker run -it ubuntu bash

# 5. get docker compost

cd ~
mkdir bin
cd bin
ls bin
wget https://github.com/docker/compose/releases/download/v2.40.2/docker-compose-linux-x86_64 -O docker-compose

# make it executable
chmod +x docker-compose

<!-- add bin to PATH. add the following to .bashrc -->
export PATH="${HOME}/bin:${PATH}"

docker-compose --version

# 6. running the pipeline built earlier in the code
# probably will run into errors as we figure out paths and permissions etc

cd 2_docker_sql/

docker-compose up -d

pip install pgcli 

# for running on vm
pgcli -h localhost -U root -d ny_taxi

# after forwarding ports, you can do something like (though this kind of timed out for me):
pgcli -h localhost -U root -d ny_taxi

# my venv got corrupted for some reason so had to create it again

pip install sqlalchemy
pip install psycopg2-binary

# in bash

URL="https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"

python ingest_data.py \
  --user=root \
  --password=root \
  --host=localhost \
  --port=5432 \
  --db=ny_taxi \
  --table_name=yellow_taxi_trips \
  --url=${URL} \
  --table_name_zones=taxi_zones \
  --schema=DE25

# 7. terraform setup

# download terraform to bin directory

cd ~
cd bin
wget https://releases.hashicorp.com/terraform/1.13.4/terraform_1.13.4_linux_amd64.zip
sudo apt install unzip
unzip terraform_1.13.4_linux_amd64
rm terraform_1.13.4_linux_amd64

# you should be able to run terraform now
terraform -version

# lets run terraform on the vm now
# navigate to repo where the terraform stuff was saved

cd ~/DE25/1_terraform_gcp

# but first we need to configure the keys
# copy over your json credentials to ~/.gc. Can do sftp or just use the file browser in vscode
# then add it to the env variables
export GOOGLE_APPLICATION_CREDENTIALS=~/.gc/my_creds.json
echo $GOOGLE_APPLICATION_CREDENTIALS

gcloud auth activate-service-account --key-file $GOOGLE_APPLICATION_CREDENTIALS

terraform init
terraform plan
terraform apply
terraform destroy