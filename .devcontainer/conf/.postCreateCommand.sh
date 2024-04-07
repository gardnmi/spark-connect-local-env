# !/bin/bash
sudo apt-get update && sudo apt-get -y upgrade

# Install Java
sudo apt-get -y install default-jdk-headless

# Install Python Package
pip install --upgrade pip
pipx install hatch
pip install -e .


