#!/usr/bin/env bash
# One-shot setup for the HR Insights ETL demo on a fresh Oracle Cloud (OCI)
# Always-Free Ubuntu VM (ARM Ampere A1 or x86). Run it as a user with sudo.
#
#   curl -fsSL <raw-url>/deploy/setup-oracle-vm.sh -o setup.sh
#   chmod +x setup.sh
#   DOMAIN=myhost.example.com ./setup.sh
#
# Or clone the repo first and run ./deploy/setup-oracle-vm.sh from inside it.
#
# What it does:
#  1. Installs Docker Engine + compose plugin.
#  2. Opens ports 80/443 in the VM firewall (OCI images ship with a closed iptables).
#  3. Clones the repo (if not already inside it) and starts the prod stack.
#
# IMPORTANT: opening ports in the VM is only half the job — you ALSO must add
# ingress rules for 80/443 in the OCI Security List / Network Security Group from
# the Oracle web console. See deploy/DEPLOY-oracle.md.
set -euo pipefail

REPO_URL="${REPO_URL:-https://github.com/Bootcamp-IA-MAD-P7/Proyecto1_modulo3_DE2.git}"
APP_DIR="${APP_DIR:-$HOME/hr-etl}"
DOMAIN="${DOMAIN:-}"

if [ -z "$DOMAIN" ]; then
  echo "ERROR: set DOMAIN, e.g.  DOMAIN=myhost.example.com ./setup-oracle-vm.sh" >&2
  echo "       (for a quick test without a domain, use <PUBLIC_IP>.nip.io)" >&2
  exit 1
fi

echo ">> [1/4] Installing Docker ..."
if ! command -v docker >/dev/null 2>&1; then
  sudo apt-get update -y
  sudo apt-get install -y ca-certificates curl git
  sudo install -m 0755 -d /etc/apt/keyrings
  curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
  sudo chmod a+r /etc/apt/keyrings/docker.gpg
  echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu $(. /etc/os-release && echo "$VERSION_CODENAME") stable" \
    | sudo tee /etc/apt/sources.list.d/docker.list >/dev/null
  sudo apt-get update -y
  sudo apt-get install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin
  sudo usermod -aG docker "$USER" || true
else
  echo "   Docker already installed."
fi

echo ">> [2/4] Opening ports 80/443 in the VM firewall ..."
# OCI Ubuntu images use iptables with a default-deny INPUT chain. Insert accepts.
sudo iptables -I INPUT 5 -p tcp --dport 80 -j ACCEPT || true
sudo iptables -I INPUT 6 -p tcp --dport 443 -j ACCEPT || true
# Persist the rules across reboots.
sudo apt-get install -y netfilter-persistent iptables-persistent >/dev/null 2>&1 || true
sudo netfilter-persistent save >/dev/null 2>&1 || true

echo ">> [3/4] Getting the code ..."
if [ -f "docker-compose.prod.yml" ]; then
  APP_DIR="$(pwd)"
  echo "   Already inside the repo ($APP_DIR)."
elif [ -d "$APP_DIR/.git" ]; then
  echo "   Repo present, pulling latest ..."
  git -C "$APP_DIR" pull --ff-only
else
  git clone "$REPO_URL" "$APP_DIR"
fi
cd "$APP_DIR"

echo ">> [4/4] Preparing env + starting the stack ..."
if [ ! -f .env ]; then
  cp .env.example .env
  echo "   Created .env from .env.example — review credentials before a real deploy."
fi
export DOMAIN

# Query side + monitoring behind Caddy. Use sudo if the docker group isn't active yet.
DOCKER="docker"
if ! docker ps >/dev/null 2>&1; then DOCKER="sudo docker"; fi

$DOCKER compose -f docker-compose.yml -f docker-compose.prod.yml up -d --build

cat <<EOF

============================================================
  Stack up. Next:
   - Ensure OCI Security List allows ingress TCP 80 and 443.
   - Point DNS A record of ${DOMAIN} to this VM's public IP
     (or use <PUBLIC_IP>.nip.io as DOMAIN).
   - Frontend : https://${DOMAIN}
   - API      : https://api.${DOMAIN}
   - Grafana  : https://${DOMAIN}/grafana/
  If you just added yourself to the docker group, log out/in
  (or run 'newgrp docker') so 'docker' works without sudo.
============================================================
EOF
