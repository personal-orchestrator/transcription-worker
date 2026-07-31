#!/bin/bash
set -e

echo "Building local/transcription-worker:latest with podman..."
podman build -t local/transcription-worker:latest .

echo "Loading the image into the k3s containerd registry..."
podman save local/transcription-worker:latest | sudo k3s ctr images import -
echo "Done!"
