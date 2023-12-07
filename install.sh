#!/bin/bash

# Check if a version is specified
if [ -z "$1" ]; then
    # Fetch the latest version if not specified
    VERSION=$(curl -s https://api.github.com/repos/go-to-k/cls3/releases/latest | grep '"tag_name":' | cut -d '"' -f 4)
    if [ -z "$VERSION" ]; then
        echo "Failed to fetch the latest version"
        exit 1
    fi
else
    VERSION=$1
fi

# Remove 'v' prefix if present
VERSION=${VERSION#v}

# Check dependencies
if ! command -v curl &> /dev/null; then
    echo "curl could not be found"
    exit 1
fi

if ! command -v tar &> /dev/null; then
    echo "tar could not be found"
    exit 1
fi

# Detect architecture
ARCH=$(uname -m)
case $ARCH in
    x86_64) ARCH="x86_64" ;;
    amd64)  ARCH="x86_64" ;;
    arm64)  ARCH="arm64" ;;
    aarch64) ARCH="arm64" ;;
    i386)   ARCH="i386" ;;
    i686)   ARCH="i386" ;;
    *) echo "Unsupported architecture: $ARCH"; exit 1 ;;
esac

# Detect OS
OS=$(uname -s)
case $OS in
    Linux) OS="Linux" ;;
    Darwin) OS="Darwin" ;;
    *) echo "Unsupported OS: $OS"; exit 1 ;;
esac

# Construct file name and download URL
FILE_NAME="cls3_${VERSION}_${OS}_${ARCH}.tar.gz"
URL="https://github.com/go-to-k/cls3/releases/download/v${VERSION}/${FILE_NAME}"

# Download the binary
echo "Downloading cls3..."
curl -L -o "$FILE_NAME" "$URL" || { echo "Failed to download cls3"; exit 1; }

# Install
echo "Installing cls3..."
tar -xzf "$FILE_NAME" || { echo "Failed to extract cls3"; exit 1; }
sudo mv cls3 /usr/local/bin/cls3 || { echo "Failed to install cls3"; exit 1; }

# Cleanup
rm "$FILE_NAME"

echo "cls3 installation complete."

# Usage prompt
echo "Run 'cls3 -h' to see how to use cls3."

exit 0