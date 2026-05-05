#!/bin/sh
# gpl-boundary installer.
#
# Usage:
#   curl -fsSL https://github.com/the-miint/GPL-boundary/releases/latest/download/install.sh | sh
#
# Environment overrides:
#   GPL_BOUNDARY_VERSION=v0.2.0   pin to a specific release tag (default: latest)
#   INSTALL_DIR=/usr/local/bin    install location (default: ~/.local/bin)
#
# Verifies SHA256 against the release's SHA256SUMS file.

set -eu

REPO="the-miint/GPL-boundary"
VERSION="${GPL_BOUNDARY_VERSION:-latest}"
INSTALL_DIR="${INSTALL_DIR:-$HOME/.local/bin}"

# --- Detect target triple from uname --------------------------------------
os="$(uname -s)"
arch="$(uname -m)"
case "$os/$arch" in
    Linux/x86_64)   target="x86_64-unknown-linux-gnu" ;;
    Darwin/arm64)   target="aarch64-apple-darwin" ;;
    Darwin/x86_64)
        printf 'gpl-boundary: Intel Macs are not in the prebuilt matrix.\n' >&2
        printf 'Build from source — see https://github.com/%s#building-from-source\n' "$REPO" >&2
        exit 1
        ;;
    *)
        printf 'gpl-boundary: no prebuilt binary for %s/%s.\n' "$os" "$arch" >&2
        printf 'Build from source — see https://github.com/%s#building-from-source\n' "$REPO" >&2
        exit 1
        ;;
esac

asset="gpl-boundary-${target}.tar.gz"
if [ "$VERSION" = "latest" ]; then
    base_url="https://github.com/${REPO}/releases/latest/download"
else
    base_url="https://github.com/${REPO}/releases/download/${VERSION}"
fi

# --- Soft runtime-dep check (warn, don't block) ---------------------------
# macOS has no runtime check: libomp is linked statically into the binary,
# and zlib lives in the macOS SDK.
case "$os" in
    Linux)
        if ! ldconfig -p 2>/dev/null | grep -q 'libz\.so'; then
            printf 'warning: libz not found in ldconfig cache.\n' >&2
            printf '         Install with: sudo apt-get install zlib1g (or your distro equivalent)\n' >&2
        fi
        ;;
esac

# --- Download tarball + checksums -----------------------------------------
tmpdir="$(mktemp -d)"
trap 'rm -rf "$tmpdir"' EXIT

printf 'Downloading %s...\n' "$asset"
curl --proto '=https' --tlsv1.2 -fSL "$base_url/$asset"        -o "$tmpdir/$asset"
curl --proto '=https' --tlsv1.2 -fSL "$base_url/SHA256SUMS"    -o "$tmpdir/SHA256SUMS"

# --- Verify checksum ------------------------------------------------------
expected="$(grep " $asset\$" "$tmpdir/SHA256SUMS" | cut -d' ' -f1 || true)"
if [ -z "$expected" ]; then
    printf 'error: %s not listed in SHA256SUMS.\n' "$asset" >&2
    exit 1
fi
if command -v sha256sum >/dev/null 2>&1; then
    actual="$(sha256sum "$tmpdir/$asset" | cut -d' ' -f1)"
elif command -v shasum >/dev/null 2>&1; then
    actual="$(shasum -a 256 "$tmpdir/$asset" | cut -d' ' -f1)"
else
    printf 'error: neither sha256sum nor shasum available.\n' >&2
    exit 1
fi
if [ "$expected" != "$actual" ]; then
    printf 'error: SHA256 mismatch for %s.\n' "$asset" >&2
    printf '       expected %s\n       got      %s\n' "$expected" "$actual" >&2
    exit 1
fi

# --- Extract + install ----------------------------------------------------
tar -xzf "$tmpdir/$asset" -C "$tmpdir"
mkdir -p "$INSTALL_DIR"
mv "$tmpdir/gpl-boundary" "$INSTALL_DIR/gpl-boundary"
chmod +x "$INSTALL_DIR/gpl-boundary"

printf '\nInstalled to %s/gpl-boundary\n' "$INSTALL_DIR"
"$INSTALL_DIR/gpl-boundary" --version || true

case ":$PATH:" in
    *":$INSTALL_DIR:"*) ;;
    *)
        printf '\n%s is not on your PATH. Add it with:\n' "$INSTALL_DIR"
        printf '  export PATH="%s:$PATH"\n' "$INSTALL_DIR"
        ;;
esac
