#!/usr/bin/env bash
#
# Assembles and publishes Dynoxide npm packages from release binaries.
#
# Usage:
#   ./publish.sh --version 0.9.5 --release-url https://github.com/nubo-db/dynoxide/releases/download/v0.9.5
#   ./publish.sh --version 0.9.5 --release-url ... --dry-run
#
# Requires: jq, curl, npm, tar, unzip

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
NPM_DIR="$(cd "$SCRIPT_DIR/.." && pwd)"
PLATFORMS_JSON="$NPM_DIR/dynoxide/platforms.json"

DRY_RUN=""
VERSION=""
RELEASE_URL=""

usage() {
  echo "Usage: $0 --version VERSION --release-url URL [--dry-run]"
  echo ""
  echo "  --version      Release version (e.g. 0.9.5)"
  echo "  --release-url  Base URL for release assets"
  echo "  --dry-run      Assemble packages but do not publish"
  exit 1
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    --version) VERSION="$2"; shift 2 ;;
    --release-url) RELEASE_URL="$2"; shift 2 ;;
    --dry-run) DRY_RUN="--dry-run"; shift ;;
    *) echo "Unknown option: $1"; usage ;;
  esac
done

if [[ -z "$VERSION" || -z "$RELEASE_URL" ]]; then
  usage
fi

WORK_DIR=$(mktemp -d)
trap 'rm -rf "$WORK_DIR"' EXIT

echo "Publishing Dynoxide $VERSION to npm"
echo "Release URL: $RELEASE_URL"
if [[ -n "$DRY_RUN" ]]; then
  echo "DRY RUN - packages will be assembled but not published"
fi
echo ""

# Generate THIRD_PARTY_LICENSES if cargo-about is available
REPO_ROOT="$(cd "$NPM_DIR/.." && pwd)"
THIRD_PARTY_LICENSES=""
if command -v cargo-about &>/dev/null && [[ -f "$REPO_ROOT/Cargo.toml" ]]; then
  echo "Generating THIRD_PARTY_LICENSES..."
  THIRD_PARTY_LICENSES="$WORK_DIR/THIRD_PARTY_LICENSES"
  if cargo about generate \
    --manifest-path "$REPO_ROOT/Cargo.toml" \
    --config "$REPO_ROOT/about.toml" \
    "$REPO_ROOT/about.hbs" \
    -o "$THIRD_PARTY_LICENSES" 2>/dev/null; then
    echo "  Generated $(wc -l < "$THIRD_PARTY_LICENSES" | tr -d ' ') lines"
  else
    echo "Warning: could not generate THIRD_PARTY_LICENSES, continuing without it"
    THIRD_PARTY_LICENSES=""
  fi
fi

# Publish each platform package
PLATFORM_COUNT=$(jq length "$PLATFORMS_JSON")
for i in $(seq 0 $((PLATFORM_COUNT - 1))); do
  NPM_PKG=$(jq -r ".[$i].npm_package" "$PLATFORMS_JSON")
  RUST_TARGET=$(jq -r ".[$i].rust_target" "$PLATFORMS_JSON")
  OS=$(jq -r ".[$i].os" "$PLATFORMS_JSON")
  CPU=$(jq -r ".[$i].cpu" "$PLATFORMS_JSON")
  LIBC=$(jq -r ".[$i].libc // empty" "$PLATFORMS_JSON")
  ARCHIVE_EXT=$(jq -r ".[$i].archive_ext" "$PLATFORMS_JSON")
  BINARY_NAME=$(jq -r ".[$i].binary_name" "$PLATFORMS_JSON")

  echo "--- $NPM_PKG ---"

  PKG_DIR="$WORK_DIR/$NPM_PKG"
  mkdir -p "$PKG_DIR"

  # Build platform-specific package.json
  DESCRIPTION="$OS $CPU"
  if [[ -n "$LIBC" ]]; then
    DESCRIPTION="$OS $CPU ($LIBC)"
  fi

  # Start with os and cpu fields
  PKG_JSON=$(jq -n \
    --arg name "$NPM_PKG" \
    --arg version "$VERSION" \
    --arg description "Dynoxide binary for $DESCRIPTION" \
    --arg os "$OS" \
    --arg cpu "$CPU" \
    '{
      name: $name,
      version: $version,
      description: $description,
      author: "Martin Hicks",
      license: "(MIT OR Apache-2.0)",
      repository: { type: "git", url: "https://github.com/nubo-db/dynoxide" },
      os: [$os],
      cpu: [$cpu],
      preferUnplugged: true
    }')

  # Add libc field if set
  if [[ -n "$LIBC" ]]; then
    PKG_JSON=$(echo "$PKG_JSON" | jq --arg libc "$LIBC" '. + { libc: [$libc] }')
  fi

  echo "$PKG_JSON" > "$PKG_DIR/package.json"

  # Download and extract binary
  ARCHIVE_NAME="dynoxide-${RUST_TARGET}.${ARCHIVE_EXT}"
  ARCHIVE_PATH="$WORK_DIR/$ARCHIVE_NAME"

  if [[ ! -f "$ARCHIVE_PATH" ]]; then
    echo "  Downloading $ARCHIVE_NAME..."
    curl -fsSL --connect-timeout 30 --max-time 120 --retry 3 --retry-delay 5 \
      -o "$ARCHIVE_PATH" "${RELEASE_URL}/${ARCHIVE_NAME}"
  fi

  if [[ "$ARCHIVE_EXT" == "tar.gz" ]]; then
    tar xzf "$ARCHIVE_PATH" -C "$PKG_DIR"
  elif [[ "$ARCHIVE_EXT" == "zip" ]]; then
    unzip -qo "$ARCHIVE_PATH" -d "$PKG_DIR"
  fi

  # Set executable permissions (not needed on Windows)
  if [[ "$OS" != "win32" ]]; then
    chmod +x "$PKG_DIR/$BINARY_NAME"
  fi

  # Copy THIRD_PARTY_LICENSES if available
  if [[ -n "$THIRD_PARTY_LICENSES" && -f "$THIRD_PARTY_LICENSES" ]]; then
    cp "$THIRD_PARTY_LICENSES" "$PKG_DIR/THIRD_PARTY_LICENSES"
  fi

  # Publish (only 409/EPUBLISHCONFLICT is safe to skip)
  echo "  Publishing $NPM_PKG@$VERSION..."
  PUBLISH_OUTPUT=$(cd "$PKG_DIR" && npm publish --access public --provenance $DRY_RUN 2>&1) || {
    if echo "$PUBLISH_OUTPUT" | grep -q 'EPUBLISHCONFLICT\|previously published\|cannot publish over'; then
      echo "  Already published, skipping."
    else
      echo "  ERROR: publish failed for $NPM_PKG@$VERSION"
      echo "  $PUBLISH_OUTPUT"
      exit 1
    fi
  }

  echo ""
done

# Publish the wrapper package
echo "--- dynoxide (wrapper) ---"

WRAPPER_DIR="$WORK_DIR/dynoxide-wrapper"
cp -r "$NPM_DIR/dynoxide" "$WRAPPER_DIR"

# Stamp version into wrapper package.json using jq
jq --arg v "$VERSION" '
  .version = $v |
  .optionalDependencies = (.optionalDependencies | to_entries | map(.value = $v) | from_entries)
' "$WRAPPER_DIR/package.json" > "$WRAPPER_DIR/package.json.tmp"
mv "$WRAPPER_DIR/package.json.tmp" "$WRAPPER_DIR/package.json"

echo "  Publishing dynoxide@$VERSION..."
PUBLISH_OUTPUT=$(cd "$WRAPPER_DIR" && npm publish --access public --provenance $DRY_RUN 2>&1) || {
  if echo "$PUBLISH_OUTPUT" | grep -q 'EPUBLISHCONFLICT\|previously published\|cannot publish over'; then
    echo "  Already published, skipping."
  else
    echo "  ERROR: wrapper publish failed for dynoxide@$VERSION"
    echo "  $PUBLISH_OUTPUT"
    exit 1
  fi
}

echo ""
echo "Done. Published Dynoxide $VERSION to npm."
if [[ -n "$DRY_RUN" ]]; then
  echo "(dry run - nothing was actually published)"
fi
