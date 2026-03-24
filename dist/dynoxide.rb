# Homebrew formula for Dynoxide
# Place this in a tap repo (e.g., nubo-db/homebrew-tap) as Formula/dynoxide.rb
# Install with: brew install nubo-db/tap/dynoxide
#
# NOTE: sha256 values are "PLACEHOLDER" in this template. They are replaced
# automatically by the release CI workflow (.github/workflows/homebrew.yml)
# when a new version is published. Do not fill them in manually.

class Dynoxide < Formula
  desc "Fast, lightweight drop-in replacement for DynamoDB Local, backed by SQLite"
  homepage "https://github.com/nubo-db/dynoxide"
  version "0.9.1"
  license any_of: ["MIT", "Apache-2.0"]

  on_macos do
    on_arm do
      url "https://github.com/nubo-db/dynoxide/releases/download/v#{version}/dynoxide-aarch64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER"
    end

    on_intel do
      url "https://github.com/nubo-db/dynoxide/releases/download/v#{version}/dynoxide-x86_64-apple-darwin.tar.gz"
      sha256 "PLACEHOLDER"
    end
  end

  on_linux do
    on_arm do
      url "https://github.com/nubo-db/dynoxide/releases/download/v#{version}/dynoxide-aarch64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER"
    end

    on_intel do
      url "https://github.com/nubo-db/dynoxide/releases/download/v#{version}/dynoxide-x86_64-unknown-linux-gnu.tar.gz"
      sha256 "PLACEHOLDER"
    end
  end

  def install
    bin.install "dynoxide"
  end

  test do
    assert_match version.to_s, shell_output("#{bin}/dynoxide --version")
  end
end
