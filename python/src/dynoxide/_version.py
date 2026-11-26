# The Dynoxide release whose binaries the wheels carry. It moves with VERSION
# at the root of the repository, and scripts/check-versions.sh fails if the
# two differ.
ENGINE_VERSION = "1.8.1"

# The package's own version: the engine version in PEP 440 form. The release
# workflow rewrites this line for a post-release such as 1.8.0.post1.
__version__ = "1.8.1"
