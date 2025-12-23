from .api.chunkr import Chunkr
from importlib import metadata

# Get version from installed package metadata
try:
    __version__ = metadata.version("chunkr-ai")
except metadata.PackageNotFoundError:
    # package is not installed
    print("Error: chunkr-ai package not found. contact team@chunkr.ai")
    __version__ = "unknown"

__all__ = ["Chunkr", "__version__"]
