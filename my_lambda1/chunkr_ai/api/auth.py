import platform
import sys

class HeadersMixin:
    """Mixin class for handling authorization headers"""
    _api_key: str = ""

    def get_api_key(self) -> str:
        """Get the API key"""
        if not hasattr(self, "_api_key") or not self._api_key:
            raise ValueError("API key not set")
        return self._api_key

    def _headers(self) -> dict:
        """Generate authorization headers and version information"""
        # Import version from the main package to avoid duplication
        from .. import __version__
        user_agent = f"chunkr-ai/{__version__} (Python/{sys.version.split()[0]}; {platform.system()}/{platform.release()})"
        return {
            "Authorization": self.get_api_key(),
            "User-Agent": user_agent
        }
