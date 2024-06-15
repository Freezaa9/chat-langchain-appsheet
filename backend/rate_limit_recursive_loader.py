import time
import asyncio
import random
from typing import Set, Iterator, List, Optional
from langchain_core.documents import Document
from langchain.document_loaders import RecursiveUrlLoader
import aiohttp

class RateLimitedRecursiveUrlLoader(RecursiveUrlLoader):
    def __init__(self, *args, min_rate_limit: float = 1.0, max_rate_limit: float = 3.0, **kwargs):
        """Initialize with additional rate_limit parameters.

        Args:
            min_rate_limit: Minimum time interval between requests in seconds.
            max_rate_limit: Maximum time interval between requests in seconds.
        """
        super().__init__(*args, **kwargs)
        self.min_rate_limit = min_rate_limit
        self.max_rate_limit = max_rate_limit

    def _get_random_delay(self) -> float:
        """Generate a random delay within the specified rate limit range."""
        random_delay = random.uniform(self.min_rate_limit, self.max_rate_limit)
        print("Waiting: "+str(random_delay))
        return random_delay

    def _get_child_links_recursive(
        self, url: str, visited: Set[str], *, depth: int = 0
    ) -> Iterator[Document]:
        """Recursively get all child links starting with the path of the input URL, with rate limiting.

        Args:
            url: The URL to crawl.
            visited: A set of visited URLs.
            depth: Current depth of recursion. Stop when depth >= max_depth.
        """
        time.sleep(self._get_random_delay())  # Add random sleep to respect rate limiting
        yield from super()._get_child_links_recursive(url, visited, depth=depth)

    async def _async_get_child_links_recursive(
        self,
        url: str,
        visited: Set[str],
        *,
        session: Optional[aiohttp.ClientSession] = None,
        depth: int = 0,
    ) -> List[Document]:
        """Recursively get all child links starting with the path of the input URL, with rate limiting.

        Args:
            url: The URL to crawl.
            visited: A set of visited URLs.
            depth: To reach the current url, how many pages have been visited.
        """
        await asyncio.sleep(self._get_random_delay())  # Add random sleep to respect rate limiting
        return await super()._async_get_child_links_recursive(url, visited, session=session, depth=depth)

# Example usage
# if __name__ == "__main__":
#     url_loader = RateLimitedRecursiveUrlLoader(url="https://example.com", min_rate_limit=2.0, max_rate_limit=5.0)
#     for document in url_loader.lazy_load():
#         print(document)
