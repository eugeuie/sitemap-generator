import sys, time, logging
import requests
from collections import deque
from urllib.parse import urljoin, urlparse
from bs4 import BeautifulSoup
from lxml import etree
from tabulate import tabulate
from typing import Optional, Tuple, Callable, TypeVar, Any, cast


logging.basicConfig(
    # filename="logfile.log",
    level=logging.INFO,
    format="[%(threadName)s, %(asctime)s, %(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
F = TypeVar("F", bound=Callable[..., Any])


def measure_time(method: F) -> F:
    def timer(self, *args, **kwargs) -> None:
        start = time.time()
        method(self, *args, **kwargs)
        self.processing_time = time.time() - start

    return cast(F, timer)


class Crawler:
    def __init__(self, root_url: str) -> None:
        self.root_url = root_url
        self.processing_time = 0
        self.site_urls_set = set()
        self._site_urls_deque = deque([root_url])
        self._seen_urls_set = set()

    def _normalize_url(self, url: str) -> Optional[str]:
        url = urljoin(self.root_url, urlparse(url).path)
        return url if url != self.root_url else None

    def _is_site_url(self, url: str) -> Optional[bool]:
        url = urljoin(url, urlparse(url).path)
        root_url_parsed = urlparse(self.root_url)
        url_parsed = urlparse(url)
        root_url_scheme, root_url_netloc = (
            root_url_parsed.scheme,
            root_url_parsed.netloc,
        )
        url_scheme, url_netloc, url_path = (
            url_parsed.scheme,
            url_parsed.netloc,
            url_parsed.path,
        )
        return (
            url_scheme == root_url_scheme
            and url_netloc == root_url_netloc
            or url_scheme == ""
            and url_netloc == ""
        ) and (
            url_path.startswith("/")
            and len(url_path) > 1
            or "/" not in url_path
            and len(url_path) > 0
        )

    def _fetch_url(self, url: str) -> Optional[bool]:
        try:
            response = requests.get(url)

        except Exception as e:
            logging.info(f"URL [{url}] caused an exception [{e}]")
            return None

        return response if response.status_code == requests.codes.ok else None
    
    @measure_time
    def _get_site_urls_dfs(self, url: str = None) -> None:
        if url is None:
            url = self.root_url

        response = self._fetch_url(url)

        if response:
            soup = BeautifulSoup(response.content, "html.parser")

            for url in soup.find_all("a"):
                url = url.get("href")
                if url and self._is_site_url(url):
                    url = self._normalize_url(url)
                    if (
                        url
                        and url not in self.site_urls_set
                        and url not in self._seen_urls_set
                    ):
                        self.site_urls_set.add(url)
                        self._seen_urls_set.add(url)
                        logging.info(
                            f"{len(self.site_urls_set)} URLs found, ADDED [{url}]"
                        )
                        self._get_site_urls_dfs(url)

        else:
            self.site_urls_set.discard(url)
            logging.info(
                f"{len(self.site_urls_set)} URLs found, DELETED BAD URL [{url}]"
            )

    # @measure_time
    def _get_site_urls_bfs(self) -> None:
        start = time.time()
        while len(self._site_urls_deque):
            url = self._site_urls_deque.popleft()
            self._seen_urls_set.add(url)

            response = self._fetch_url(url)

            if response:
                if url != self.root_url:
                    self.site_urls_set.add(url)
                    logging.info(f"{len(self.site_urls_set)} URLs found, ADDED [{url}]")

                soup = BeautifulSoup(response.content, "html.parser")

                for url in soup.find_all("a"):
                    url = url.get("href")
                    if url and self._is_site_url(url):
                        url = self._normalize_url(url)
                        if (
                            url
                            and url not in self._site_urls_deque
                            and url not in self._seen_urls_set
                        ):
                            self._site_urls_deque.append(url)

            else:
                self._seen_urls_set.add(url)
                continue

            self.processing_time = time.time() - start
            if self.processing_time > 1e3:
                break

    def run(self) -> Tuple[set, float]:
        self._get_site_urls_bfs()
        return self.site_urls_set, self.processing_time


class SitemapGenerator:
    def __init__(self, root_url) -> None:
        self.root_url = root_url
        self.processing_time = 0
        self.site_urls_count = 0
        self.sitemap_filename = f"sitemaps/{urlparse(root_url).netloc}_sitemap.xml"
        self._crawler = Crawler(root_url)
        self._site_urls = set()

    def _write_txt(self) -> None:
        with open(f"{self.sitemap_filename.split('.')[0]}.txt", "wt", encoding="utf-8") as f:
            f.writelines("\n".join(self._site_urls))

    def _write_xml(self) -> None:
        urlset = etree.Element("urlset")
        urlset.attrib["xmlns"] = "http://www.sitemaps.org/schemas/sitemap/0.9"

        for site_url in self._site_urls:
            url = etree.Element("url")
            loc = etree.Element("loc")
            loc.text = site_url
            url.append(loc)
            urlset.append(url)

        with open(self.sitemap_filename, "w", encoding="utf-8") as f:
            f.writelines(
                etree.tostring(
                    urlset, pretty_print=True, encoding="unicode", method="xml"
                )
            )

    def run(self) -> Tuple[str, float, int, str]:
        self._site_urls, self.processing_time = self._crawler.run()
        self.site_urls_count = len(self._site_urls)
        # self._write_txt()
        self._write_xml()
        logging.info(
            f"\nSITE: {self.root_url}\nPROCESSING TIME: {self.processing_time:.2f} sec\nURLS FOUND: {self.site_urls_count}\nSITEMAP FILENAME: {self.sitemap_filename}\n\n"
        )
        return self.root_url, round(self.processing_time, 2), self.site_urls_count, self.sitemap_filename


def main():
    urls = [
        "http://crawler-test.com/",
        "http://google.com/",
        "https://vk.com",
        "https://yandex.ru",
        "https://stackoverflow.com",
        # "https://www.apple.com/",
        # "http://mathprofi.ru/"
    ]
    headers = ['site', 'processing time (sec)', 'URLs count', 'sitemap filename']
    table = []
    for url in urls:
        sitemap_generator = SitemapGenerator(url)
        results = sitemap_generator.run()
        table.append(list(results))
    
    with open('results.md', 'w', encoding='utf-8') as f:
        f.writelines(tabulate(table, headers, tablefmt="github"))


if __name__ == "__main__":
    # url = sys.argv[1]
    # sitemap_generator = SitemapGenerator(url)
    # sitemap_generator.run()
    main()
