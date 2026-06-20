import requests
import urllib.request


def check_url(name: str, url: str) -> None:
    print(f"Checking {name}...")
    try:
        with urllib.request.urlopen(url, timeout=20) as response:
            print(f"{name}: {response.status}")
            print(response.read(120))
    except Exception as error:
        print(f"{name}: FAILED")
        print(type(error).__name__, error)


check_url(
    "Hacker News topstories",
    "https://hacker-news.firebaseio.com/v0/topstories.json",
)

check_url(
    "DEV.to top articles",
    "https://dev.to/api/articles?per_page=1&top=7",
)

HEADERS = {
    "User-Agent": "Mozilla/5.0 tech-news-data-pipeline/1.0",
}


URLS = [
    "https://hacker-news.firebaseio.com/v0/topstories.json",
    "https://dev.to/api/articles?per_page=1&top=7",
]


for url in URLS:
    print("=" * 80)
    print("Checking:", url)

    try:
        response = requests.get(
            url,
            headers=HEADERS,
            timeout=60,
        )

        print("Status:", response.status_code)
        print("First bytes:", response.text[:200])

    except requests.RequestException as error:
        print(type(error).__name__)
        print(error)