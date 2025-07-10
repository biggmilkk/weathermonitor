import requests
from bs4 import BeautifulSoup

def scrape(url):
    try:
        res = requests.get(url, timeout=5)
        soup = BeautifulSoup(res.text, "html.parser")

        location = soup.find("h2", class_="panel-title")
        temp = soup.find("p", class_="myforecast-current-lrg")
        condition = soup.find("p", class_="myforecast-current")

        return {
            "location": location.text.strip() if location else "Unknown",
            "temperature": temp.text.strip() if temp else "N/A",
            "condition": condition.text.strip() if condition else "",
            "source": url
        }
    except Exception as e:
        return {"error": str(e), "source": url}
