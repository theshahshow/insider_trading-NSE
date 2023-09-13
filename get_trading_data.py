from requests_html import HTMLSession
import os

def get_data():
    # Create a session
    session = HTMLSession()

    # Define the headers
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/88.0.4324.182 Safari/537.36",
        "Accept-Language": "en-US,en;q=0.9",
        "Accept-Encoding": "gzip, deflate, br",
        "Referer": "https://www.nseindia.com/",
    }

    # Set the headers
    session.headers.update(headers)

    # Get the main page first to start a session and set any necessary cookies
    main_page = session.get("https://www.nseindia.com/companies-listing/corporate-filings-insider-trading")

    # Now get the data from the API
    response = session.get("https://www.nseindia.com/api/corporates-pit")

    # Check if the request was successful
    if response.status_code == 200:
        data = response.json()
        return data
    else:
        error_text = f"Request failed with status code {response.status_code}"
        return error_text