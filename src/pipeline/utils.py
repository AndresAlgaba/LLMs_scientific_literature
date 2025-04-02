import os
import re
import time
from typing import Dict, List, Optional

import requests
from requests.exceptions import RequestException


def semantic_scholar_search(
    query: str,
    number_of_papers: int = 1,
    fields: str = "title,authors,year,venue,abstract,externalIds",
    offset: int = 0,
    max_retries: int = 3,
    base_delay: float = 1.0,
) -> Optional[List[Dict]]:
    """
    Search for papers on Semantic Scholar with exponential backoff.

    Args:
        query (str): The search query.
        number_of_papers (int): Number of papers to retrieve. Default is 3.
        fields (str): Comma-separated list of fields to return.
        Default includes title, authors, year, venue, abstract, and embedding.
        offset (int): Starting index for search results. Default is 0.
        max_retries (int): Maximum number of retry attempts. Default is 5.
        base_delay (float): Base delay for exponential backoff in seconds. Default is 1.0.

    Returns:
        Optional[List[Dict]]: List of paper data dictionaries, or None if no results found.

    Raises:
        requests.RequestException: If there's an error with the API request after all retries.
        ValueError: If the API key is missing or invalid.
    """
    base_url = "https://api.semanticscholar.org/graph/v1/paper/search"
    timeout = 100
    api_key = os.getenv("SEMANTIC_SCHOLAR_KEY")
    fields = re.sub(r"\s+", "", fields, flags=re.UNICODE)

    if not api_key:
        raise ValueError(
            "Semantic Scholar API key is missing. Set the SEMANTIC_SCHOLAR_KEY environment variable."
        )

    headers = {"x-api-key": api_key}
    params = {
        "query": query.replace("-", " "),
        "limit": number_of_papers,
        "offset": offset,
        "fields": fields,
    }

    for attempt in range(max_retries):
        try:
            response = requests.get(
                base_url, params=params, headers=headers, timeout=timeout
            )
            response.raise_for_status()
            results = response.json()
            return results.get("data") if results.get("total", 0) > 0 else None
        except RequestException as e:
            if attempt == max_retries - 1:
                raise RequestException(
                    f"Error querying Semantic Scholar API after {max_retries} attempts: {str(e)}"
                )

            delay = base_delay * (2**attempt)
            print(f"Attempt {attempt + 1} failed. Retrying in {delay:.2f} seconds...")
            time.sleep(delay)

    return None
