def assert_response_status(url: str, response, expected_status_code: int):
    assert response.status_code == expected_status_code, (
        f"API call failed.\n"
        f"Expected status code: {expected_status_code}\n"
        f"Actual status code: {response.status_code}\n"
        f"URL: {url}\n"
        f"Response text: {response.text[:100]}..."
    )
