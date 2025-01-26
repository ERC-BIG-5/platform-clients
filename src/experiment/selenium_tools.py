import json
import os

import atexit
from dotenv import load_dotenv
from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver import Firefox
from selenium.webdriver.common.by import By
from selenium.webdriver.firefox.options import Options as FirefoxOptions
from selenium.webdriver.firefox.service import Service
from selenium.webdriver.remote.webdriver import WebDriver
from time import time, sleep
import browser_cookie3

def find_with_timeout(find_function, timeout=20):
    """
    Wraps a Selenium find element operation with timeout functionality.

    Args:
        find_function: Lambda or function that performs the find element operation
        timeout (int): Maximum time to wait for element in seconds

    Returns:
        The found WebElement or raises TimeoutError if timeout is reached

    Example:
        element = with_timeout(lambda: browser.find_element(By.TAG_NAME, "input"), timeout=20)
    """
    start_time = time()

    while True:
        try:
            return find_function()
        except NoSuchElementException as exc:
            if time() - start_time > timeout:
                raise TimeoutError(
                    f"Element not found after {timeout} seconds: {str(exc)}"
                )
            sleep(1)


def exit_handler_browser(browser: WebDriver):
    browser.close()
    browser.quit()


def get_browser(headless: bool = True) -> Firefox:
    options = FirefoxOptions()
    if headless:
        options.add_argument("-headless")
    geckodriver_path = "/snap/bin/firefox.geckodriver"  # specify the path to your geckodriver
    print(f"opening{headless if headless else ''} browser")
    browser = options
    browser = Firefox(options=options, service=Service(executable_path=geckodriver_path))
    atexit.register(exit_handler_browser, browser)
    return browser


def main():
    get_browser(headless=False)
    pass


def get_browser_cookies(domain: str, required_cookie_keys: list[str] = None):
    """Get Twitter cookies from your browser"""
    # Try to get cookies from multiple browsers - modify as needed
    browsers = [
        browser_cookie3.firefox,
        browser_cookie3.librewolf
    ]
    collected_cookies = []
    for browser in browsers:
        try:
            cookies = browser(domain_name=domain)
            cookie_list = []
            for cookie in cookies:
                cookie_dict = {
                    'name': str(cookie.name),
                    'value': str(cookie.value),
                    'domain': str(cookie.domain),
                    'path': str(cookie.path)
                }

                # Only add non-None optional attributes
                if hasattr(cookie, 'secure') and cookie.secure is not None:
                    cookie_dict['secure'] = bool(cookie.secure)
                if hasattr(cookie, 'expires') and cookie.expires:
                    cookie_dict['expiry'] = int(cookie.expires)
                if hasattr(cookie, 'http_only'):
                    cookie_dict['httpOnly'] = bool(cookie.http_only)

                cookie_list.append(cookie_dict)

            # Check for required cookies if specified
            if required_cookie_keys:
                cookie_names = {cookie['name'] for cookie in cookie_list}
                if all(key in cookie_names for key in required_cookie_keys):
                    return cookie_list
            else:
                return cookie_list
        except:
            continue
    return None


if __name__ == "__main__":
    main()
