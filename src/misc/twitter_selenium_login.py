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


def make_login(browser) -> dict[str,str]:
    """

    :param browser:
    :return: the status div
    """
    load_dotenv()
    browser.get("https://x.com/i/flow/login")

    username_input = find_with_timeout(lambda: browser.find_element(By.TAG_NAME, "input"), 20)

    sleep(0.4)
    username_input.send_keys(os.getenv("TWITTER_USERNAME"))

    buttons = browser.find_elements(By.TAG_NAME, "button")
    next_button = None
    for button in buttons:
        if button.accessible_name == "Next":
            next_button = button
            break
    next_button.click()
    sleep(1)
    inputs = []
    while len(inputs) == 0:
        inputs = find_with_timeout(lambda: browser.find_elements(By.TAG_NAME, "input"), 20)
    if len(inputs) == 1:
        password_input = inputs[0]
    else:
        password_input = inputs[1]
        if not password_input.accessible_name == "Password Reveal password":
            print("probably something wrong...")
    password_input.send_keys(os.getenv("TWITTER_PASSWORD"))
    sleep(0.2)

    buttons = browser.find_elements(By.TAG_NAME, "button")
    next_button = None
    for button in buttons:
        btn_txt = button.accessible_name
        # print(btn_txt)
        if btn_txt in ["Next", "Log in"]:
            next_button = button
            break
    # print(next_button)
    if not next_button:
        pass
    else:
        next_button.click()
    sleep(2)
    inputs = []
    while len(inputs) == 0:
        inputs = find_with_timeout(lambda: browser.find_elements(By.TAG_NAME, "input"), 20)
    for input_ in inputs:
        if input_.accessible_name == 'Confirmation code':
            confirmation_code = input("get confirmation_code")
            input_.send_keys(confirmation_code)
    # todo: get the next button and click it

    cookies = {c["name"]: c["value"] for c in browser.get_cookies()}
    # todo ready to plug them in again...
    return cookies

if __name__ == "__main__":
    print(json.dumps(make_login(get_browser(False)), indent=2))
