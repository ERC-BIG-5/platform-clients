import asyncio
from typing import Optional

from twscrape import AccountsPool
import browser_cookie3
import json
import sqlite3

from tools.env_root import root


def get_twitter_cookies():
    """Get Twitter cookies from your browser"""
    # Try to get cookies from multiple browsers - modify as needed
    browsers = [
        browser_cookie3.chrome,
        browser_cookie3.firefox,
        browser_cookie3.edge,
        browser_cookie3.safari,
        browser_cookie3.librewolf
    ]

    for browser in browsers:
        print(browser)
        try:
            cookies = browser(domain_name='.x.com')
            cookie_dict = {cookie.name: cookie.value for cookie in cookies}

            # Check for required cookies
            if 'ct0' in cookie_dict and 'auth_token' in cookie_dict:
                return cookie_dict
        except:
            continue
    return None


def add_to_twscrape(username, password, cookies):
    """Add cookies to twscrape accounts database"""
    pool = AccountsPool(root() / "accounts.db")

    try:
        asyncio.run(pool.get(username))
        print("deleting existing account")
        # todo, does not work!?!?
        asyncio.run(pool.delete_accounts(username))
    except ValueError:
        pass

    # Convert cookies to required format
    account_data = {
        "username": username,
        "cookies": json.dumps(cookies),
        "password":password,
        "email": "",  # Can be left empty
        "email_password": "",  # Can be left empty
        "proxy": None
    }

    # Add account to pool
    asyncio.run(pool.add_account(**account_data))
    return True


def main(username: Optional[str] = None, pasword: Optional[str] = None):
    # Get cookies from browser
    cookies = get_twitter_cookies()
    if not cookies:
        print("Could not find Twitter cookies in any browser")
        return

    # Add to twscrape
    try:
        print(json.dumps(cookies, indent=2))
        print(json.dumps(cookies))
        if username:
            add_to_twscrape(username,pasword, cookies)
        #print("Successfully added cookies to twscrape")
    except Exception as e:
        print(f"Failed to add cookies: {str(e)}")


if __name__ == "__main__":
    main()