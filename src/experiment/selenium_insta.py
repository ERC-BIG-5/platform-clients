from selenium.common.exceptions import NoSuchElementException
from selenium.webdriver.common.by import By

from src.experiment.selenium_tools import get_browser_cookies, get_browser, find_with_timeout


def extract_instagram_post_data(post_element):
    """
    Extract data from a Selenium WebElement containing an Instagram post
    """
    try:
        data = {
            "username": None,
            "post_id": None,
            "likes": None,
            "comments_count": None,
            "post_text": None,
            "is_sponsored": False,
            "profile_picture_url": None,
            "post_image_url": None
        }

        # Username using find_element
        try:
            username_div = post_element.find_element(By.CSS_SELECTOR, "span[style*='line-height: 18px']")
            data["username"] = username_div.text.strip()
        except NoSuchElementException:
            pass

        # Check if sponsored
        try:
            sponsored = post_element.find_element(By.XPATH, ".//span[text()='Sponsored']")
            data["is_sponsored"] = True
        except NoSuchElementException:
            pass

        # Likes count
        try:
            likes_element = post_element.find_element(By.XPATH, ".//span[contains(@class, 'html-span')]/ancestor::a[contains(@href, '/liked_by/')]")
            likes_text = likes_element.text.split()[0]
            data["likes"] = likes_text.replace(',', '')
        except NoSuchElementException:
            pass

        # Comments count
        try:
            comments = post_element.find_element(By.XPATH, ".//a[contains(@href, '/comments/')]")
            comments_text = comments.text
            if 'View all' in comments_text:
                data["comments_count"] = comments_text.split()[2]
        except NoSuchElementException:
            pass

        # Post text
        try:
            text_element = post_element.find_element(By.CSS_SELECTOR, "span._ap3a._aaco._aacu._aacx._aad7._aade")
            data["post_text"] = text_element.text.strip()
        except NoSuchElementException:
            pass

        # Post ID from comments URL
        try:
            comments_link = post_element.find_element(By.XPATH, ".//a[contains(@href, '/p/')]")
            href = comments_link.get_attribute('href')
            data["post_id"] = href.split('/p/')[1].split('/')[0]
        except NoSuchElementException:
            pass

        # Profile picture URL
        try:
            profile_img = post_element.find_element(By.CSS_SELECTOR, "img.x972fbf")
            data["profile_picture_url"] = profile_img.get_attribute('src')
        except NoSuchElementException:
            pass

        # Post image URL
        try:
            post_img = post_element.find_element(By.CSS_SELECTOR, "img.x5yr21d")
            data["post_image_url"] = post_img.get_attribute('src')
        except NoSuchElementException:
            pass

        return data

    except Exception as e:
        print(f"Error extracting post data: {e}")
        return None

# Usage example:
"""
# Assuming you already have your WebElements in a list called 'post_elements'
for post_element in post_elements:
    post_data = extract_instagram_post_data(post_element)
    print(post_data)
"""

def main():
    cookies = get_browser_cookies(".instagram.com")
    print(cookies)
    browser = get_browser(False)
    browser.get("https://instagram.com")
    browser.implicitly_wait(5)
    # set cookies
    for cookie in cookies:
        try:
            browser.add_cookie(cookie)
        except Exception as e:
            print(f"Could not set cookie {cookie['name']}: {e}")
    browser.refresh()
    elements = find_with_timeout(lambda: browser.find_elements(By.TAG_NAME,"article"))
    for elem in elements:
        print(extract_instagram_post_data(elem))
    pass

if __name__ == "__main__":
    main()
