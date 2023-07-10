from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.commom.exceptions import NoSuchElementException
import time, os

url = 'https://www.korea.kr/archive/expDocView.do?docId='
delete_path = './Downloads/'

for i in range(1500):
    driver = webdriver.Firefox()
    urls = url + str(40545 - i)
    driver.get(urls)

    try:
        element = driver.find_element(By.CLASS_NAME, 'down')
        # element.click()
        driver.execute_script("arguments[0].click();", element)

        time.sleep(10)

        upload(key_path, bucket_name, folder_path)

        for file_name in os.listdir(delete_path):
            file_path = os.path.join(delete_path, file_name)
            if os.path.isfile(file_path):
                os.remove(file_path)
    except NoSuchElementException:
        pass

    driver.quit()
