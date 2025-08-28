import os
import time
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options as ChromeOptions
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
import requests
import pandas as pd
from bs4 import BeautifulSoup
import base64
import re
from webdriver_manager.chrome import ChromeDriverManager
from concurrent.futures import ThreadPoolExecutor, as_completed

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
CATEGORY_DIR = os.path.join(BASE_DIR, "silverjewelry", "bracelet")
CAT_OUTPUT_DIR = os.path.join(CATEGORY_DIR, "silveracc_output")
THUMBNAIL_DIR = os.path.join(CATEGORY_DIR, "썸네일")
DETAIL_IMAGES_DIR = os.path.join(CATEGORY_DIR, "상세사진")

if not os.path.exists(CAT_OUTPUT_DIR):
    os.makedirs(CAT_OUTPUT_DIR)
if not os.path.exists(THUMBNAIL_DIR):
    os.makedirs(THUMBNAIL_DIR)
if not os.path.exists(DETAIL_IMAGES_DIR):
    os.makedirs(DETAIL_IMAGES_DIR)

def setup_driver():
    chrome_options = ChromeOptions()
    chrome_options.add_argument("--no-sandbox")
    chrome_options.add_argument("--disable-dev-shm-usage")
    chrome_options.add_argument("--disable-gpu")
    user_agent_string = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    chrome_options.add_argument(f"user-agent={user_agent_string}")
    chrome_options.add_experimental_option("excludeSwitches", ["enable-logging"])
    chrome_options.add_argument("--incognito")
    chrome_options.page_load_strategy = 'normal'
    service = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=service, options=chrome_options)
    driver.set_window_size(1200, 800)
    return driver

def sanitize_filename(filename):
    return re.sub(r'[<>:"/\\|?*]', '_', filename)

def clean_image_url(url, base_url):
    if not url:
        return None
    
    if url.startswith("data:image"):
        return url
    elif url.startswith("//"):
        return "https:" + url
    elif url.startswith("/"):
        return base_url.rstrip('/') + url
    else:
        return url

def save_image(image_url, filename, folder_path):
    file_path = os.path.join(folder_path, filename)
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        if image_url.startswith("data:image"):
            match = re.match(r"data:image/(png|jpeg|jpg);base64,(.*)", image_url)
            if match:
                image_bytes = base64.b64decode(match.group(2))
                with open(file_path, 'wb') as out_file:
                    out_file.write(image_bytes)
            else:
                pass
        else:
            response = requests.get(image_url, stream=True, headers=headers)
            response.raise_for_status()
            with open(file_path, 'wb') as out_file:
                for chunk in response.iter_content(chunk_size=8192):
                    out_file.write(chunk)
        
        if os.path.exists(file_path) and os.path.getsize(file_path) > 0:
            pass
        else:
            pass
    except requests.exceptions.RequestException as e:
        status_code_info = f"상태 코드: {e.response.status_code}" if e.response is not None else "상태 코드 없음"
    except Exception as e:
        pass

def get_product_details(driver, product_info_list, product_url, product_name_on_list, product_price_on_list, thumbnail_url, thumbnail_counter, base_url, current_page_number):
    product_data = {
        '순위': thumbnail_counter,
        '페이지': current_page_number,
        '상품명': product_name_on_list,
        '썸네일 이미지 파일명': f"{thumbnail_counter}.jpg",
        '가격': product_price_on_list,
        '상품 상세': None,
        '상세 이미지 파일명': []
    }
    
    save_image(thumbnail_url, product_data['썸네일 이미지 파일명'], THUMBNAIL_DIR)
    print(f"썸네일 저장 완료: {product_data['썸네일 이미지 파일명']}")
    driver.get(product_url)
    
    try:
        try:
            expand_button = WebDriverWait(driver, 10).until(
                EC.element_to_be_clickable((By.XPATH, '//*[@id="INTRODUCE"]/div/div[3]/button'))
            )
            expand_button.click()
            print("'상세정보 펼쳐보기' 버튼을 클릭 완료")
        except:
            print("버튼을 찾지 못했습니다. 페이지 중간까지 스크롤을 시도합니다.")
            try:
                driver.execute_script("window.scrollTo(0, document.body.scrollHeight / 2);")
                time.sleep(1) 
                expand_button = WebDriverWait(driver, 10).until(
                    EC.element_to_be_clickable((By.XPATH, '//*[@id="INTRODUCE"]/div/div[3]/button'))
                )
                expand_button.click()
                print("스크롤 후 '상세정보 펼쳐보기' 버튼을 클릭 완료")
            except Exception as e:
                print(f"스크롤 후에도 버튼을 찾을 수 없습니다: {e}")

        detail_extraction_start = time.time()
        
        soup = BeautifulSoup(driver.page_source, 'html.parser')
        desc_container = soup.find('div', class_='LXGzUhHJC2')
        description_texts = []
        if desc_container:
            p_tags = desc_container.find_all('p')
            if p_tags:
                for p_tag in p_tags:
                    cleaned_text = p_tag.get_text(strip=True)
                    if cleaned_text:
                        description_texts.append(cleaned_text)
        product_data['상품 상세'] = "\n".join(description_texts) if description_texts else None
        detail_extraction_end = time.time()
        print(f"상품 상세 정보 추출 완료 - 소요시간: {detail_extraction_end - detail_extraction_start:.2f}초")
    except Exception as e:
        print(f"상품 상세 정보 추출 중 오류 발생: {e}")
        return None

    image_collection_start = time.time()
    all_detail_images_tags = soup.find_all('img', class_=['__cu_imgsize_800_800', 'se-inline-image-resource'])
    detail_image_urls = []
    
    for img_tag in all_detail_images_tags:
        img_to_process = img_tag.get('data-src') or img_tag.get('src')
        if img_to_process:
            cleaned_url = clean_image_url(img_to_process, base_url)
            if cleaned_url and cleaned_url.lower().endswith(('.jpg', '.jpeg', '.png', 'webp', 'gif')):
                detail_image_urls.append(cleaned_url)

    for i, img_url in enumerate(detail_image_urls, 1):
        filename = f"{thumbnail_counter}_{i}.jpg"
        save_image(img_url, filename, DETAIL_IMAGES_DIR)
        product_data['상세 이미지 파일명'].append(filename)
        print(f"상세 이미지 저장 완료: {filename}")

    image_collection_end = time.time()
    print(f"상세 이미지 URL 수집 및 저장 완료 ({len(detail_image_urls)}개) - {image_collection_end - image_collection_start:.2f}초")
    
    return product_data

def main():
    total_start_time = time.time()
    print("크롤링을 시작합니다.")
    driver = setup_driver()
    
    current_page_number = 1
    thumbnail_counter = 1
    
    csv_path = os.path.join(CAT_OUTPUT_DIR, "raw_data.csv")
    
    if os.path.exists(csv_path):
        os.remove(csv_path)
        print(f"기존 CSV 파일 '{csv_path}'을(를) 삭제했습니다.")
    
    cumulative_data = []

    try:
        while True:
            base_url = f"https://smartstore.naver.com/dadenda0/category/b2ce3fa6da7a4074b6e3dd2b1f2417ba?cp=1"
            print(f"사이트에 접속합니다: {base_url}")
            driver.get(base_url)

            print(f"\n======== {current_page_number} 페이지 크롤링 시작 ========")
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_element_located((By.CSS_SELECTOR, "li.Hz4XxKbt9h"))
                )
            except Exception as e:
                print("더 이상 상품이 없거나 페이지 로딩 오류로 인해 크롤링을 종료합니다.")
                break

            product_elements = driver.find_elements(By.CSS_SELECTOR, "li.Hz4XxKbt9h")
            if not product_elements:
                print("상품 목록을 찾을 수 없어 크롤링을 종료합니다.")
                break

            product_info_list = []
            for i, product in enumerate(product_elements):
                try:
                    info_container = product.find_element(By.CSS_SELECTOR, "div.jtczQG9UJQ")
                    thumbnail_element = product.find_element(By.CSS_SELECTOR, "img.eGeLGHztiu")
                    thumbnail_url = thumbnail_element.get_attribute('src')
                    
                    product_name_element = info_container.find_element(By.CSS_SELECTOR, 'strong.xSW7C99vO3')
                    price_element = info_container.find_element(By.CSS_SELECTOR, 'div.RIs7NC5ZLT')
                    price_text = price_element.text.strip()
                    product_price = int(price_text.replace('원', '').replace(',', '').strip())
                    product_name = product_name_element.text.strip()
                    product_link_element = product.find_element(By.CSS_SELECTOR, "div.Da08Est7iL > a")
                    product_url = product_link_element.get_attribute('href')
                    
                    product_info_list.append({
                        'index': i,
                        'name': product_name,
                        'price': product_price,
                        'thumbnail_url': thumbnail_url,
                        'product_url': product_url
                    })
                except Exception as e:
                    print(f"상품 목록 정보 추출 중 오류: {e}")
                    continue
            
            print(f"현재 페이지에서 {len(product_info_list)}개의 상품 정보를 수집했습니다.")

            for product_info in product_info_list:
                try:
                    # 함수 정의에 맞게 모든 인자를 개별적으로 전달
                    product_data = get_product_details(
                        driver, 
                        product_info_list, # 이 인자는 함수 내부에서 사용되지 않지만, 정의에 맞게 전달
                        product_info['product_url'], 
                        product_info['name'], 
                        product_info['price'], 
                        product_info['thumbnail_url'], 
                        thumbnail_counter, 
                        base_url, 
                        current_page_number
                    )
                    
                    if product_data:
                        print(f"상품 '{product_info['name']}' (순위 {thumbnail_counter}) 정보 추출 완료.")
                        
                        cumulative_data.append(product_data)
                        
                        df = pd.DataFrame(cumulative_data)
                        df.to_csv(csv_path, mode='w', index=False, encoding='utf-8-sig')
                        print(f"상품 '{product_info['name']}' (순위 {thumbnail_counter})까지의 모든 데이터를 CSV에 덮어쓰기 완료.")

                except Exception as e:
                    print(f"상품 '{product_info['name']}' (순위 {thumbnail_counter}) 처리 중 치명적인 오류: {e}")
                
                thumbnail_counter += 1
            break
            
    finally:
        if driver:
            driver.quit()
            print("WebDriver 종료.")
        
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        print("\n======== 전체 크롤링 완료 ========")
        print(f"총 소요 시간: {total_duration:.2f}초 ({total_duration/60:.2f}분)")

if __name__ == "__main__":
    main()