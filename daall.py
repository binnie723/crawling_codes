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
CATEGORY_DIR = os.path.join(BASE_DIR, "watch")
CAT_OUTPUT_DIR = os.path.join(CATEGORY_DIR, "watch_output")
THUMBNAIL_DIR = os.path.join(CATEGORY_DIR, "썸네일")
DETAIL_IMAGES_DIR = os.path.join(CATEGORY_DIR, "상세사진")

if not os.path.exists(CAT_OUTPUT_DIR):
    os.makedirs(CAT_OUTPUT_DIR)
if not os.path.exists(THUMBNAIL_DIR):
    os.makedirs(THUMBNAIL_DIR)
if not os.path.exists(DETAIL_IMAGES_DIR):
    os.makedirs(DETAIL_IMAGES_DIR)
    
print("--- 디렉토리 설정 완료 ---")

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
    print("--- WebDriver 설정 완료 ---")
    return driver

def save_image(image_url, filename, folder_path):
    if not image_url.startswith("data:image") and image_url.startswith("//"):
        image_url = "https:" + image_url
    
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36"
    }
    
    try:
        file_path = os.path.join(folder_path, filename)
        if image_url.startswith("data:image"):
            match = re.match(r"data:image/(png|jpeg|jpg);base64,(.*)", image_url)
            if match:
                base64_data = match.group(2)
                image_bytes = base64.b64decode(base64_data)
                with open(file_path, 'wb') as out_file:
                    out_file.write(image_bytes)
                print(f"  > BASE64 이미지 저장: {filename}")
            else:
                print(f"  > BASE64 형식 불일치: {image_url[:50]}...")
                pass
        else:
            response = requests.get(image_url, stream=True, headers=headers)
            response.raise_for_status()
            with open(file_path, 'wb') as out_file:
                out_file.write(response.content)
            print(f"  > URL 이미지 저장: {filename}")

        if os.path.exists(file_path):
            file_size = os.path.getsize(file_path)
            if file_size == 0:
                print(f"  > 파일 크기가 0바이트입니다: {filename}")
                pass
            else:
                pass
        else:
            print(f"  > 파일 저장 실패: {filename}")
            pass

    except requests.exceptions.RequestException as e:
        print(f"  > 이미지 다운로드 실패 (RequestsError): {filename}, URL: {image_url}, 오류: {e}")
    except Exception as e:
        print(f"  > 이미지 다운로드 실패 (일반 오류): {filename}, URL: {image_url}, 오류: {e}")


def main(): 
    total_start_time = time.time()
    driver = setup_driver()
    all_product_data = []
    
    save_interval = 5
    last_save_count = 0
    current_page_number = 1
    thumbnail_counter = (current_page_number - 1)*20 + 1
    base_url = "https://thedaall-dn.com/category/watch/23/"
    
    try:
        print(f"\n--- 웹사이트 접속: {base_url} ---")
        driver.get(base_url)
        crawled_product_ids = set()
        
        while True:
            page_start_time = time.time()
            print(f"\n--- {current_page_number} 페이지 상품 목록 스크래핑 시작 ---")
            try:
                WebDriverWait(driver, 30).until(
                    EC.presence_of_all_elements_located((By.CSS_SELECTOR, 'ul.prdList li'))
                )
            except Exception:
                print("상품 목록을 찾을 수 없습니다. 스크래핑을 종료합니다.")
                break
            
            products_on_page = driver.find_elements(By.CSS_SELECTOR, "ul.prdList.grid4 li")
            
            newly_found_products = []
            for product in products_on_page:
                try:
                    product_id = product.get_attribute('id')
                    if product_id and product_id not in crawled_product_ids:
                        newly_found_products.append(product)
                        crawled_product_ids.add(product_id)
                except Exception:
                    continue
            
            if not newly_found_products:
                print(f"{current_page_number} 페이지에서 새로운 상품을 찾을 수 없습니다. 스크래핑을 종료합니다.")
                break

            product_info_list = []
            for i, product in enumerate(newly_found_products):
                absolute_index = len(crawled_product_ids) - len(newly_found_products) + i
                try:
                    thumbnail_element = product.find_element(By.CSS_SELECTOR, 'div.thumbnail a img')
                    thumbnail_url = thumbnail_element.get_attribute('src')
                    
                    product_name_element = product.find_element(By.CSS_SELECTOR, 'strong.name')
                    product_name = product_name_element.text.strip()
                    
                    product_link_element = product.find_element(By.CSS_SELECTOR, 'div.thumbnail a')
                    product_url = product_link_element.get_attribute('href')
                    
                    product_info_list.append({
                        'index': absolute_index,
                        'name': product_name,
                        'thumbnail_url': thumbnail_url,
                        'product_url': product_url
                    })
                except Exception as e:
                    print(f"상품 목록 정보 추출 실패: {e}")
                    continue
            
            for product_info in product_info_list:
                i = product_info['index']
                product_name_on_list = product_info['name']
                thumbnail_url = product_info['thumbnail_url']
                product_url = product_info['product_url']
                
                product_start_time = time.time()
                current_product_data_raw = {}
                
                try:
                    print(f"\n--- [{thumbnail_counter}번 상품] 상품명: {product_name_on_list} ---")
                    current_product_data_raw['순위'] = thumbnail_counter
                    current_product_data_raw['페이지'] = current_page_number
                    current_product_data_raw['상품명'] = product_name_on_list
                    thumbnail_filename = f"{thumbnail_counter}.jpg"
                    print(f"  > 썸네일 이미지 다운로드 시작...")
                    save_image(thumbnail_url, thumbnail_filename, folder_path=THUMBNAIL_DIR)
                    current_product_data_raw['썸네일 이미지 파일명'] = thumbnail_filename
                    
                    print(f"  > 상품 상세 페이지 접속: {product_url}")
                    product_page_start = time.time()
                    driver.get(product_url)
                    time.sleep(0.1)
                    
                    soup = BeautifulSoup(driver.page_source, 'html.parser')
                    
                    # 💡 상품 가격 정보 추출 (새로운 ID 사용)
                    price_element = soup.find('strong', id='span_product_price_text')
                    product_price = '가격 정보 없음'
                    if price_element:
                        product_price = price_element.text.strip()
                    
                    current_product_data_raw['상품가격'] = product_price
                    print(f"  > 상품 가격 추출 완료: {product_price}")

                    prd_detail_div = soup.find('div', id='prdDetail')
                    if prd_detail_div:
                        detail_text = prd_detail_div.get_text(separator="\n", strip=True)
                        current_product_data_raw['상품 상세'] = detail_text
                        print("  > 상품 상세 텍스트 추출 완료")
                    else:
                        current_product_data_raw['상품 상세'] = None
                        print("  > 상품 상세 텍스트를 찾을 수 없습니다.")
                        
                    all_detail_images_tags = soup.find_all('img')
                    detail_image_urls = []
                    for img_tag in all_detail_images_tags:
                        img_to_process = img_tag.get('ec-data-src') or img_tag.get('src')
                        if img_to_process:
                            if img_to_process.startswith("data:image"):
                                continue
                            if img_to_process.startswith("//"):
                                img_to_process = "https:" + img_to_process
                            elif not img_to_process.startswith('http'):
                                img_to_process = base_url.rstrip('/') + img_to_process
                            
                            if (('/detail/' in img_to_process or '/product/' in img_to_process) and 
                                img_to_process.lower().endswith(('.jpg', '.jpeg', '.png'))):
                                detail_image_urls.append(img_to_process)

                    detail_image_urls = list(dict.fromkeys(detail_image_urls))
                    
                    if detail_image_urls:
                        print(f"  > 상세 이미지 {len(detail_image_urls)}개 다운로드 시작 (멀티스레딩)")
                        def download_single_image(url_and_index):
                            url, index = url_and_index
                            detail_image_filename = f"{thumbnail_counter}_{index}.jpg"
                            time.sleep(0.5)
                            save_image(url, detail_image_filename, folder_path=DETAIL_IMAGES_DIR)
                            return detail_image_filename
                        
                        url_index_pairs = [(url, idx + 1) for idx, url in enumerate(detail_image_urls)]
                        
                        with ThreadPoolExecutor(max_workers=5) as executor:
                            futures = [executor.submit(download_single_image, pair) for pair in url_index_pairs]
                            downloaded_files = [future.result() for future in as_completed(futures)]
                        print("  > 상세 이미지 다운로드 완료")
                    else:
                        print("  > 상세 이미지를 찾을 수 없습니다.")
                        
                    all_product_data.append(current_product_data_raw)
                    
                    current_count = len(all_product_data)
                    should_save = (current_count - last_save_count >= save_interval or i == len(product_info_list) - 1)
                    
                    if should_save:
                        print(f"  > [{current_count}개 상품] raw_data.csv 파일에 중간 저장...")
                        df = pd.DataFrame(all_product_data)
                        csv_path = os.path.join(CAT_OUTPUT_DIR, "raw_data.csv")
                        df.to_csv(csv_path, index=False, encoding='utf-8-sig')
                        last_save_count = current_count

                    driver.back()
                    print(f"  > 상품 상세 페이지에서 목록 페이지로 복귀. 소요 시간: {time.time() - product_page_start:.2f}초")
                except Exception as e:
                    print(f"  > [{thumbnail_counter}번 상품] 처리 중 오류 발생: {e}")
                    try:
                        driver.back()
                    except:
                        print("  > 목록 페이지로 돌아가기 실패. 다음 상품으로 넘어갑니다.")
                        pass
                    continue
                
                thumbnail_counter += 1
            
            page_navigation_start = time.time()
            more_button_xpath = '//div[contains(@class, "xans-product-listmore")]/a[contains(@class, "btnMore")]'
            
            try:
                next_page_button = WebDriverWait(driver, 10).until(
                    EC.presence_of_element_located((By.XPATH, more_button_xpath))
                )
                print(f"\n[{current_page_number} 페이지] '더보기' 버튼 클릭...")
                driver.execute_script("arguments[0].click();", next_page_button)
                time.sleep(2)
            except Exception:
                print(f"\n[{current_page_number} 페이지] '더보기' 버튼을 찾을 수 없습니다. 스크래핑을 종료합니다.")
                break
            
            current_page_number += 1
            print(f"[{current_page_number-1} 페이지] 처리 완료. 총 소요 시간: {time.time() - page_start_time:.2f}초")
            
    finally:
        if driver:
            driver.quit()
        
        if all_product_data and len(all_product_data) > last_save_count:
            print("\n--- 스크래핑 종료. 최종 데이터 저장 ---")
            df = pd.DataFrame(all_product_data)
            csv_path = os.path.join(CAT_OUTPUT_DIR, "raw_data.csv")
            df.to_csv(csv_path, index=False, encoding='utf-8-sig')
        
        total_end_time = time.time()
        total_duration = total_end_time - total_start_time
        print(f"\n--- 스크래핑 작업 완료! 총 소요 시간: {total_duration:.2f}초 ---")

if __name__ == "__main__":
    main()