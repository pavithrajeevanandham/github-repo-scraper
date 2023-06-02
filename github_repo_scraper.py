import pandas as pd
from bs4 import BeautifulSoup
import requests
import os
import time
import re


def stripit(text):
    return re.sub('\s+', ' ', text)


def status_log(r):
    """Pass response as a parameter to this function"""
    url_log_file = 'url_log.txt'
    if not os.path.exists(os.getcwd() + '\\' + url_log_file):
        with open(url_log_file, 'w') as f:
            f.write('url, status_code\n')
    with open(url_log_file, 'a') as file:
        file.write(f'{r.url}, {r.status_code}\n')


def retry(func, retries=3):
    """Decorator function"""
    retry.count = 0

    def retry_wrapper(*args, **kwargs):
        attempt = 0
        while attempt < retries:
            try:
                return func(*args, **kwargs)
            except requests.exceptions.ConnectionError as e:
                attempt += 1
                total_time = attempt * 10
                print(f'Retrying {attempt}: Sleeping for {total_time} seconds, error: ', e)
                time.sleep(total_time)
            if attempt == retries:
                retry.count += 1
                url_log_file = 'url_log.txt'
                if not os.path.exists(os.getcwd() + '\\' + url_log_file):
                    with open(url_log_file, 'w') as f:
                        f.write('url, status_code\n')
                with open(url_log_file, 'a') as file:
                    file.write(f'{args[0]}, requests.exceptions.ConnectionError\n')
            if retry.count == 3:
                print("Stopped after retries, check network connection")
                raise SystemExit

    return retry_wrapper


@retry
def get_soup(url, headers):
    refer = r'https://developer.mozilla.org/en-US/docs/Web/HTTP/Status#server_error_responses'
    r = requests.get(url, headers=headers)
    if r.status_code == 200:
        soup = BeautifulSoup(r.text, 'html.parser')
        return soup
    elif 499 >= r.status_code >= 400:
        print(f'client error response, status code {r.status_code} \nrefer: {refer}')
        status_log(r)
        return None
    elif 599 >= r.status_code >= 500:
        print(f'server error response, status code {r.status_code} \nrefer: {refer}')
        count = 1
        while count != 6:
            print('while', count)
            r = requests.get(url, headers=headers)  # your request get or post
            print('status_code: ', r.status_code)
            if r.status_code == 200:
                soup = BeautifulSoup(r.text, 'html.parser')
                return soup
            else:
                print('retry ', count)
                count += 1
                time.sleep(count * 2)
    else:
        status_log(r)
        return None


if __name__ == '__main__':
    data_list = []
    SOURCE_URL = 'https://github.com/search?o=desc&q=data+analysis&s=stars&type=Repositories'
    BASE_URL = 'https://github.com'
    headers = {
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:109.0) Gecko/20100101 Firefox/113.0'
    }
    page_soup = get_soup(SOURCE_URL, headers)
    page = 1
    if page_soup:
        next_page_url = SOURCE_URL
        while next_page_url is not None:
            page_soup = get_soup(next_page_url, headers)
            if page_soup:
                repo_list_items = page_soup.find_all('li', class_='repo-list-item')
                for repo_list_item in repo_list_items:
                    try:
                        repo_full_username = repo_list_item.find('div', class_='f4 text-normal').text.strip()
                        repo_username, repo_name = repo_full_username.split('/', 1)
                    except:
                        repo_username, repo_name = '', ''
                    try:
                        stars_count = repo_list_item.find('a', class_='Link--muted').text.strip()
                    except:
                        stars_count = ''
                    try:
                        repo_url = BASE_URL + repo_list_item.find('div', class_='f4 text-normal').find('a')['href']
                    except:
                        repo_url = ''
                    try:
                        topics = ''
                        topics_tag_list = repo_list_item.find_all('a', class_='topic-tag topic-tag-link f6 px-2 mx-0')
                        topics_tags = [stripit(topics_tag.text).strip() for topics_tag in topics_tag_list]
                        topics = '; '.join(topics_tags)
                    except:
                        topics = ''
                    repos_dict = {
                        'username': repo_username,
                        'repo_name': repo_name,
                        'stars': stars_count,
                        'repo_url': repo_url,
                        'topics': topics
                    }
                    data_list.append(repos_dict)
                    df = pd.DataFrame(data_list)
                    df.drop_duplicates(inplace=True, keep='first')
                    df.to_csv('github_top50_data_analysis_repos.csv', index=False)
            if page != 5:
                page += 1
                try:
                    next_page_url = BASE_URL + page_soup.find('a', class_='next_page')['href']
                except:
                    next_page_url = None
            else:
                next_page_url = None

