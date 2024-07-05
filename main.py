import os
import psycopg2
import time
import dotenv
import logging
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.chrome.options import Options
from selenium.common.exceptions import NoSuchElementException, ElementClickInterceptedException, StaleElementReferenceException
import telebot
from telebot import types
import asyncio
import concurrent.futures
import random

dotenv.load_dotenv()

logging.basicConfig(
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    level=logging.INFO
)

TOKEN = os.environ.get('TELEGRAM_TOKEN')

bot = telebot.TeleBot(TOKEN)

def connect_db():
    conn = psycopg2.connect(
        dbname="postgres",
        user="postgres",
        password="2778",
        host="db",
        port="5432"
    )
    return conn

def insert_vacancy(conn, company, title, meta_info, salary, skills, link):
    with conn.cursor() as cur:
        cur.execute("""
        INSERT INTO vacancies (company, vacancy, location, salary, skills, link)
        VALUES (%s, %s, %s, %s, %s, %s)
        RETURNING id;
        """, (company, title, meta_info, salary, skills, link))
        conn.commit()
        return cur.fetchone()[0]

def parse_habr(query):
    options = Options()
    options.add_argument('--headless')
    options.add_argument('--disable-gpu')
    options.add_argument('--no-sandbox')
    options.add_argument('--disable-dev-shm-usage')
    options.add_argument('--disable-software-rasterizer')
    options.add_argument('--disable-webgl')
    options.add_argument('--disable-web-security')
    options.add_argument('--disable-features=WebRtcHideLocalIpsWithMdns,WebContentsDelegate::CheckMediaAccessPermission')
    options.add_argument('--disable-extensions')
    options.add_argument('--disable-infobars')
    options.add_argument('--remote-debugging-port=9222')
    options.add_argument('--enable-features=NetworkService,NetworkServiceInProcess')
    options.add_experimental_option('excludeSwitches', ['enable-logging', 'enable-automation'])
    options.add_experimental_option('useAutomationExtension', False)
    options.add_experimental_option('prefs', {
        'profile.managed_default_content_settings.images': 2,
        'disk-cache-size': 4096
    })

    driver = webdriver.Chrome(options=options)

    conn = connect_db()

    try:
        driver.get('https://career.habr.com')

        search_input = driver.find_element(By.CSS_SELECTOR, '.l-page-title__input')
        search_input.send_keys(query)
        search_input.send_keys(Keys.RETURN)

        time.sleep(1)

        while True:
            vacancies = driver.find_elements(By.CLASS_NAME, 'vacancy-card__info')
            for vacancy in vacancies:
                try:
                    company_element = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__company-title')
                    company = company_element.text
                except NoSuchElementException:
                    company = 'Компания не указана'

                title_element = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__title')
                title = title_element.text
                link = title_element.find_element(By.TAG_NAME, 'a').get_attribute('href')

                try:
                    meta_element = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__meta')
                    meta_info = meta_element.text
                except NoSuchElementException:
                    meta_info = 'Местоположение не указано'

                try:
                    salary = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__salary').text
                except NoSuchElementException:
                    salary = 'ЗП не указана'

                try:
                    skills = vacancy.find_element(By.CLASS_NAME, 'vacancy-card__skills').text
                except NoSuchElementException:
                    skills = 'Скиллы не указаны'

                vacancy_id = insert_vacancy(conn, company, title, meta_info, salary, skills, link)

                print(f'Компания: {company}\nВакансия: {title}\nСсылка: {link}\nМестоположение и режим работы: {meta_info}\nЗарплата: {salary}\nСкиллы: {skills}')

            try:
                next_button = driver.find_element(By.CSS_SELECTOR, 'a.button-comp--appearance-pagination-button[rel="next"]')
                driver.execute_script("arguments[0].scrollIntoView(true);", next_button)
                time.sleep(1)
                
                for _ in range(3):
                    try:
                        driver.execute_script("arguments[0].click();", next_button)
                        break
                    except StaleElementReferenceException:
                        next_button = driver.find_element(By.CSS_SELECTOR, 'a.button-comp--appearance-pagination-button[rel="next"]')
                        time.sleep(1)
                else:
                    break
                
                time.sleep(1)
            except (NoSuchElementException, ElementClickInterceptedException):
                break

    finally:
        driver.quit()
        conn.close()

@bot.message_handler(commands=['start'])
def start(message):
    bot.reply_to(message, 'Используйте /search <запрос>, чтобы искать вакансии.\nДля остального функционала можно задействовать /help')

@bot.message_handler(commands=['help'])
def help(message):
    bot.reply_to(message, 'Краткая сводка по командам\n/start - запуск/перезапуск бота\n/search <запрос> - поиск вакансий по запросу\n/recent - вывод 5 случайных вакансий\n/count - вывод общего кол-ва вакансий в бд\n/grafic - вывод на выбор режима раб. дня\n/search_company - поиск вакансий по компании из бд\n/search_vacancy - поиск вакансий по названию вакансии из бд')

@bot.message_handler(commands=['search'])
def search(message):
    query = message.text[len('/search '):]
    logging.info(f"Получен запрос для поиска: {query}")
    if not query:
        bot.reply_to(message, 'Пожалуйста, введите запрос после команды /search.')
        return

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vacancies;")
        initial_count = cur.fetchone()[0]
    conn.close()

    bot.reply_to(message, f'Ищу вакансии для: {query}')
    asyncio.run(run_parse_habr(query))
    bot.reply_to(message, 'Поиск завершен. Проверьте свою базу данных.')

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE id > %s ORDER BY id LIMIT 5;", (initial_count,))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.reply_to(message, 'Новые вакансии не найдены.')
    else:
        bot.reply_to(message, 'Ниже представлены 5 новых вакансий:')
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

async def run_parse_habr(query: str):
    loop = asyncio.get_event_loop()
    executor = concurrent.futures.ThreadPoolExecutor()
    await loop.run_in_executor(executor, parse_habr, query)

@bot.message_handler(commands=['recent'])
def recent(message):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies ORDER BY RANDOM() LIMIT 5;")
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.reply_to(message, 'Вакансии не найдены.')
    else:
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

@bot.message_handler(commands=['count'])
def count(message):
    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT COUNT(*) FROM vacancies;")
        count = cur.fetchone()[0]
    conn.close()
    bot.reply_to(message, f'Общее количество вакансий в базе данных: {count}')

@bot.message_handler(commands=['grafic'])
def grafic(message):
    keyboard = types.InlineKeyboardMarkup(row_width=2)
    buttons = [
        types.InlineKeyboardButton(text="Неполный рабочий день", callback_data='part_time'),
        types.InlineKeyboardButton(text="Полный рабочий день", callback_data='full_time')
    ]
    keyboard.add(*buttons)
    bot.reply_to(message, "Выберите график работы:", reply_markup=keyboard)

@bot.callback_query_handler(func=lambda call: call.data in ['part_time', 'full_time'])
def button(call):
    query_data = call.data

    conn = connect_db()
    with conn.cursor() as cur:
        if query_data == 'part_time':
            cur.execute("SELECT COUNT(*) FROM vacancies WHERE location ILIKE '%Неполный рабочий день%';")
        elif query_data == 'full_time':
            cur.execute("SELECT COUNT(*) FROM vacancies WHERE location ILIKE '%Полный рабочий день%';")
        count = cur.fetchone()[0]
    conn.close()

    bot.answer_callback_query(call.id)
    bot.edit_message_text(text=f'Количество вакансий с графиком "{query_data}": {count}',
                          chat_id=call.message.chat.id,
                          message_id=call.message.message_id)

@bot.message_handler(commands=['search_company'])
def search_by_company(message):
    company_name = message.text[len('/search_company '):]
    logging.info(f"Получен запрос для поиска по компании: {company_name}")
    if not company_name:
        bot.reply_to(message, 'Пожалуйста, введите название компании после команды /search_company.')
        return

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE company ILIKE %s ORDER BY RANDOM() LIMIT 5;", (f"%{company_name}%",))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.reply_to(message, f'Вакансии компании "{company_name}" не найдены.')
    else:
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

@bot.message_handler(commands=['search_vacancy'])
def search_by_vacancy(message):
    vacancy_query = message.text[len('/search_vacancy '):]
    logging.info(f"Получен запрос для поиска по вакансии: {vacancy_query}")
    if not vacancy_query:
        bot.reply_to(message, 'Пожалуйста, введите название вакансии после команды /search_vacancy.')
        return

    conn = connect_db()
    with conn.cursor() as cur:
        cur.execute("SELECT company, vacancy, location, salary, skills, link FROM vacancies WHERE vacancy ILIKE %s ORDER BY RANDOM() LIMIT 5;", (f"%{vacancy_query}%",))
        rows = cur.fetchall()
    conn.close()

    if not rows:
        bot.reply_to(message, f'Вакансии по запросу "{vacancy_query}" не найдены.')
    else:
        for row in rows:
            bot.send_message(message.chat.id, f'Компания: {row[0]}\nВакансия: {row[1]}\nМестоположение: {row[2]}\nЗарплата: {row[3]}\nСкиллы: {row[4]}\nСсылка: {row[5]}\n')

if __name__ == '__main__':
    bot.polling(none_stop=True)
