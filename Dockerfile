# Используем базовый образ с установленными Python и Selenium
FROM joyzoursky/python-chromedriver:3.9-selenium

# Устанавливаем рабочую директорию
WORKDIR /app

# Копируем необходимые файлы в контейнер
COPY . .

# Устанавливаем зависимости
RUN pip install --no-cache-dir -r requirements.txt

# Устанавливаем переменные окружения
ENV TELEGRAM_TOKEN=7423021400:AAGZr0VRxe5mpAVCCrnH2xF20enjWDdq0XM
ENV DB_NAME=postgres
ENV DB_USER=postgres
ENV DB_PASSWORD=2778
ENV DB_HOST=db
ENV DB_PORT=5432

# Команда для запуска вашего приложения
CMD ["python", "main.py"]
