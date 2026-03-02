import requests
import json
from datetime import datetime, timedelta
import pytz
import os

# Конфигурационные константы
API_BASE_URL = "https://gamma-api.polymarket.com/events"
LOCAL_TZ = pytz.timezone("Europe/Warsaw")
OUTPUT_DATA_FILE = "public/data.json"
REQUEST_TIMEOUT = 10
MAX_HISTORY_DAYS = 365

def fetch_market_resolution(timestamp: int) -> str:
    """Извлекает финальный результат рынка по Unix-таймстампу."""
    slug = f"btc-updown-5m-{timestamp}"
    try:
        response = requests.get(f"{API_BASE_URL}?slug={slug}", timeout=REQUEST_TIMEOUT)
        if response.status_code!= 200:
            return None
        data = response.json()
        if not data or len(data) == 0:
            return None
            
        market_obj = data.get("markets", [{}])
        if not market_obj.get("closed", False):
            return None
            
        prices_str = market_obj.get("outcomePrices", "")
        prices = json.loads(prices_str)
        
        if not prices or len(prices) < 2:
            return None
            
        price_up, price_down = float(prices), float(prices)
        if price_up > price_down: return "Up"
        elif price_down > price_up: return "Down"
        else: return "Draw"
            
    except Exception as e:
        print(f"Ошибка для {slug}: {e}")
        return None

def load_existing_data():
    """Загружает существующую историю из файла, преобразуя ее в плоский список."""
    if not os.path.exists(OUTPUT_DATA_FILE):
        return
    try:
        with open(OUTPUT_DATA_FILE, "r", encoding="utf-8") as f:
            grouped_data = json.load(f)
            flat_list =
            for date_key, records in grouped_data.items():
                flat_list.extend(records)
            # Сортировка по времени по возрастанию
            return sorted(flat_list, key=lambda x: x["ts"])
    except Exception:
        return

def main():
    now_local = datetime.now(LOCAL_TZ)
    end_ts = int(now_local.timestamp())
    end_ts = end_ts - (end_ts % 300) # Округление до текущей свечи
    
    existing_records = load_existing_data()
    
    if existing_records:
        start_ts = existing_records[-1]["ts"] + 300
    else:
        # Если файла нет, собираем данные только за последние 24 часа для старта
        start_time = now_local - timedelta(days=1)
        start_ts = int(start_time.timestamp())
        start_ts = start_ts - (start_ts % 300)
    
    print(f"[INFO] Скачивание данных начиная с Unix {start_ts}")
    
    current_ts = start_ts
    new_records =
    
    while current_ts < end_ts:
        outcome = fetch_market_resolution(current_ts)
        if outcome and outcome!= "Draw":
            dt_obj = datetime.fromtimestamp(current_ts, LOCAL_TZ)
            new_records.append({
                "ts": current_ts,
                "time": dt_obj.strftime("%H:%M"),
                "date": dt_obj.strftime("%d-%m-%Y"),
                "outcome": outcome
            })
        current_ts += 300
        
    all_records = existing_records + new_records
    
    # Очистка старых данных (старше MAX_HISTORY_DAYS)
    cutoff_ts = int((now_local - timedelta(days=MAX_HISTORY_DAYS)).timestamp())
    all_records = [r for r in all_records if r["ts"] >= cutoff_ts]
    
    # Пересчет серий (Streaks >= 4)
    streak_counter = 1
    for i in range(len(all_records)):
        all_records[i]["is_streak"] = False
        all_records[i]["streak_len"] = 1
        
    for i in range(1, len(all_records)):
        if all_records[i]["outcome"] == all_records[i-1]["outcome"]:
            streak_counter += 1
            all_records[i]["streak_len"] = streak_counter
        else:
            streak_counter = 1
            all_records[i]["streak_len"] = 1
            
    # Ретроспективная маркировка элементов серии
    for i in range(len(all_records) - 1, -1, -1):
        current_len = all_records[i].get("streak_len", 1)
        if current_len >= 4:
            for j in range(i, i - current_len, -1):
                all_records[j]["is_streak"] = True

    # Группировка и сохранение
    grouped_data = {}
    for record in all_records:
        date_key = record["date"]
        if date_key not in grouped_data:
            grouped_data[date_key] =
        record.pop("streak_len", None)
        grouped_data[date_key].append(record)
        
    os.makedirs(os.path.dirname(OUTPUT_DATA_FILE), exist_ok=True)
    with open(OUTPUT_DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(grouped_data, f, ensure_ascii=False, indent=2)
    print(f"[INFO] Обновлено. Всего записей в базе: {len(all_records)}")

if __name__ == "__main__":
    main()
