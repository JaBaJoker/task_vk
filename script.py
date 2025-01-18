import pandas as pd
from datetime import datetime, timedelta

def calculate_daily_aggregates(date_str):
    # Формирование пути к входному файлу
    file_path = f'input/{date_str}.csv'
    try:
        # Чтение CSV файла
        df = pd.read_csv(file_path)
        # Группировка данных по email и подсчет количества действий
        aggregated_data = df.groupby('email')['action'].value_counts().unstack().fillna(0).astype(int)
        # Переименование столбцов
        aggregated_data.columns = ['create_count', 'read_count', 'update_count', 'delete_count']
        # Добавление столбца с email
        aggregated_data['email'] = aggregated_data.index
        # Формирование пути к выходному файлу
        output_file = f'intermediate/{date_str}.csv'
        # Сохранение агрегированных данных в CSV
        aggregated_data.to_csv(output_file, index=False)
    except FileNotFoundError:
        print(f"Файл {file_path} не найден.")

def calculate_weekly_aggregates(end_date_str):
    # Преобразование строки даты в объект datetime
    end_date = datetime.strptime(end_date_str, '%Y-%m-%d')
    # Вычисление начальной даты недели
    start_date = end_date - timedelta(days=7)
    # Создание списка дат за неделю
    date_range = [start_date + timedelta(days=i) for i in range(7)]
    # Инициализация пустого DataFrame для агрегированных данных
    aggregated_data = pd.DataFrame()

    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        # Формирование пути к промежуточному файлу
        file_path = f'intermediate/{date_str}.csv'
        try:
            # Чтение ежедневного CSV файла
            daily_df = pd.read_csv(file_path)
            # Объединение данных
            aggregated_data = pd.concat([aggregated_data, daily_df], ignore_index=True)
        except FileNotFoundError:
            print(f"Файл {file_path} не найден.")

    # Группировка и суммирование данных за неделю
    aggregated_data = aggregated_data.groupby('email')[['create_count', 'read_count', 'update_count', 'delete_count']].sum().reset_index()
    # Формирование пути к выходному файлу
    output_file = f'output/{end_date_str}.csv'
    # Сохранение недельных агрегированных данных в CSV
    aggregated_data.to_csv(output_file, index=False)

if __name__ == "__main__":
    import sys
    if len(sys.argv) != 2:
        print("Использование: python script.py <YYYY-mm-dd>")
        sys.exit(1)
    date_str = sys.argv[1]
    # Преобразование строки даты в объект datetime
    end_date = datetime.strptime(date_str, '%Y-%m-%d')
    # Вычисление начальной даты недели
    start_date = end_date - timedelta(days=7)
    # Создание списка дат за неделю
    date_range = [start_date + timedelta(days=i) for i in range(7)]

    # Вычисление ежедневных агрегатов для каждого дня недели
    for date in date_range:
        date_str = date.strftime('%Y-%m-%d')
        calculate_daily_aggregates(date_str)

    # Вычисление недельных агрегатов
    calculate_weekly_aggregates(date_str)
