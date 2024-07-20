from flask import Flask, render_template, request, redirect, url_for, flash, session
import os
import pandas as pd
import psycopg2
from datetime import date, datetime
import openpyxl
from sqlalchemy import create_engine
import matplotlib.pyplot as plt
import winshell
import config


app = Flask(__name__)
app.config['UPLOAD_FOLDER'] = 'uploads'
app.secret_key = 'your_secret_key'


if not os.path.exists('uploads'):
    os.makedirs('uploads')

@app.route('/', methods=['GET', 'POST'])
def upload_file():
    if request.method == 'POST':
        if 'file' not in request.files:
            flash('No file part')
            return redirect(request.url)
        file = request.files['file']
        if file.filename == '':
            flash('Файл не выбран')
            return redirect(request.url)
        if file:
            file_path = os.path.join(app.config['UPLOAD_FOLDER'], file.filename)
            file.save(file_path)
            
            
        
        def insert_to_family(file_path):
            df = pd.read_excel(file_path, sheet_name='old_apart')

            # Преобразуем типы данных
            df['№ кв-ры'] = df['№ кв-ры'].astype(int)
            df['кол-во комнат'] = df['кол-во комнат'].astype(int)
            df['площ. жил. пом.'] = df['площ. жил. пом.'].astype(float)
            df['общ. пл.'] = df['общ. пл.'].astype(float)
            df['жил. пл.'] = df['жил. пл.'].astype(float)
            df['Кол-во членов семьи'] = df['Кол-во членов семьи'].astype(int)
            df['Потребность'] = df['Потребность'].astype(int)

            conn = psycopg2.connect(host=config.db_server, port=config.db_port, database=config.db_database,
                                        user=config.db_log, password=config.db_pass)

            cursor = conn.cursor()
            result_of_insert = True

            try:
                for index, row in df.iterrows():
                    cursor.execute("""
                        INSERT INTO recomendation.family (row_num, district, area, apart_number, type_of_settlement, room_count,
                                            full_living_space, total_living_area, living_area, category, notes,
                                            fio, members_amount, need, insert_date, score)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (row_num) DO UPDATE SET
                            district = EXCLUDED.district, area = EXCLUDED.area, apart_number = EXCLUDED.apart_number,
                            type_of_settlement = EXCLUDED.type_of_settlement, room_count = EXCLUDED.room_count,
                            full_living_space = EXCLUDED.full_living_space, total_living_area = EXCLUDED.total_living_area,
                            living_area = EXCLUDED.living_area, category = EXCLUDED.category, notes = EXCLUDED.notes,
                            fio = EXCLUDED.fio, members_amount = EXCLUDED.members_amount, need = EXCLUDED.need, 
                            insert_date = EXCLUDED.insert_date, score = EXCLUDED.score
                    """, (int(index), row['Округ'], row['район'], int(row['№ кв-ры']), row['Вид засел.'], int(row['кол-во комнат']),
                        float(row['площ. жил. пом.']), float(row['общ. пл.']), float(row['жил. пл.']), row['Категория'], '',
                        row['ФИО'], row['Кол-во членов семьи'], row['Потребность'], date.today(),
                        round(float(row['жил. пл.'] / row['Кол-во членов семьи']), 2)))

                conn.commit()
            except psycopg2.Error as err:
                print(f"Error: {err}")
                conn.rollback()
                result_of_insert = False
            finally:
                cursor.close()
                conn.close()

            return result_of_insert


        def insert_to_new_apart(file_path):
            df_new_apart = pd.read_excel(file_path, sheet_name='new_apart')

            # Преобразуем типы данных
            df_new_apart['№ кв-ры'] = df_new_apart['№ кв-ры'].astype(int)
            df_new_apart['этаж'] = df_new_apart['этаж'].astype(int)
            df_new_apart['кол-во комнат'] = df_new_apart['кол-во комнат'].astype(int)
            df_new_apart['площ. жил. пом.'] = df_new_apart['площ. жил. пом.'].astype(float)
            df_new_apart['общ. пл.'] = df_new_apart['общ. пл.'].astype(float)
            df_new_apart['жил. пл.'] = df_new_apart['жил. пл.'].astype(float)

            conn = psycopg2.connect(host=config.db_server, port=config.db_port, database=config.db_database,
                                    user=config.db_log, password=config.db_pass)

            cursor = conn.cursor()
            result_of_insert = True

            try:
                for index, row in df_new_apart.iterrows():
                    cursor.execute("""
                        INSERT INTO recomendation.new_apart (district, area, house_adress, apart_number, floor, room_count, full_living_space, total_living_area, living_area)
                        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                        ON CONFLICT (house_adress, apart_number) DO UPDATE SET
                            district = EXCLUDED.district, area = EXCLUDED.area, floor = EXCLUDED.floor,
                            room_count = EXCLUDED.room_count, full_living_space = EXCLUDED.full_living_space,
                            total_living_area = EXCLUDED.total_living_area, living_area = EXCLUDED.living_area
                    """, (row['Округ'], row['район'], row['адрес дома'], int(row['№ кв-ры']), int(row['этаж']),
                        int(row['кол-во комнат']),
                        float(row['площ. жил. пом.']), float(row['общ. пл.']), float(row['жил. пл.'])))

                conn.commit()
            except psycopg2.Error as err:
                print(f"Error: {err}")
                conn.rollback()
                result_of_insert = False
            finally:
                cursor.close()
                conn.close()

            return result_of_insert


        def match_new_apart_to_family():
            try:
                conn = psycopg2.connect(host=config.db_server, port=config.db_port, database=config.db_database,
                                        user=config.db_log, password=config.db_pass)
                cursor = conn.cursor()

                # Извлечение данных из таблицы family
                cursor.execute("""
                    SELECT family_id, district, area, room_count, total_living_area, living_area, need
                    FROM recomendation.family WHERE family_id NOT IN (SELECT family_id FROM recomendation.offer)
                    ORDER BY room_count ASC, total_living_area ASC, living_area ASC, score DESC
                """)
                old_aparts = cursor.fetchall()

                # Извлечение данных из таблицы new_apart
                cursor.execute("""
                    SELECT new_apart_id, district, area, room_count, total_living_area, living_area, floor
                    FROM recomendation.new_apart
                    WHERE status = 'Свободна'
                    ORDER BY room_count ASC, total_living_area ASC, living_area ASC
                """)
                new_aparts = cursor.fetchall()

                # Создание DataFrame для старых и новых квартир
                df_old_apart = pd.DataFrame(old_aparts,
                                            columns=['family_id', 'district', 'area', 'room_count', 'total_living_area',
                                                    'living_area', 'need'])
                df_new_apart = pd.DataFrame(new_aparts,
                                            columns=['new_apart_id', 'district', 'area', 'room_count', 'total_living_area',
                                                    'living_area', 'floor'])

                # Создаем множество для отслеживания уже подобранных квартир
                already_matched_new_aparts = set()

                for _, old_apart in df_old_apart.iterrows():
                    family_id = int(old_apart['family_id'])  # Определяем family_id здесь

                    # Фильтрация новых квартир на основе критериев старых квартир, медиан и потребности в этаже
                    suitable_aparts = df_new_apart[
                        (df_new_apart['district'] == old_apart['district']) &
                        (df_new_apart['area'] == old_apart['area']) &
                        (df_new_apart['room_count'] == old_apart['room_count']) &
                        (df_new_apart['total_living_area'] >= old_apart['total_living_area']) &
                        (df_new_apart['living_area'] >= old_apart['living_area'])
                        ]

                    # Учитываем потребность в определенном этаже, если need = 1
                    if old_apart['need'] == 1:
                        # Filter suitable_aparts based on floor range
                        floor_range = [1, 2, 3, 4, 5]
                        suitable_aparts = suitable_aparts[suitable_aparts['floor'].isin(floor_range)]

                    # Исключаем уже подобранные квартиры
                    suitable_aparts = suitable_aparts[~suitable_aparts['new_apart_id'].isin(already_matched_new_aparts)]

                    if not suitable_aparts.empty:
                        # Выбираем первую подходящую квартиру
                        suitable_apart = suitable_aparts.iloc[0]
                        new_apart_id = int(suitable_apart['new_apart_id'])

                        # Добавляем подобранный new_apart_id в множество already_matched_new_aparts
                        already_matched_new_aparts.add(new_apart_id)

                        # Выводим результат подбора
                        print(f"Family ID {family_id} подобрана квартира с new_apart_id {new_apart_id}")
                        cursor.execute(f"INSERT INTO recomendation.offer (family_id, new_apart_id) VALUES ({family_id}, {new_apart_id})")
                        conn.commit()

                    else:
                        # Проверка наличия записи в cannot_offer перед добавлением
                        cursor.execute("SELECT 1 FROM recomendation.cannot_offer WHERE family_id = %s", (family_id,))
                        exists = cursor.fetchone()
                        if not exists:
                            # Выводим сообщение о том, что для семьи не найдено подходящих квартир
                            print(f"Для Family ID {family_id} не найдено подходящих квартир")
                            cursor.execute(f"INSERT INTO recomendation.cannot_offer (family_id) VALUES ({family_id})")
                            conn.commit()

            except psycopg2.Error as e:
                print(f"Database Error: {e}")

            finally:
                cursor.close()
                conn.close()


        def save_views_to_excel():
            try:
                # Подключение к базе данных
                conn = psycopg2.connect(host=config.db_server, port=config.db_port, database=config.db_database,
                                        user=config.db_log, password=config.db_pass)
                print("Successfully connected to the database")

                views = ['available_apart', 'svod_balance', 'result_of_recomendation', 'balance', 'ranked', 'cannot_to_excel']
                desktop = winshell.desktop()
                print(desktop)  # Вывод: C:\Users\пользователь\OneDrive\Рабочий стол
                excel_file = os.path.join(desktop, 'output.xlsx')

                # Создаем Excel-файл и записываем данные в него
                with pd.ExcelWriter(excel_file, engine='openpyxl') as writer:
                    for view in views:
                        query = f"SELECT * FROM recomendation.{view}"
                        print(f"Executing query: {query}")

                        try:
                            cursor = conn.cursor()
                            cursor.execute(query)
                            columns = [col[0] for col in cursor.description]
                            data = cursor.fetchall()
                            df = pd.DataFrame(data, columns=columns)

                            if not df.empty:
                                df.to_excel(writer, sheet_name=view, index=False)
                                print(f"Saved {view} to Excel")
                            else:
                                print(f"No data found for view: {view}")

                        except Exception as e:
                            print(f"Error executing query for view {view}: {e}")

                        finally:
                            if cursor:
                                cursor.close()

                print(f'Data saved to {excel_file}')

            except Exception as e:
                print(f"Error: {e}")

            finally:
                conn.close()


        def delete():
            conn = psycopg2.connect(host=config.db_server, port=config.db_port, database=config.db_database,
                                    user=config.db_log, password=config.db_pass)
            cursor = conn.cursor()
            cursor.execute('DELETE FROM recomendation.offer')
            conn.commit()
            cursor.execute('DELETE FROM recomendation.cannot_offer')
            conn.commit()
            cursor.execute('delete FROM recomendation.new_apart')
            conn.commit()
            cursor.execute('DELETE FROM recomendation.family')
            conn.commit()
            conn.close()

        start_time = datetime.now()
        first = insert_to_family(file_path)
        print(f'result = {first} time of execute = {datetime.now() - start_time}')
        second = insert_to_new_apart(file_path)
        print(f'result = {second} time of execute = {datetime.now() - start_time}')
        third = match_new_apart_to_family()
        print(f'result = {third} time of execute = {datetime.now() - start_time}')
        four = save_views_to_excel()
        print(f'Total time of execute = {datetime.now() - start_time}')
        flash('Файл сохранен на рабочий стол')
        return redirect(url_for('upload_file'))
    
    return render_template('index.html')


if __name__ == '__main__':
    app.run(debug=True)
