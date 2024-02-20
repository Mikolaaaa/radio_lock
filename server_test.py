import datetime
import socket
import struct
import psycopg2
import asyncio
import asyncpg


# Функция для выполнения асинхронного SQL-запроса к PostgreSQL
async def execute_query(query, values=None):
    conn = await asyncpg.connect(host='192.168.64.6', port=5432, database='test', user='pgpro', password='pgpro')

    try:
        if values:
            result = await conn.fetch(query, *values)

        else:
            result = await conn.fetch(query)
        return result
    finally:
        await conn.close()


# Функция для обработки UDP-данных
def handle_udp_data(data, client_address):
    # Обрабатываем байты параметров
    param1 = data[2]
    param2 = data[3]
    if len(data) <= 5:
        pass
    elif len(data) < 7:
        param3 = data[4]
    else:
        param3 = data[4]
        param4 = data[5]
        param5 = data[6]
    # Создаем параметры для запросов
    date = ''
    param1_new = ''
    param2_new = ''
    param3_new = ''
    param4_new = ''
    param5_new = ''
    param6_new = ''
    result = ''

    # Для пункта 2(SET) фиксируем значение нового уровня
    if len(data) == 9:
        param_lvl = data[7]

    # Определяем дату, либо то, что она введена неправильно
    if len(data) > 9:
        param8 = data[7]
        param9 = data[8]
        param10 = data[9]
        param11 = data[10]
        number = struct.unpack('I', data[7:11])[0]
        if number < 1669075200:  # 2022-11-22 00:00:00 - первая запись в таблице
            param6 = '1970-01-01 00:00:00'
        else:
            param6 = datetime.datetime.fromtimestamp(number)
            param6 = str(param6)
        param6_new = f"and time = '{param6}'"

    # Обрабатываем параметры в запросах к БД
    if param1 == 1 and param2 != 1:
        param1_new = 'FROM shd_from_csv WHERE'
        if param2 == 0:
            if param4 == 240:
                if param3 == 0:
                    param3_new = "sn = '20231121130700' and object = 'System'"
                elif param3 == 1:
                    param3_new = "sn = '20231121131110' and object = 'System'"
                elif param3 == 2:
                    param3_new = "sn = '20231121131239' and object = 'System'"
            elif param4 == 0 or param4 == 1:
                if param3 == 0:
                    if param4 == 0:
                        param3_new = "sn = '20231121130700' and object = 'StoragePool001'"
                    elif param4 == 1:
                        param3_new = "sn = '20231121130700' and object = 'StoragePool002'"
                elif param3 == 1:
                    if param4 == 0:
                        param3_new = "sn = '20231121131110' and object = 'StoragePool001'"
                    elif param4 == 1:
                        param3_new = "sn = '20231121131110' and object = 'StoragePool002'"
                elif param3 == 2:
                    if param4 == 0:
                        param3_new = "sn = '20231121131239' and object = 'StoragePool001'"
                    elif param4 == 1:
                        param3_new = "sn = '20231121131239' and object = 'StoragePool002'"
            if param2 == 0:
                if param5 == 0:
                    param5_new = 'SELECT time, "Total capacity(MB)", "Used capacity(MB)", "Mapped LUN capacity(MB)"'
                elif param5 == 1:
                    param5_new = 'SELECT time, "Used capacity(MB)"'
                elif param5 == 2:
                    param5_new = 'SELECT time, "Total capacity(MB)"'
                elif param5 == 3:
                    param5_new = 'SELECT time, "Mapped LUN capacity(MB)"'
            """else:
                param5_new = 'SELECT "Capacity usage(%)"'"""
        elif param2 == 2:
            param2_new = "SELECT COUNT(DISTINCT sn) AS unique_values_count FROM shd_from_csv where object = 'System' GROUP BY object "
            param3_new = "SELECT SUM(cnt) AS unique_values_count FROM (SELECT COUNT(DISTINCT sn) AS cnt FROM shd_from_csv WHERE sn = '20231121130700' AND object != 'System' GROUP BY object) subquery_alias"
            param4_new = "SELECT SUM(cnt) AS unique_values_count FROM (SELECT COUNT(DISTINCT sn) AS cnt FROM shd_from_csv WHERE sn = '20231121131110' AND object != 'System' GROUP BY object) subquery_alias"
            param5_new = "SELECT SUM(cnt) AS unique_values_count FROM (SELECT COUNT(DISTINCT sn) AS cnt FROM shd_from_csv WHERE sn = '20231121131239' AND object != 'System' GROUP BY object) subquery_alias"
    if param2 == 1:
        if param1 == 1 or param1 == 2:
            param2_new = "from level"
            if param5 == 0:
                param1_new = 'select level_0'
            elif param5 == 1:
                param1_new = 'select level_1'
            elif param5 == 2:
                param1_new = 'select level_2'
            if param4 == 240:
                if param3 == 0:
                    param3_new = "where object = 'Array1'"
                elif param3 == 1:
                    param3_new = "where object = 'Array2'"
                elif param3 == 2:
                    param3_new = "where object = 'Array3'"
            elif param4 == 0 or param4 == 1:
                if param3 == 0:
                    if param4 == 0:
                        param3_new = "where object = 'Array1 SP1'"
                    elif param4 == 1:
                        param3_new = "where object = 'Array1 SP2'"
                elif param3 == 1:
                    if param4 == 0:
                        param3_new = "where object = 'Array2 SP1'"
                    elif param4 == 1:
                        param3_new = "where object = 'Array2 SP2'"
                elif param3 == 2:
                    if param4 == 0:
                        param3_new = "where object = 'Array3 SP1'"
                    elif param4 == 1:
                        param3_new = "where object = 'Array3 SP2'"
        if param1 == 2 or (param1 == 1 and param2 == 1):
            param5_new = 'SELECT time, "Total capacity(MB)", "Used capacity(MB)", "Mapped LUN capacity(MB)"'
            param4_new = 'FROM shd_from_csv WHERE'
            if param4 == 240:
                if param3 == 0:
                    param6_new = "sn = '20231121130700' and object = 'System'"
                elif param3 == 1:
                    param6_new = "sn = '20231121131110' and object = 'System'"
                elif param3 == 2:
                    param6_new = "sn = '20231121131239' and object = 'System'"
            elif param4 == 0 or param4 == 1:
                if param3 == 0:
                    if param4 == 0:
                        param6_new = "sn = '20231121130700' and object = 'StoragePool001'"
                    elif param4 == 1:
                        param6_new = "sn = '20231121130700' and object = 'StoragePool002'"
                elif param3 == 1:
                    if param4 == 0:
                        param6_new = "sn = '20231121131110' and object = 'StoragePool001'"
                    elif param4 == 1:
                        param6_new = "sn = '20231121131110' and object = 'StoragePool002'"
                else:
                    if param4 == 0:
                        param6_new = "sn = '20231121131239' and object = 'StoragePool001'"
                    elif param4 == 1:
                        param6_new = "sn = '20231121131239' and object = 'StoragePool002'"
            if param1 == 2:
                if param5 == 0 and param2 < 4 and param3 < 3 and (param4 < 2 or param4 == 240):
                    lvl_0 = asyncio.run(execute_query(f"select level_1 from level {param3_new}"))
                    for item in lvl_0:
                        for num in item:
                            if num > param_lvl >= 0:
                                param2_new = f'UPDATE level SET level_0 = {param_lvl}'
                                param1_new = 'select level_0 from level'
                            else:
                                param2_new = param_lvl
                elif param5 == 1 and param2 < 4 and param3 < 3 and (param4 < 2 or param4 == 240):
                    lvl_0 = asyncio.run(execute_query(f"select level_2 from level {param3_new}"))
                    lvl_1 = asyncio.run(execute_query(f"select level_0 from level {param3_new}"))
                    for item in lvl_1:
                        for num1 in item:
                            lvl_1 = num1
                    for item in lvl_0:
                        for num in item:
                            if lvl_1 < param_lvl < num:
                                param2_new = f'UPDATE level SET level_1 = {param_lvl}'
                                param1_new = 'select level_1 from level'
                            else:
                                param2_new = param_lvl
                elif param5 == 2 and param2 < 4 and param3 < 3 and (param4 < 2 or param4 == 240):
                    lvl_0 = asyncio.run(execute_query(f"select level_1 from level {param3_new}"))
                    for item in lvl_0:
                        for num in item:
                            if num < param_lvl < 101:
                                param2_new = f'UPDATE level SET level_2 = {param_lvl}'
                                param1_new = 'select level_2 from level'
                            else:
                                param2_new = param_lvl
    if param1 == 1 and param2 == 0 and param3 < 3 and param5 < 4:
        if param4 == 240:
            if param6_new != '':
                result = asyncio.run(execute_query(
                    f"{param5_new} {param1_new} {param3_new} {param6_new} order by time desc limit 1"))
            else:
                result = asyncio.run(
                    execute_query(f"{param5_new} {param1_new} {param3_new} order by time desc limit 1"))
        elif param4 == 0 or param4 == 1:
            if param6_new != '':
                result = asyncio.run(execute_query(
                    f"{param5_new} {param1_new} {param3_new} {param6_new} order by time desc limit 1"))
            else:
                result = asyncio.run(
                    execute_query(f"{param5_new} {param1_new} {param3_new} order by time desc limit 1"))
    elif param2 == 2 and len(data) <= 5 and (param1 <= 3):
        result = asyncio.run(execute_query(f"{param2_new}"))
        result += asyncio.run(execute_query(f"{param3_new}"))
        result += asyncio.run(execute_query(f"{param4_new}"))
        result += asyncio.run(execute_query(f"{param5_new}"))
    elif param2 == 3 and (param1 <= 3):
        if param3 == 0:
            result = asyncio.run(execute_query(
                f"select sn from shd_from_csv where array_num = 'Array1' order by array_num desc limit 1"))
        elif param3 == 1:
            result = asyncio.run(execute_query(
                f"select sn from shd_from_csv where array_num = 'Array2' order by array_num desc limit 1"))
        elif param3 == 2:
            result = asyncio.run(execute_query(
                f"select sn from shd_from_csv where array_num = 'Array3' order by array_num desc limit 1"))
    elif param2 == 1 and param1 == 2:
        if param2_new != param_lvl:
            if param3_new != '' and param2_new != '' and param1_new != '' and param4_new != '' and param5_new != '' and param6_new != '':
                asyncio.run(execute_query(f"{param2_new} {param3_new}"))
                result = asyncio.run(execute_query(f"{param1_new} {param3_new}"))
                result += asyncio.run(execute_query(f"{param5_new} {param4_new} {param6_new} order by time desc limit 1"))
        else:
            result = param_lvl
    elif param1 == 1 and param2 == 1:
        if param3_new != '' and param2_new != '' and param1_new != '' and param4_new != '' and param5_new != '' and param6_new != '':
            result = asyncio.run(execute_query(f"{param1_new} {param2_new} {param3_new}"))
            result += asyncio.run(execute_query(f"{param5_new} {param4_new} {param6_new} order by time desc limit 1"))
    print(f"Данные взяты из базы данных: {result}")

    # Делаем запросы к БД, учитывая параметры
    if (len(data) <= 5) and (param1 < 3) and (param2 < 4):
        get_data = send_udp_data_struct_1(result, param1, param2)
    elif (len(data) < 8) and (param1 < 3) and (param2 < 4) and (param3 < 3):
        get_data = send_udp_data_struct_2(result, param1, param2, param3)
    elif (param1 < 3) and (param2 < 4) and (param3 < 3) and (param4 < 2 or param4 == 240) and (param5 < 4):
        if (len(data) > 9):
            if param6 == '1970-01-01 00:00:00' or result == []:
                get_data = send_udp_data(number, param1, param2, param3, param4, param5)
            else:
                get_data = send_udp_data(result, param1, param2, param3, param4, param5)
        else:
            get_data = send_udp_data(result, param1, param2, param3, param4, param5)
    elif (len(data) <= 5) and ((param1 > 2) or (param2 > 3)):
        get_data = send_udp_data_struct_1('error', param1, param2)
    elif (len(data) < 8) and ((param1 > 2) or (param2 > 3) or (param3 > 2)):
        get_data = send_udp_data_struct_2('error', param1, param2, param3)
    elif (len(data) > 9) and (
            (param1 > 2) or (param2 > 3) or (param3 > 2) or (param4 > 1 and param4 != 240) or (param5 > 4)):
        get_data = send_udp_data(number, param1, param2, param3, param4, param5)
    elif (len(data) < 10) and ((param1 > 2) or (param2 > 3) or (param3 > 2) or (param4 > 1 and param4 != 240) or (
            param5 > 4)) or result == []:
        get_data = send_udp_data(param_lvl, param1, param2, param3, param4, param5)

    return get_data, client_address[0], client_address[1]


# Функция для отправки данных по UDP для получения структуры(пункт 3.1)
def send_udp_data_struct_1(data, param1, param2):
    # Если ошибка в команде
    if data == 'error':
        header = 0xC1
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2)
        )

    # STRUCTURE
    elif param2 == 2:
        header = 0x41
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2)
        )
        # Получение элемента(ов) из списка кортежей
        for item in data:
            for num in item:
                num = int(num)
                if isinstance(num, int):
                    try:
                        # Упаковка числа
                        packed_data += struct.pack('B', int(num))
                    except:
                        # Упаковка числа
                        packed_data += struct.pack('<Q', num)[:6]
                else:
                    print("Неизвестный тип данных")

    # Считаем длину
    lenght = len(packed_data)

    # Добавляем длину
    packed_data = (
            packed_data[:1] +
            struct.pack('B', lenght) +
            packed_data[1:]
    )

    # Считаем контрольную сумму
    x = 0

    map(lambda c: hex(ord(c)), packed_data)
    for ch in map(lambda c: hex(c), packed_data):
        ch = int(ch, 16)
        x += ch

    checksum = x & 0xFF

    checksum = 0x100 - checksum

    # Добавляем контрольную сумму
    packed_data += struct.pack('B', checksum)

    return packed_data


# Функция для отправки данных по UDP для получения SN(пункты 3.2-3.4)
def send_udp_data_struct_2(data, param1, param2, param3):
    # Если ошибка в команде
    if data == 'error':
        header = 0xC1
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2) +
                struct.pack('B', param3)
        )
    elif param2 == 3:
        header = 0x41
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2) +
                struct.pack('B', param3)
        )
        # Получение элемента(ов) из списка кортежей
        for item in data:
            for num in item:
                num = int(num)
                # Упаковка числа
                packed_data += struct.pack('<Q', num)[:6]

    # Считаем длину
    lenght = len(packed_data)

    # Добавляем длину
    packed_data = (
            packed_data[:1] +
            struct.pack('B', lenght) +
            packed_data[1:]
    )

    # Считаем контрольную сумму
    x = 0

    map(lambda c: hex(ord(c)), packed_data)
    for ch in map(lambda c: hex(c), packed_data):
        ch = int(ch, 16)
        x += ch

    checksum = x & 0xFF

    checksum = 0x100 - checksum

    # Добавляем контрольную сумму
    packed_data += struct.pack('B', checksum)

    return packed_data


# Функция для отправки данных по UDP(для пукнтов 1-2)
def send_udp_data(data, param1, param2, param3, param4, param5):
    # Если ошибка в команде
    if data == 'error':
        header = 0xC1
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2) +
                struct.pack('B', param3) +
                struct.pack('B', param4) +
                struct.pack('B', param5)
        )
    elif type(data) == int:
        header = 0xC1
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2) +
                struct.pack('B', param3) +
                struct.pack('B', param4) +
                struct.pack('B', param5)
        )
        if param2 == 0:
            packed_data += struct.pack('<I', data)
        elif param2 == 1:
            packed_data += struct.pack('B', data)

    elif param2 == 0:
        header = 0x41
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2) +
                struct.pack('B', param3) +
                struct.pack('B', param4) +
                struct.pack('B', param5)
        )
        # Получение элемента(ов) из списка кортежей
        for item in data:
            for i in range(len(item)):
                if isinstance(item[i], datetime.datetime):
                    time = int(item[i].timestamp())
                    # Упаковка времени
                    packed_data += struct.pack('<I', time)
                elif type(item[i]) == float:
                    if item[i] is None:
                        item[i] = 0
                    # Упаковка числа
                    packed_data += struct.pack('<d', item[i])
                elif item[i] is None:
                    # Упаковка если значение "Mapped LUN capacity(MB)" = None
                    packed_data += struct.pack('<d', 0)
    elif param2 == 1:
        header = 0x41
        if type(data) == int:
            header = 0xC1
        # Сбор header и параметров в один байтовый объект
        packed_data = (
                struct.pack('B', header) +
                struct.pack('B', param1) +
                struct.pack('B', param2) +
                struct.pack('B', param3) +
                struct.pack('B', param4) +
                struct.pack('B', param5)
        )
        # Получение элемента(ов) из списка кортежей
        if type(data) == list:
            for item in data:
                for num in item:
                    if isinstance(num, int):
                        # Упаковка числа
                        packed_data += struct.pack('B', int(num))
                    elif type(num) == float:
                        # Упаковка числа
                        packed_data += struct.pack('<d', num)
                    elif isinstance(num, datetime.datetime):
                        # Упаковка времени
                        time = int(num.timestamp())
                        packed_data += struct.pack('<I', time)
                    elif num is None:
                        # Упаковка если значение "Mapped LUN capacity(MB)" = None
                        packed_data += struct.pack('<d', 0)
        else:
            # Упаковка данных
            packed_data += struct.pack('B', data)

    # Считаем длину
    lenght = len(packed_data)

    # Добавляем длину
    packed_data = (
            packed_data[:1] +
            struct.pack('B', lenght) +
            packed_data[1:]
    )

    # Считаем контрольную сумму
    x = 0

    map(lambda c: hex(ord(c)), packed_data)
    for ch in map(lambda c: hex(c), packed_data):
        ch = int(ch, 16)
        x += ch

    checksum = x & 0xFF

    checksum = 0x100 - checksum

    # Добавляем контрольную сумму
    packed_data += struct.pack('B', checksum)

    return packed_data


# Функция для запуска UDP-сервера
def start_udp_server(host, port):
    # Создание сокета
    udp_server_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Фиксируем хост и порт сервера
    udp_server_socket.bind((host, port))

    print(f"UDP сервер запущен на {host}:{port}")

    while True:
        # Получение данных и информацию о клиенте
        data, client_address = udp_server_socket.recvfrom(1024)

        print('-' * 100)
        print(f"Получены данные от {client_address}: {data}")
        print()

        # Вызов функции обработки данных
        packed_data, host_client, port_client = handle_udp_data(data, client_address)

        # Отправка данных
        udp_server_socket.sendto(packed_data, (host_client, port_client))

        print()

        print(f"Данные отправлены по UDP: {packed_data}")


if __name__ == "__main__":
    # Хост и порт нашего сервера
    server_host = '192.168.1.71'
    server_port = 50011

    # Импорт модуля для работы с потоками
    import threading

    # Создаем объект потока
    server_thread = threading.Thread(target=start_udp_server, args=(server_host, server_port))

    # Запускаем поток
    server_thread.start()
