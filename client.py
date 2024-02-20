import socket
import struct
import datetime


def create_dynamic_structure(header, *data_values, time):

    # Смотрим, указано ли время
    if time is not None:
        data_length = len(data_values) + 5
    else:
        data_length = len(data_values) + 1

    if time is not None:

        # Формирование строки формата
        format_string = 'BB' + 'B' * len(data_values)

        # Добавление заголовка, длины суммы к данным
        data_to_pack = (header, data_length, *data_values)

        # Упаковка данных в поток байтов
        packed_data = struct.pack(format_string, *data_to_pack)

        # Упаковка времени в поток байтов
        packed_data += struct.pack('<I', time)

        # Подсчет контрольной суммы
        x = 0

        map(lambda c: hex(ord(c)), packed_data)
        for ch in map(lambda c: hex(c), packed_data):
            ch = int(ch, 16)
            x += ch

        checksum = x & 0xFF

        checksum = 0x100 - checksum

        # Упаковка контрольной суммы в поток байтов
        packed_data += struct.pack('B', checksum)
    else:
        # Формирование строки формата
        format_string = 'BB' + 'B' * len(data_values) + 'B'

        # Добавление заголовка, длины к данным
        data_to_pack = (header, data_length, *data_values)

        # Подсчет контрольной суммы
        x = 0

        map(lambda c: hex(ord(c)), data_to_pack)
        for ch in map(lambda c: hex(c), data_to_pack):
            ch = int(ch, 16)
            x += ch

        checksum = x & 0xFF

        checksum = 0x100 - checksum

        # Упаковка данных в поток байтов
        packed_data = struct.pack(format_string, *data_to_pack, checksum)

    map(lambda c: hex(ord(c)), packed_data)
    for ch in map(lambda c: hex(c), packed_data):
        print(ch, end=' ')

    print()

    map(lambda c: hex(ord(c)), packed_data)
    for ch in map(lambda c: hex(c), packed_data):
        ch = int(ch, 16)
        print(ch, end=' ')

    return packed_data


def send_udp_data(host, port, data):
    # Создание сокета
    udp_client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)

    # Фиксируем хост и порт
    udp_client_socket.bind(('192.168.1.71', 54321))

    udp_client_socket.sendto(data, (host, port))

    # Принимаем ответ от сервера
    response_bytes, server_addr = udp_client_socket.recvfrom(2048)

    map(lambda c: hex(ord(c)), response_bytes)
    for ch in map(lambda c: hex(c), response_bytes):
        print(ch, end=' ')

    print()

    map(lambda c: hex(ord(c)), response_bytes)
    for ch in map(lambda c: hex(c), response_bytes):
        ch = int(ch, 16)
        print(ch, end=' ')

    # Распаковка первого байта
    header = struct.unpack('B', response_bytes[0:1])[0]

    length = struct.unpack('B', response_bytes[1:2])[0]

    try:
        # Распаковка параметров
        param1, param2, param3, param4, param5 = struct.unpack('BBBBB', response_bytes[2:7])

        # Переводим числа в слова, для понимания ответа в консоли
        if param1 == 1:
            param1 = 'GET'
        elif param1 == 2:
            param1 = 'SET'

        if param2 == 0:
            param2 = 'CAPACITY_INFO'
        elif param2 == 1:
            param2 = 'LEVEL'
        elif param2 == 2:
            param2 = 'STRUCTURE'
        elif param2 == 3:
            param2 = 'SN'

        if param2 != 'STRUCTURE':
            if param3 == 0:
                param3 = 'SYSTEM_ARRAY1'
            elif param3 == 1:
                param3 = 'SYSTEM_ARRAY2'
            elif param3 == 2:
                param3 = 'SYSTEM_ARRAY3'
        if param2 != 'SN':
            if param4 == 0:
                param4 = 'STORAGE_POOL1'
            elif param4 == 1:
                param4 = 'STORAGE_POOL2'
            elif param4 == 2:
                param4 = 'NO_STORAGE_POOL'

        if param2 == 'CAPACITY_INFO':
            if param5 == 0:
                param5 = 'ALL'
            elif param5 == 1:
                param5 = 'Used_capacity'
            elif param5 == 2:
                param5 = 'Total_usage'
        if param2 == 'LEVEL':
            if param5 == 0:
                param5 = 'LEVEL0'
            elif param5 == 1:
                param5 = 'LEVEL1'
            elif param5 == 2:
                param5 = 'LEVEL2'
    except:
        try:
            # Распаковка параметров
            param1, param2, param3 = struct.unpack('BBB', response_bytes[2:5])

            # Переводим числа в слова, для понимания ответа в консоли
            if param1 == 1:
                param1 = 'GET'
            elif param1 == 2:
                param1 = 'SET'

            if param2 == 0:
                param2 = 'CAPACITY_INFO'
            elif param2 == 1:
                param2 = 'LEVEL'
            elif param2 == 2:
                param2 = 'STRUCTURE'
            elif param2 == 3:
                param2 = 'SN'

            if param2 != 'STRUCTURE':
                if param3 == 0:
                    param3 = 'SYSTEM_ARRAY1'
                elif param3 == 1:
                    param3 = 'SYSTEM_ARRAY2'
                elif param3 == 2:
                    param3 = 'SYSTEM_ARRAY3'
        except:
            # Распаковка параметров
            param1, param2 = struct.unpack('BB', response_bytes[2:4])

            # Переводим числа в слова, для понимания ответа в консоли
            if param1 == 1:
                param1 = 'GET'
            elif param1 == 2:
                param1 = 'SET'

            if param2 == 0:
                param2 = 'CAPACITY_INFO'
            elif param2 == 1:
                param2 = 'LEVEL'
            elif param2 == 2:
                param2 = 'STRUCTURE'
            elif param2 == 3:
                param2 = 'SN'

    # Распаковка данных
    offset = 7
    unpacked_numbers = []
    if param2 == 'CAPACITY_INFO':
        if len(response_bytes) > 8:
            # Распаковка времени
            param8 = response_bytes[offset:offset + 1]
            param9 = response_bytes[offset + 1: offset + 2]
            param10 = response_bytes[offset + 2: offset + 3]
            param11 = response_bytes[offset + 3: offset + 4]
            number = param8 + param9 + param10 + param11
            param6 = struct.unpack('<I', number)[0]
            unpacked_numbers.append(param6)

            offset = 11

            while offset + 8 <= len(response_bytes) - 1:  # -1 для исключения последнего байта (checksum)
                number = struct.unpack('<d', response_bytes[offset:offset + 8])[0]
                unpacked_numbers.append(number)
                offset += 8

    if param2 == 'STRUCTURE':
        offset = 4
        while offset + 1 < len(response_bytes) and response_bytes[offset] != 0:
            # Распаковываем данные посимвольно
            char = struct.unpack('B', response_bytes[offset:offset + 1])[0]
            unpacked_numbers.append(char)
            offset += 1

    if param2 == 'SN':
        if len(response_bytes) > 6:
            offset = 5
            char = struct.unpack('<Q', response_bytes[offset:offset + 6] + b'\x00\x00')[0]
            unpacked_numbers.append(char)
            offset += 6

    if param2 == 'LEVEL':
        offset = 7
        if offset + 1 < len(response_bytes):
            char = struct.unpack('B', response_bytes[offset:offset + 1])[0]
            unpacked_numbers.append(char)
            offset += 1

        if offset + 4 < len(response_bytes):
            param8 = response_bytes[offset:offset + 1]
            param9 = response_bytes[offset + 1: offset + 2]
            param10 = response_bytes[offset + 2: offset + 3]
            param11 = response_bytes[offset + 3: offset + 4]
            number = param8 + param9 + param10 + param11
            param6 = struct.unpack('<I', number)[0]
            unpacked_numbers.append(param6)

        offset = 12

        while offset + 8 <= len(response_bytes) - 1:  # -4 for time and -1 для исключения последнего байта (checksum)
            number = struct.unpack('<d', response_bytes[offset:offset + 8])[0]
            unpacked_numbers.append(number)
            offset += 8

    # Распаковка последнего байта (checksum)
    checksum = struct.unpack('B', response_bytes[-1:])[0]

    # Вывод полученных данных
    print()
    if param2 != 'STRUCTURE' and param2 != 'SN' and len(response_bytes) > 7:
        print(f'От сервера {server_addr} получено: {param1, param2, param3, param4, param5, unpacked_numbers}')
    elif param2 == 'SN':
        print(unpacked_numbers)
        print(f'От сервера {server_addr} получено: {param1, param2, param3, unpacked_numbers}')
    else:
        print(f'От сервера {server_addr} получено: {param1, param2, unpacked_numbers}')
    print()
    print(f'Данные по UDP: {response_bytes}')
    udp_client_socket.close()


if __name__ == "__main__":
    server_host = '192.168.1.71'
    server_port = 50011

    GET = 0x01
    SET = 0x02

    ERROR = 0x07

    CAPACITY_INFO = 0x00
    LEVEL = 0x01
    STRUCTURE = 0x02
    SN = 0x03

    SYSTEM_ARRAY1 = 0x00
    SYSTEM_ARRAY2 = 0x01
    SYSTEM_ARRAY3 = 0x02

    STORAGE_POOL1 = 0x00
    STORAGE_POOL2 = 0x01
    NO_STORAGE_POOL = 0xF0

    ALL = 0x00
    Used_capacity = 0x01
    Total_usage = 0x02
    Mapped_LUN = 0x03

    LEVEL0 = 0x00
    LEVEL1 = 0x01
    LEVEL2 = 0x02

    header = 0x40

    dynamic_data = create_dynamic_structure(header, GET, CAPACITY_INFO, SYSTEM_ARRAY1, NO_STORAGE_POOL, ALL, time=1688158800)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, CAPACITY_INFO, SYSTEM_ARRAY2, NO_STORAGE_POOL, ALL, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, CAPACITY_INFO, SYSTEM_ARRAY3, NO_STORAGE_POOL, Used_capacity, time=1690837200)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, SET, LEVEL, SYSTEM_ARRAY1, NO_STORAGE_POOL, LEVEL0, 40, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, LEVEL, SYSTEM_ARRAY1, NO_STORAGE_POOL, LEVEL0, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, SET, LEVEL, SYSTEM_ARRAY1, STORAGE_POOL1, LEVEL1, 65, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, LEVEL, SYSTEM_ARRAY1, STORAGE_POOL2, LEVEL1, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, SET, LEVEL, SYSTEM_ARRAY2, STORAGE_POOL1, LEVEL2, 95, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, LEVEL, SYSTEM_ARRAY3, STORAGE_POOL2, LEVEL2, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, STRUCTURE, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, SN, SYSTEM_ARRAY1, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, SN, SYSTEM_ARRAY2, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)

    dynamic_data = create_dynamic_structure(header, GET, SN, SYSTEM_ARRAY3, time=None)

    print('-' * 100)
    print(f"Отправлен пакет на запрос всей строки system array: {dynamic_data}")

    send_udp_data(server_host, server_port, dynamic_data)


