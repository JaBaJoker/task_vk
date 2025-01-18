Основная цель задания - создать скрипт для автоматизированной обработки и агрегации данных о действиях пользователей, с возможностью получения как ежедневной, так и еженедельной статистики.
Ниже приведены необходимые шаги для выполнения задания.
1) Ежедневная обработка данных:
Читать ежедневные CSV-файлы с информацией о действиях пользователей (create, read, update, delete).
Агрегировать данные по email пользователя, подсчитывая количество каждого типа действия.
Сохранять результаты в промежуточные CSV-файлы.
2) Еженедельная агрегация данных:
Обрабатывать данные за неделю, используя промежуточные файлы.
Суммировать количество действий каждого типа для каждого пользователя за всю неделю.
Сохранять итоговые результаты в выходной CSV-файл
3) Автоматизация процесса
Принимать дату окончания недели в качестве аргумента командной строки.
Автоматически вычислять диапазон дат для обработки (7 дней, заканчивающихся указанной датой).
Последовательно выполнять ежедневную и еженедельную агрегацию для указанного периода
4) Обработка ошибок:
Предусмотреть обработку ситуаций, когда входные файлы отсутствуют.
Выводить сообщения об ошибках при отсутствии файлов
