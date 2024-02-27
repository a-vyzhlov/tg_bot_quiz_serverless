import os
import ydb

YDB_ENDPOINT = os.getenv("YDB_ENDPOINT")
YDB_DATABASE = os.getenv("YDB_DATABASE")

def get_ydb_pool(ydb_endpoint, ydb_database, timeout=30):
    ydb_driver_config = ydb.DriverConfig(
        ydb_endpoint,
        ydb_database,
        credentials=ydb.credentials_from_env_variables(),
        root_certificates=ydb.load_ydb_root_certificate(),
    )

    ydb_driver = ydb.Driver(ydb_driver_config)
    ydb_driver.wait(fail_fast=True, timeout=timeout)
    return ydb.SessionPool(ydb_driver)


def _format_kwargs(kwargs):
    return {"${}".format(key): value for key, value in kwargs.items()}


# Заготовки из документации
# https://ydb.tech/en/docs/reference/ydb-sdk/example/python/#param-prepared-queries
def execute_update_query(pool, query, **kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, _format_kwargs(kwargs), commit_tx=True
        )

    return pool.retry_operation_sync(callee)


# Заготовки из документации
# https://ydb.tech/en/docs/reference/ydb-sdk/example/python/#param-prepared-queries
def execute_select_query(pool, query, **kwargs):
    def callee(session):
        prepared_query = session.prepare(query)
        result_sets = session.transaction(ydb.SerializableReadWrite()).execute(
            prepared_query, _format_kwargs(kwargs), commit_tx=True
        )
        return result_sets[0].rows

    return pool.retry_operation_sync(callee)    

# Зададим настройки базы данных 
pool = get_ydb_pool(YDB_ENDPOINT, YDB_DATABASE)


# Структура квиза
quiz_data = [
    {
        'question': 'Фильм - Подкидыш',
        'options': ['Муля, не нервируй меня!', 'Муля не зли меня!', 'Муля не беси меня!'],
        'correct_option': 0
    },
    {
        'question': 'Фильм - Мимино',
        'options': ['Алло! Ларису Ивановну позови!', 'Алло! Ларису Ивановну хочу!', 'Алло! Ларису Ивановну надо!'],
        'correct_option': 1
    },
    {
        'question': 'Фильм - Карнавальная ночь',
        'options': ['Докладчик сделает доклад, коротенько так, минут на 20', 'Докладчик сделает доклад, коротенько так, минут на 40', 'Докладчик сделает доклад, коротенько так, минут на 100'],
        'correct_option': 1
    },
    {
        'question': 'Фильм - Свадьба в Малиновке',
        'options': ['И шо я в тебя такой влюбленный, а?', 'И чаго я в тебя такой влюбленный, а?', 'И зачем я в тебя такой влюбленный, а?'],
        'correct_option': 0
    },
    {
        'question': 'Фильм - Вокзал для двоих',
        'options': ['Ты хорошо бежишь, очень хорошо... только не поможет!', 'Ты хорошо бежишь, очень хорошо... только не в ту сторону!', 'Ты хорошо бежишь, очень хорошо... только медленно!'],
        'correct_option': 2
    },
    {
        'question': 'Фильм - Обыкновенное чудо',
        'options': ['Мне попала муха под мантию', 'Мне попала блоха под мантию', 'Мне попала вожжа под мантию'],
        'correct_option': 2
    },
    {
        'question': 'Фильм - Иван Васильевич меняет профессию',
        'options': ['Танцуем все!', 'Танцуют все!', 'Пляшем все!'],
        'correct_option': 1
    },
    {
        'question': 'Фильм - Джентльмены удачи',
        'options': ['Украл, выпил, в тюртьму! Мечта!', 'Украл, выпил, в тюртьму! Красота!', 'Украл, выпил, в тюртьму! Романтика!'],
        'correct_option': 2
    },
    {
        'question': 'Фильм - Бриллиантовая рука',
        'options': ['Семё-ё-н Семёныч!...', 'Ива-а-н Иваныч!...', 'Пе-е-тр Петрович'],
        'correct_option': 0
    },
    {
        'question': 'Фильм - Двенадцать стульев',
        'options': ['Командовать парадом будете вы!', 'Командовать парадом буду я!', 'Командовать парадом будешь ты!'],
        'correct_option': 1
    },
]
