## Семинар 1. Поднимаем OLTP для сервиса в Docker.

### Демо 1 - Самый простой голый PostgreSQL.

Для такого сценария достаточно максимально просто конфига:
```yaml
version: "3"
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: "postgres"
      POSTGRES_USER: "postgres"
      POSTGRES_PASSWORD: "postgres"
      PGDATA: "/var/lib/postgresql/data/pgdata"
    volumes:
      - ./data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
```

Здесь:
- version - версия docker-compose
- services - название сети (группы контейнеров) в docker-compose
- postgres - название контейнера

Далее идут настройки приложения:
- image - образ приложения (локальный или из docker hub), тут - официальный контейнер PostgreSQL 13.
- environment - значения переменных среды внутри контейнера
- volumes - mount директорий с хост машины на контейнер<br>
В данном случае директория `./data` на нашей ОС, маунтится на `/var/lib/postgresql/data` внутри контейнера. Это значит, что файлы и их изменения будут синхронизовываться, а при старте пустого контейнера в него будут добавлены соотвествущие  файлы с хоста.
**Тут важно заметить** - если при старте контейнера в папке pgdata будет лежать сконфигурированная база, то новая проинициализирована не будет, а база поднимется из состояния, которое лежит в pgdata.
- ports - соответствие портов на хосте и на контейнере. В данном случае обращение по порту 5432 к хосту будет транслировано на порт 5432 контейнера.

Прямое указание конфига в базе можно заменить на указание конфига в .env файле.
Тогда это будет выглядеть следующим образом:

`docker-compose.yml`:<br>
```yaml
version: "3"
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: ${POSTGRES_DB}
      POSTGRES_USER: ${POSTGRES_USER}
      POSTGRES_PASSWORD: ${POSTGRES_PASSWORD}
      PGDATA: ${PGDATA}
    volumes:
      - ./data:/var/lib/postgresql/data
    ports:
      - "5432:5432"
```

`.env`:<br>
```bash
POSTGRES_DB=postgres
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
PGDATA=/var/lib/postgresql/data/pgdata
```

Так делать правильнее, потому что при запуске в промышленной среде у средств контроля контейнеров (например, k8s) будет возможность пробрасывать секреты через конфиги, а не через явное указание их в коде.

### Демо 2 - Добавляем первичную инициализацию базы из SQL / данных.

```yaml
version: "3"
services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_DB: "postgres"
      POSTGRES_USER: "postgres"
      POSTGRES_PASSWORD: "postgres"
      PGDATA: "/var/lib/postgresql/data/pgdata"
    volumes:
      - .:/var/lib/postgresql/data
      - ./createdb.sql:/docker-entrypoint-initdb.d/createdb.sql
    ports:
      - "5432:5432"
```

В этом конфиге мы добавили 2 вещи:
1. Положили в директорию, которая маунтится на PGDATA, файл data.csv; теперь он появится в контейнере, и мы сможем использовать его внутри него;
2. Положили в `/docker-entrypoint-initdb.d/createdb.sql` файл с инициализацией нашей БД; На самом деле, таких файлов может быть сколько угодно, а PostgreSQL просто при инициализации БД выполнит их все в алфавитном порядке;

В примере показано, как создать таблицы в новой БД и как наполнить их файлами;
В целом, в БД можно выполнить любые операции в SQL-синтаксисе:
- создание ролей
- создание БД/таблиц/view/триггеров
- insert данных

### Чтобы выполнить это у себя

1. Заходим в директорию `demo*`
2. `docker-compose build`
3. `docker-compose up`