# WhatsApp Private Core

Private Core размещается на защищенном VPS. Здесь хранятся Baileys auth-файлы, WhatsApp-сессии и выполняется отправка/прием сообщений. Публичный шлюз получает доступ к HTTP API только с секретным заголовком `X-Gateway-Secret`.

## Установка

```bash
cd personal
npm install
cp .env.example .env
nano .env
```

Настройте `.env`:

```env
PRIVATE_PORT=4000
PUBLIC_GATEWAY_URL=http://<IP_ПУБЛИЧНОГО_VPS>:3000
GATEWAY_SECRET=<та же 64-символьная строка, что в front/.env>
SESSIONS_DIR=./sessions
DB_FILE=./data/private-core.sqlite
```

## Запуск

```bash
npm start
```

Для разработки:

```bash
npm run dev
```

Сначала запускайте Private Core, затем Public Gateway. При старте ядро подключается к публичному шлюзу через Socket.IO исходящим соединением и восстанавливает существующие сессии из `SESSIONS_DIR`.

## Firewall

На приватном VPS разрешите порт `4000` только для IP публичного VPS:

```bash
sudo ufw allow from <PUBLIC_VPS_IP> to any port 4000 proto tcp
sudo ufw reload
```

Не открывайте `4000` для всего интернета.

## Сессии

Каждый аккаунт хранится в отдельной подпапке `sessions/<phoneNumber>`. Не публикуйте эту папку и не переносите ее на публичный шлюз.

## База данных

Private Core сохраняет аккаунты, чаты и сообщения в SQLite:

```env
DB_FILE=./data/private-core.sqlite
MAX_STORED_MESSAGES_PER_CHAT=500
```

Папка `data/` добавлена в `.gitignore`. При деплое после обновления зависимостей выполните:

```bash
npm install --omit=dev
sudo systemctl restart whatsapp-private-core
```

Резервная копия базы:

```bash
sqlite3 /opt/whatsapp-private-core/data/private-core.sqlite ".backup '/opt/whatsapp-private-core/data/private-core.backup.sqlite'"
```

Если `better-sqlite3` не установится из prebuild-пакета, поставьте системные инструменты сборки и повторите `npm install`:

```bash
sudo apt install -y build-essential python3 make g++
```

## Приватный UI

Private Core также отдает read-only интерфейс просмотра:

```text
http://<PRIVATE_VPS_IP>:4000/ui
```

Если в `.env` задан `PRIVATE_UI_TOKEN`, страница запросит этот токен и будет отправлять его в API и Socket.IO. С учетом firewall обычно удобнее открывать UI через SSH-туннель с вашей машины:

```bash
ssh -L 4000:127.0.0.1:4000 root@<PRIVATE_VPS_IP>
```

После этого откройте:

```text
http://127.0.0.1:4000/ui
```

UI показывает аккаунты, чаты и сообщения из SQLite. Новые входящие и исходящие сообщения сохраняются сразу. WhatsApp может не отдать всю старую историю после первой привязки, но все новые события после запуска ядра будут сохраняться в `DB_FILE`.

## Использование

1. Откройте страницу Public Gateway.
2. Введите номер в международном формате без `+`.
3. В WhatsApp на телефоне откройте `Настройки -> Связанные устройства -> Код сопряжения`.
4. Введите 8-значный код, показанный на странице.

Используйте систему только для собственных аккаунтов и обычной переписки. Массовые рассылки и спам могут нарушать правила WhatsApp.
