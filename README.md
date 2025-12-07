# rasbottg – VT WhatsApp Panel & TG Telegram Panel

Монорепозиторий с двумя связанными панелями управления:

- `vt_project` – оригинальная **VT Panel** для управления сессиями WhatsApp (через Baileys) и сбора ссылок на WhatsApp/Telegram‑группы.
- `tg_project` – новая **TG Panel** для управления Telegram userbot‑сессиями (через gramJS) и автодовступления в Telegram‑группы по ссылкам.

---

## Структура репозитория

- `vt_project/`
  - `vt.js` – основной сервер: Telegram‑бот‑управлялка + веб‑панель для WhatsApp.
  - `public/` – фронтенд VT Panel.
  - `sessions/` – сессии WhatsApp (Baileys multi‑file auth).
  - `links/` – сохранённые ссылки на группы (JSON по одной ссылке).
  - `users.json` – учёт пользователей Telegram‑бота.
- `tg_project/`
  - `tg.js` – основной сервер TG Panel (Telegram userbot + веб‑панель).
  - `public/` – фронтенд TG Panel.
  - `tg_sessions/` – сессии Telegram userbot‑аккаунтов.
  - `.env` – настройки API Telegram для userbot‑части.

---

## Требования

- Node.js 18+ (рекомендовано 18/20/22).
- npm.
- Один Telegram‑бот (для `vt_project`) и один Telegram‑API‑ключ (для `tg_project`).

---

## Установка зависимостей

```bash
cd vt_project
npm install

cd ../tg_project
npm install
```

---

## Настройка окружения

### 1. VT Panel (WhatsApp)

В папке `vt_project` создайте файл `.env`:

```env
BOT_TOKEN=ваш_telegram_bot_token
WA_PHONE=ваш_номер_телефона_для_whatsapp (например +79991234567)
WEB_PORT=3000
```

### 2. TG Panel (Telegram userbot)

В папке `tg_project` создайте файл `.env`:

```env
TG_API_ID=ваш_app_api_id_из_my.telegram.org
TG_API_HASH=ваш_app_api_hash_из_my.telegram.org
TG_WEB_PORT=4000
```

**Важно:** `TG_API_ID` и `TG_API_HASH` – это не токен бота, а параметры приложения Telegram из раздела *API Development Tools* на `my.telegram.org`.

---

## Запуск VT Panel (WhatsApp)

```bash
cd vt_project
node vt.js
```

После запуска:

- Веб‑панель будет доступна по адресу `http://localhost:3000` (порт можно изменить через `WEB_PORT`).
- Telegram‑бот (по `BOT_TOKEN`) даст доступ к меню управления сессиями WhatsApp.

Основные возможности:

- Создание/активация/удаление WhatsApp‑сессий.
- Получение QR/пэйринг‑кода для привязки WhatsApp.
- Авто‑отслеживание ссылок в группах WhatsApp.
- Автовступление в группы с очередью и настраиваемыми интервалами.
- Просмотр групп сессии, сохранённых ссылок и пользователей бота.

Подробное описание API VT Panel – в `vt_project/README.md`.

---

## Запуск TG Panel (Telegram userbot)

```bash
cd tg_project
node tg.js
```

После запуска:

- Веб‑панель будет доступна по адресу `http://localhost:4000` (порт можно изменить через `TG_WEB_PORT`).  
- Через панель можно создавать Telegram userbot‑сессии, активировать существующие и управлять вступлением в группы.

### Основные функции TG Panel

- **Управление сессиями Telegram (userbot):**
  - создание сессии по номеру телефона;
  - подтверждение кода авторизации из Telegram;
  - активация уже существующей сессии без повторного ввода кода;
  - статусы сессий и длина очереди.

- **Авто‑вступление в Telegram‑группы:**
  - поддержка ссылок формата `https://t.me/...`, `t.me/...`, `@username`, `https://t.me/+invite`;
  - очередь задач на вступление по сессии;
  - настраиваемые интервалы между вступлениями (минимум/максимум, 3–3600 сек);
  - ручное добавление задач через панель.

- **Авто‑мониторинг ссылок:**
  - userbot отслеживает новые сообщения во всех чатах, где он состоит;
  - при появлении t.me/@‑ссылки автоматически добавляет её в очередь вступления для соответствующей сессии.

---

## GitHub: как залить репозиторий

Ниже пример команд для первого пуша в пустой репозиторий (подставьте свой URL вместо `YOUR_REPO_URL`):

```bash
cd E:\project\rasbottg
git init
git add .
git commit -m "Initial commit: VT WhatsApp panel and TG Telegram panel"
git branch -M main
git remote add origin https://github.com/Artem4422/tg-sbor-group.git
git push -u origin main
```

> Перед пушем убедитесь, что `.env` файлы не закоммичены (добавьте их в `.gitignore`), чтобы не выкладывать секретные токены и ключи.





