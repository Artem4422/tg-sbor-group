import 'dotenv/config';
import fs from 'fs';
import path from 'path';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';
import { fileURLToPath } from 'url';
import { TelegramClient } from 'telegram';
import { NewMessage } from 'telegram/events/index.js';
import { StringSession } from 'telegram/sessions/index.js';
import { Api } from 'telegram/tl/index.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

// --- Константы и директории ---
const TG_API_ID = Number(process.env.TG_API_ID || 0);
const TG_API_HASH = process.env.TG_API_HASH || '';
const TG_WEB_PORT = Number(process.env.TG_WEB_PORT || 4000);

if (!TG_API_ID || !TG_API_HASH) {
  console.error('TG_API_ID или TG_API_HASH не заданы в .env (в папке tg_project)');
  process.exit(1);
}

const TG_SESSIONS_DIR = path.join(process.cwd(), 'tg_sessions');
fs.mkdirSync(TG_SESSIONS_DIR, { recursive: true });

// --- Внутреннее состояние ---
const tgClients = new Map();          // name -> TelegramClient
const tgSessionStatus = new Map();    // name -> 'inactive' | 'auth' | 'active' | 'error'
const tgSessionMeta = new Map();      // name -> { phone }
const tgJoinQueue = new Map();        // name -> [{ link, timestamp, status }]
const processingJoin = new Map();     // name -> boolean
const tgSessionIntervals = new Map(); // name -> { min, max } (сек)

// --- Вспомогательные функции ---
const statusEmoji = (s) => ({
  inactive: '🔴',
  auth: '🟡',
  active: '🟢',
  error: '⚠️',
}[s] || '⚪');

const sessionHuman = (s) =>
  s === 'active' ? 'Активна' :
  s === 'auth' ? 'Ожидает кода' :
  s === 'error' ? 'Ошибка' :
  'Не активна';

const listTgSessions = () =>
  fs.readdirSync(TG_SESSIONS_DIR)
    .filter(f => f.endsWith('.session'))
    .map(f => f.replace(/\.session$/, ''));

const safeSessionPath = (name) => {
  const clean = String(name || '').trim().replace(/[^\w\-]+/g, '_').toLowerCase();
  if (!clean) throw new Error('Некорректное имя сессии');
  const file = path.join(TG_SESSIONS_DIR, `${clean}.session`);
  const base = path.resolve(TG_SESSIONS_DIR);
  const real = path.resolve(file);
  if (!real.startsWith(base)) throw new Error('Недопустимый путь сессии');
  return { safeName: clean, filePath: file };
};

// Нормализация телеграм-ссылок/юзернеймов в единый формат https://t.me/...
const normalizeTgLink = (raw) => {
  const s = String(raw || '').trim();
  if (!s) return null;

  // @username
  if (s.startsWith('@')) {
    const u = s.slice(1).trim();
    if (!u) return null;
    return `https://t.me/${u}`;
  }

  // t.me/username (без протокола)
  if (s.toLowerCase().startsWith('t.me/')) {
    return `https://${s}`;
  }

  // http/https t.me/...
  if (s.toLowerCase().startsWith('http://t.me/') || s.toLowerCase().startsWith('https://t.me/')) {
    return s.replace(/^http:\/\//i, 'https://');
  }

  return null;
};

const setStatus = (io, name, status) => {
  tgSessionStatus.set(name, status);
  console.log(`[TG_STATUS] ${name}: ${statusEmoji(status)} ${sessionHuman(status)}`);
  if (io) {
    io.emit('tg_session_status', {
      name,
      status,
      statusText: sessionHuman(status),
    });
  }
};

// --- Telegram client helpers ---
async function getOrCreateClient(io, name) {
  const { filePath } = safeSessionPath(name);
  if (tgClients.has(name)) {
    return tgClients.get(name);
  }

  let sessionString = '';
  if (fs.existsSync(filePath)) {
    try {
      sessionString = fs.readFileSync(filePath, 'utf8');
    } catch {
      sessionString = '';
    }
  }

  const stringSession = new StringSession(sessionString || '');
  const client = new TelegramClient(stringSession, TG_API_ID, TG_API_HASH, {
    connectionRetries: 3,
  });

  // Автоматическое отслеживание ссылок в новых сообщениях
  client.addEventHandler(async (event) => {
    try {
      const msg = event?.message;
      if (!msg) return;
      const text = msg.message || '';
      if (!text) return;

      console.log(`[TG_MONITOR] ${name}: получено сообщение: "${text.slice(0,80)}"`);

      const linkRe = /(@[a-zA-Z0-9_]{4,}|https?:\/\/t\.me\/[^\s]+|t\.me\/[^\s]+)/gi;
      const matches = text.match(linkRe) || [];
      for (const raw of matches) {
        const normalized = normalizeTgLink(raw);
        if (!normalized) continue;
        console.log(`[TG_MONITOR] ${name}: найдена ссылка ${raw} -> ${normalized}`);
        addToJoinQueue(io, name, normalized);
      }
    } catch (e) {
      console.error('[TG_MONITOR_ERROR]', e.message);
    }
  }, new NewMessage({}));

  tgClients.set(name, client);

  client.session.setDC(2, '149.154.167.51', 443); // дефолтный датацентр (можно не трогать)

  client.addEventHandler(() => {}, new Api.UpdatesTooLong());

  client.on('disconnected', () => {
    console.log(`[TG_CLIENT] ${name} disconnected`);
    setStatus(io, name, 'inactive');
  });

  return client;
}

async function saveClientSession(name) {
  const client = tgClients.get(name);
  if (!client) return;
  const { filePath } = safeSessionPath(name);
  const session = client.session.save();
  fs.writeFileSync(filePath, session, 'utf8');
}

// --- Процесс авторизации: шаг 1 (отправка кода) ---
async function requestAuthCode(io, name, phoneRaw) {
  const phone = String(phoneRaw || '').trim();
  if (!phone) {
    throw new Error('Номер телефона пустой или не задан');
  }

  const client = await getOrCreateClient(io, name);
  setStatus(io, name, 'auth');

  await client.connect();

  console.log(`[TG_AUTH_DEBUG] ${name}: phone="${phone}"`);

  // Используем обертку TelegramClient.sendCode(apiCredentials, phoneNumber)
  const result = await client.sendCode(
    { apiId: TG_API_ID, apiHash: TG_API_HASH },
    phone,
  );

  tgSessionMeta.set(name, { phone, phoneCodeHash: result.phoneCodeHash });
  console.log(`[TG_AUTH] Код отправлен на ${phone} для сессии ${name}`);
  return { phone, phoneCodeHash: result.phoneCodeHash };
}

// --- Процесс авторизации: шаг 2 (подтверждение кода) ---
async function confirmAuthCode(io, name, code) {
  const meta = tgSessionMeta.get(name);
  if (!meta || !meta.phone || !meta.phoneCodeHash) {
    throw new Error('Нет активного запроса кода для этой сессии');
  }
  const client = await getOrCreateClient(io, name);

  await client.connect();

  // Низкоуровневый вызов auth.SignIn через MTProto
  const result = await client.invoke(new Api.auth.SignIn({
    phoneNumber: meta.phone,
    phoneCodeHash: meta.phoneCodeHash,
    phoneCode: String(code).trim(),
  }));

  if (!result || !result.user) {
    throw new Error('Не удалось войти. Проверьте код.');
  }

  await saveClientSession(name);
  setStatus(io, name, 'active');

  console.log(`[TG_AUTH] Сессия ${name} успешно активирована`);
}

// --- Очередь вступлений ---
async function processJoinQueue(io, name) {
  if (processingJoin.get(name)) return;
  processingJoin.set(name, true);

  const queue = tgJoinQueue.get(name) || [];
  if (!queue.length) {
    processingJoin.set(name, false);
    return;
  }

  const client = await getOrCreateClient(io, name);
  try {
    await client.connect();
  } catch (e) {
    console.error(`[TG_JOIN] ${name}: ошибка подключения: ${e.message}`);
    processingJoin.set(name, false);
    return;
  }

  const intervals = tgSessionIntervals.get(name) || { min: 5, max: 30 };

  while (queue.length) {
    const task = queue.shift();
    if (!task) break;
    io.emit('tg_join_queue_update', { name });

    const link = task.link;
    console.log(`[TG_JOIN] ${name}: попытка вступить по ссылке ${link}`);

    try {
      if (!link.startsWith('https://t.me/')) {
        throw new Error('Неверная t.me ссылка');
      }

      const path = link.replace('https://t.me/', '').split('?')[0].split('/')[0];

      if (path.startsWith('+') || /^[a-zA-Z0-9_-]+$/.test(path) === false) {
        // пригласительная ссылка вида https://t.me/+xxxx
        const hash = path.replace(/^\+/, '');
        await client.invoke(new Api.messages.ImportChatInvite({ hash }));
      } else {
        // публичная группа/канал @username
        await client.invoke(new Api.channels.JoinChannel({
          channel: link,
        })).catch(async () => {
          await client.invoke(new Api.channels.JoinChannel({
            channel: `https://t.me/${path}`,
          }));
        });
      }

      console.log(`[TG_JOIN] ${name}: успешно вступил по ссылке ${link}`);
    } catch (e) {
      console.error(`[TG_JOIN_ERROR] ${name}: ${link} - ${e.message}`);
    }

    await saveClientSession(name);

    const delayMin = Math.max(3, intervals.min || 5);
    const delayMax = Math.min(3600, intervals.max || 30);
    const delay = Math.floor(Math.random() * (delayMax - delayMin) + delayMin);
    console.log(`[TG_JOIN] ${name}: пауза ${delay} сек до следующей ссылки`);
    await new Promise(r => setTimeout(r, delay * 1000));
  }

  processingJoin.set(name, false);
}

function addToJoinQueue(io, name, link) {
  if (!tgJoinQueue.has(name)) {
    tgJoinQueue.set(name, []);
  }
  const queue = tgJoinQueue.get(name);
  queue.push({ link, timestamp: Date.now() });
  io.emit('tg_join_queue_update', { name });
  processJoinQueue(io, name).catch(e => {
    console.error(`[TG_JOIN_QUEUE_PROCESS] ${name}:`, e.message);
  });
}

// --- Web server / API ---
const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static(path.join(__dirname, 'public')));

// GET /api/tg/sessions
app.get('/api/tg/sessions', (req, res) => {
  try {
    const names = listTgSessions();
    const result = names.map(name => ({
      name,
      status: tgSessionStatus.get(name) || 'inactive',
      statusText: sessionHuman(tgSessionStatus.get(name) || 'inactive'),
      phone: tgSessionMeta.get(name)?.phone || null,
      joinQueueLength: (tgJoinQueue.get(name)?.length) || 0,
      intervals: tgSessionIntervals.get(name) || { min: 5, max: 30 },
    }));
    res.json(result);
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/tg/sessions  { name, phone }
app.post('/api/tg/sessions', async (req, res) => {
  const { name, phone } = req.body || {};
  if (!name || !phone) {
    return res.status(400).json({ error: 'name и phone обязательны' });
  }
  try {
    const { safeName } = safeSessionPath(name);
    const { filePath } = safeSessionPath(safeName);
    if (fs.existsSync(filePath)) {
      return res.status(400).json({ error: 'Сессия уже существует' });
    }

    fs.writeFileSync(filePath, '', 'utf8');
    setStatus(io, safeName, 'auth');
    await requestAuthCode(io, safeName, phone);

    res.json({ success: true, name: safeName, phone });
  } catch (e) {
    console.error('[TG_SESSION_CREATE_ERROR]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// POST /api/tg/sessions/:name/confirm  { code }
app.post('/api/tg/sessions/:name/confirm', async (req, res) => {
  const { name } = req.params;
  const { code } = req.body || {};
  if (!code) {
    return res.status(400).json({ error: 'code обязателен' });
  }
  try {
    await confirmAuthCode(io, name, String(code).trim());
    res.json({ success: true });
  } catch (e) {
    console.error('[TG_CONFIRM_ERROR]', e.message);
    setStatus(io, name, 'error');
    res.status(500).json({ error: e.message });
  }
});

// POST /api/tg/sessions/:name/activate  — переподключить уже авторизованную сессию
app.post('/api/tg/sessions/:name/activate', async (req, res) => {
  const { name } = req.params;
  try {
    const client = await getOrCreateClient(io, name);
    setStatus(io, name, 'auth');
    await client.connect();
    setStatus(io, name, 'active');
    res.json({ success: true });
  } catch (e) {
    console.error('[TG_ACTIVATE_ERROR]', e.message);
    setStatus(io, name, 'error');
    res.status(500).json({ error: e.message });
  }
});

// GET /api/tg/sessions/:name
app.get('/api/tg/sessions/:name', (req, res) => {
  const { name } = req.params;
  try {
    const { filePath } = safeSessionPath(name);
    const exists = fs.existsSync(filePath);
    const status = tgSessionStatus.get(name) || (exists ? 'inactive' : 'inactive');
    res.json({
      name,
      exists,
      status,
      statusText: sessionHuman(status),
      phone: tgSessionMeta.get(name)?.phone || null,
      joinQueueLength: (tgJoinQueue.get(name)?.length) || 0,
      intervals: tgSessionIntervals.get(name) || { min: 5, max: 30 },
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/tg/sessions/:name
app.delete('/api/tg/sessions/:name', async (req, res) => {
  const { name } = req.params;
  try {
    const { filePath } = safeSessionPath(name);

    if (tgClients.has(name)) {
      try {
        await tgClients.get(name).disconnect();
      } catch { /* ignore */ }
      tgClients.delete(name);
    }

    tgSessionStatus.delete(name);
    tgSessionMeta.delete(name);
    tgJoinQueue.delete(name);
    processingJoin.delete(name);

    try {
      fs.unlinkSync(filePath);
    } catch { /* ignore */ }

    io.emit('tg_session_status', { name, status: 'deleted' });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// POST /api/tg/sessions/:name/join { link }
app.post('/api/tg/sessions/:name/join', async (req, res) => {
  const { name } = req.params;
  const { link } = req.body || {};

  const normalized = normalizeTgLink(link);
  if (!normalized) {
    return res.status(400).json({ error: 'Нужна ссылка вида https://t.me/..., t.me/username или @username' });
  }
  try {
    addToJoinQueue(io, name, normalized);
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// PUT /api/tg/sessions/:name/intervals { min, max }
app.put('/api/tg/sessions/:name/intervals', (req, res) => {
  const { name } = req.params;
  const { min, max } = req.body || {};

  const minNum = Number(min);
  const maxNum = Number(max);

  if (!Number.isFinite(minNum) || !Number.isFinite(maxNum) || minNum < 3 || maxNum < minNum || maxNum > 3600) {
    return res.status(400).json({ error: 'Интервалы должны быть от 3 до 3600 сек, min ≤ max' });
  }

  tgSessionIntervals.set(name, { min: minNum, max: maxNum });
  io.emit('tg_session_status', {
    name,
    status: tgSessionStatus.get(name) || 'inactive',
    statusText: sessionHuman(tgSessionStatus.get(name) || 'inactive'),
    intervals: { min: minNum, max: maxNum },
  });

  res.json({ success: true, intervals: { min: minNum, max: maxNum } });
});

// WebSocket
io.on('connection', (socket) => {
  console.log('[TG_WS] client connected');
  socket.on('disconnect', () => {
    console.log('[TG_WS] client disconnected');
  });
});

// Инициализация
(async () => {
  try {
    const existing = listTgSessions();
    existing.forEach(name => {
      tgSessionStatus.set(name, 'inactive');
    });

    httpServer.listen(TG_WEB_PORT, '0.0.0.0', () => {
      console.log(`🌐 TG Web panel started on http://localhost:${TG_WEB_PORT}`);
      console.log(`🌐 TG Web panel also available on http://0.0.0.0:${TG_WEB_PORT}`);
    });
  } catch (e) {
    console.error('[TG_INIT_ERROR]', e);
    process.exit(1);
  }
})();

process.on('unhandledRejection', (e) => console.error('[TG_UNHANDLED]', e));
process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));


