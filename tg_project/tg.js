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
const tgJoinQueue = new Map();        // name -> [{ link, timestamp }]
const processingJoin = new Map();     // name -> boolean
const tgSessionIntervals = new Map(); // name -> { min, max } (сек)
const tgBadLinks = new Set();         // ссылки, по которым уже была "фатальная" ошибка
const tgJoinedLinks = new Set();      // ссылки, в которые уже успешно вошли
const tgLastJoin = new Map();         // name -> { link, at }
const tgNotifications = [];           // [{ session, link, success, error, at }]
const TG_NOTIF_LIMIT = 500;
const tgBroadcasts = new Map();       // broadcastId -> { sessionName, messageText, mediaType, mediaInfo, minInterval, maxInterval, groups, total, sent, failed, status, startTime, completedAt, userId, recentResults, detailedLog }
const tgScheduledBroadcasts = new Map(); // scheduleId -> { data, scheduledTime, timerId, userId, timestamp }
const tgGroupsCache = new Map();      // name -> { list, ts }
const TG_GROUPS_CACHE_TTL = 2 * 60 * 1000; // 2 минуты

const pushNotification = (payload) => {
  tgNotifications.unshift({ ...payload, id: Date.now() + Math.random() });
  if (tgNotifications.length > TG_NOTIF_LIMIT) {
    tgNotifications.length = TG_NOTIF_LIMIT;
  }
};

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

      if (path.startsWith('+') || /^[a-zA-Z0-9_-]+$/.test(path) === false || link.includes('addlist/')) {
        // пригласительная ссылка вида https://t.me/+xxxx или списки addlist
        const hash = path.replace(/^\+/, '');
        await client.invoke(new Api.messages.ImportChatInvite({ hash }));
      } else {
        // публичная ссылка t.me/username — сначала проверяем, что это именно беседа, а не канал
        try {
          const resolved = await client.invoke(new Api.contacts.ResolveUsername({ username: path }));
          const chat = (resolved.chats && resolved.chats[0]) || null;

          if (!chat) {
            throw new Error('RESOLVE_EMPTY');
          }

          // Api.Channel с megagroup=false — это обычный канал (broadcast), пропускаем
          if (chat instanceof Api.Channel && !chat.megagroup) {
            throw new Error('TARGET_IS_CHANNEL');
          }
        } catch (resolveErr) {
          // Если не смогли разрезолвить или это канал — бросаем ошибку, она уйдет в общий catch ниже
          throw resolveErr;
        }

        // Если это не канал, пробуем вступить как обычно
        await client.invoke(new Api.channels.JoinChannel({
          channel: link,
        })).catch(async () => {
          await client.invoke(new Api.channels.JoinChannel({
            channel: `https://t.me/${path}`,
          }));
        });
      }

      console.log(`[TG_JOIN] ${name}: успешно вступил по ссылке ${link}`);
      tgJoinedLinks.add(link);
      tgLastJoin.set(name, { link, at: Date.now() });
      pushNotification({ session: name, link, success: true, error: null, at: Date.now() });
    } catch (e) {
      console.error(`[TG_JOIN_ERROR] ${name}: ${link} - ${e.message}`);
      pushNotification({ session: name, link, success: false, error: e.message, at: Date.now() });

      const msg = (e.message || '').toLowerCase();
      if (
        msg.includes('user_already_participant') ||
        msg.includes('username_invalid') ||
        msg.includes('cannot cast inputpeeruser') ||
        msg.includes('channel_invalid') ||
        msg.includes('invite_hash_invalid') ||
        msg.includes('invite_hash_expired') ||
        msg.includes('target_is_channel') ||
        msg.includes('resolve_empty')
      ) {
        // Помечаем ссылку как "бесполезную", чтобы больше не ставить её в очередь
        tgBadLinks.add(link);
        console.log(`[TG_JOIN_REMOVE] ${name}: ссылка больше не будет использоваться ${link}`);
      }
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
  // Если по ссылке уже была фатальная ошибка или успешный вход — больше не трогаем
  if (tgBadLinks.has(link) || tgJoinedLinks.has(link)) {
    console.log(`[TG_QUEUE_SKIP] ${name}: ссылка уже помечена как обработанная ${link}`);
    return;
  }

  if (!tgJoinQueue.has(name)) {
    tgJoinQueue.set(name, []);
  }
  const queue = tgJoinQueue.get(name);

  // Не кладём дубликаты в очередь
  if (queue.some((t) => t.link === link)) {
    console.log(`[TG_QUEUE_DUP] ${name}: ссылка уже есть в очереди ${link}`);
    return;
  }

  queue.push({ link, timestamp: Date.now() });
  io.emit('tg_join_queue_update', { name });
  processJoinQueue(io, name).catch(e => {
    console.error(`[TG_JOIN_QUEUE_PROCESS] ${name}:`, e.message);
  });
}

// --- Функции рассылки ---
async function fetchGroups(io, name) {
  const cached = tgGroupsCache.get(name);
  if (cached && (Date.now() - cached.ts) < TG_GROUPS_CACHE_TTL) {
    return cached.list;
  }

  const client = await getOrCreateClient(io, name);
  if (!client) throw new Error('Сессия не активна');
  
  try {
    await client.connect();
  } catch (e) {
    throw new Error('Не удалось подключиться к сессии');
  }

  const dialogs = await client.getDialogs({});
  const groups = [];

  for (const d of dialogs) {
    const ent = d.entity;
    if (!ent) continue;

    // Обычные групповые чаты
    if (ent instanceof Api.Chat) {
      groups.push({
        id: String(ent.id),
        title: ent.title || '(без названия)',
        type: 'chat',
        size: ent.participantsCount || 0,
      });
      continue;
    }

    // Супергруппы (megagroup=true)
    if (ent instanceof Api.Channel && ent.megagroup) {
      groups.push({
        id: String(ent.id),
        title: ent.title || ent.username || '(без названия)',
        type: 'supergroup',
        size: ent.participantsCount || 0,
      });
    }
  }

  const sortedGroups = groups.sort((a, b) => a.title.localeCompare(b.title, 'ru'));
  tgGroupsCache.set(name, { list: sortedGroups, ts: Date.now() });
  return sortedGroups;
}

function parseCustomInterval(text) {
  const cleanText = text.toLowerCase().replace(/\s+/g, '');
  
  // Диапазон: 10-30 или 10-30сек
  const rangeMatch = cleanText.match(/^(\d+)-(\d+)(сек)?$/);
  if (rangeMatch) {
    let min = parseInt(rangeMatch[1]);
    let max = parseInt(rangeMatch[2]);
    
    if (min >= 3 && max >= min && max <= 3600) {
      return { min, max };
    }
  }
  
  // Фиксированное значение: 5 или 5сек
  const singleMatch = cleanText.match(/^(\d+)(сек)?$/);
  if (singleMatch) {
    let value = parseInt(singleMatch[1]);
    
    if (value >= 3 && value <= 3600) {
      return { min: value, max: value };
    }
  }
  
  return null;
}

function parseScheduledTime(text) {
  const cleanText = text.toLowerCase().trim();
  const now = new Date();
  
  // Полная дата: ДД.ММ.ГГГГ ЧЧ:ММ
  const fullDateMatch = cleanText.match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})$/);
  if (fullDateMatch) {
    const [, day, month, year, hours, minutes] = fullDateMatch;
    const date = new Date(`${year}-${month}-${day}T${hours}:${minutes}:00`);
    return date.getTime();
  }
  
  // Сегодня ЧЧ:ММ
  const todayMatch = cleanText.match(/^сегодня\s+(\d{2}):(\d{2})$/);
  if (todayMatch) {
    const [, hours, minutes] = todayMatch;
    const date = new Date();
    date.setHours(parseInt(hours), parseInt(minutes), 0, 0);
    return date.getTime();
  }
  
  // Завтра ЧЧ:ММ
  const tomorrowMatch = cleanText.match(/^завтра\s+(\d{2}):(\d{2})$/);
  if (tomorrowMatch) {
    const [, hours, minutes] = tomorrowMatch;
    const date = new Date();
    date.setDate(date.getDate() + 1);
    date.setHours(parseInt(hours), parseInt(minutes), 0, 0);
    return date.getTime();
  }
  
  // Через N часов/минут
  const throughMatch = cleanText.match(/^через\s+(\d+)\s+(час|часа|часов|минуту|минуты|минут)$/);
  if (throughMatch) {
    const [, amount, unit] = throughMatch;
    const multiplier = unit.includes('час') ? 60 * 60 * 1000 : 60 * 1000;
    return now.getTime() + (parseInt(amount) * multiplier);
  }
  
  return null;
}

function formatTimeRemaining(ms) {
  const seconds = Math.floor(ms / 1000);
  const minutes = Math.floor(seconds / 60);
  const hours = Math.floor(minutes / 60);
  const days = Math.floor(hours / 24);
  
  if (days > 0) {
    return `${days}д ${hours % 24}ч ${minutes % 60}м`;
  } else if (hours > 0) {
    return `${hours}ч ${minutes % 60}м`;
  } else if (minutes > 0) {
    return `${minutes}м ${seconds % 60}с`;
  } else {
    return `${seconds}с`;
  }
}

async function runBroadcast(io, broadcastId) {
  const broadcast = tgBroadcasts.get(broadcastId);
  if (!broadcast) return;
  
  const client = await getOrCreateClient(io, broadcast.sessionName);
  if (!client) {
    broadcast.status = 'error';
    io.emit('tg_broadcast_update', { broadcastId, broadcast });
    return;
  }
  
  try {
    await client.connect();
  } catch (e) {
    broadcast.status = 'error';
    io.emit('tg_broadcast_update', { broadcastId, broadcast });
    return;
  }
  
  broadcast.recentResults = [];
  broadcast.detailedLog = [];
  
  for (let i = 0; i < broadcast.groups.length; i++) {
    if (broadcast.status !== 'running') break;
    
    const group = broadcast.groups[i];
    const delay = Math.random() * (broadcast.maxInterval - broadcast.minInterval) + broadcast.minInterval;
    
    try {
      const startTime = Date.now();
      
      // Получаем entity группы для правильной отправки
      let groupEntity;
      try {
        const groupId = group.type === 'supergroup' ? parseInt(group.id) : parseInt(group.id);
        groupEntity = await client.getEntity(groupId);
      } catch (e) {
        // Если не удалось получить entity, пробуем использовать ID напрямую
        groupEntity = parseInt(group.id);
      }
      
      // Отправка сообщения
      if (broadcast.mediaType && broadcast.mediaInfo) {
        // Отправка с медиа
        const sendOptions = {
          file: broadcast.mediaInfo.url,
        };
        
        if (broadcast.messageText) {
          sendOptions.caption = broadcast.messageText;
        }
        
        // Для документов можно указать имя файла
        if (broadcast.mediaType === 'document' && broadcast.mediaInfo.fileName) {
          sendOptions.fileName = broadcast.mediaInfo.fileName;
        }
        
        await client.sendFile(groupEntity, sendOptions);
      } else {
        // Отправка только текста
        await client.sendMessage(groupEntity, { message: broadcast.messageText });
      }
      
      const sendTime = Date.now() - startTime;
      broadcast.sent++;
      
      const result = {
        groupName: group.title,
        groupId: group.id,
        groupSize: group.size,
        success: true,
        sendTime: sendTime,
        timestamp: new Date().toLocaleTimeString('ru-RU'),
        nextDelay: i < broadcast.groups.length - 1 ? delay : 0
      };
      
      broadcast.recentResults.push(result);
      broadcast.detailedLog.push(result);
      
      if (broadcast.recentResults.length > 5) {
        broadcast.recentResults.shift();
      }
      
      // Обновление прогресса каждые 3 сообщения или на последнем
      if (broadcast.sent === 1 || broadcast.sent % 3 === 0 || i === broadcast.groups.length - 1) {
        io.emit('tg_broadcast_update', { broadcastId, broadcast });
      }
      
    } catch (error) {
      broadcast.failed++;
      
      const result = {
        groupName: group.title,
        groupId: group.id,
        groupSize: group.size,
        success: false,
        error: error.message,
        timestamp: new Date().toLocaleTimeString('ru-RU'),
        nextDelay: i < broadcast.groups.length - 1 ? delay : 0
      };
      
      broadcast.recentResults.push(result);
      broadcast.detailedLog.push(result);
      
      if (broadcast.recentResults.length > 5) {
        broadcast.recentResults.shift();
      }
      
      console.error(`[TG_BROADCAST_ERROR] ${group.id}:`, error.message);
      io.emit('tg_broadcast_update', { broadcastId, broadcast });
    }
    
    // Пауза перед следующим сообщением
    if (i < broadcast.groups.length - 1 && broadcast.status === 'running') {
      await new Promise(resolve => setTimeout(resolve, delay * 1000));
    }
  }
  
  broadcast.status = 'completed';
  broadcast.completedAt = Date.now();
  io.emit('tg_broadcast_update', { broadcastId, broadcast });
}

async function executeScheduledBroadcast(io, scheduleId) {
  const scheduled = tgScheduledBroadcasts.get(scheduleId);
  if (!scheduled) return;
  
  try {
    const { data } = scheduled;
    const client = await getOrCreateClient(io, data.sessionName);
    
    if (!client) {
      tgScheduledBroadcasts.delete(scheduleId);
      io.emit('tg_scheduled_broadcast_cancelled', { scheduleId, reason: 'Сессия неактивна' });
      return;
    }
    
    const groups = await fetchGroups(io, data.sessionName);
    if (groups.length === 0) {
      tgScheduledBroadcasts.delete(scheduleId);
      io.emit('tg_scheduled_broadcast_cancelled', { scheduleId, reason: 'Нет групп' });
      return;
    }
    
    const broadcastId = Date.now().toString();
    tgBroadcasts.set(broadcastId, {
      sessionName: data.sessionName,
      messageText: data.messageText,
      mediaType: data.mediaType,
      mediaInfo: data.mediaInfo,
      minInterval: data.minInterval,
      maxInterval: data.maxInterval,
      groups,
      total: groups.length,
      sent: 0,
      failed: 0,
      status: 'running',
      startTime: Date.now(),
      scheduled: true,
      userId: scheduled.userId,
      recentResults: [],
      detailedLog: []
    });
    
    io.emit('tg_broadcast_started', { broadcastId, scheduled: true });
    runBroadcast(io, broadcastId).catch(e => {
      console.error('[TG_SCHEDULED_BROADCAST_ERROR]', e);
    });
    
  } catch (error) {
    console.error('[TG_SCHEDULED_EXECUTION_ERROR]', { scheduleId, error: error.message });
    tgScheduledBroadcasts.delete(scheduleId);
    io.emit('tg_scheduled_broadcast_cancelled', { scheduleId, reason: error.message });
  }
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
      lastJoin: tgLastJoin.get(name) || null,
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
      lastJoin: tgLastJoin.get(name) || null,
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

// GET /api/tg/sessions/:name/groups — список групп/чатов, где состоит аккаунт
app.get('/api/tg/sessions/:name/groups', async (req, res) => {
  const { name } = req.params;
  try {
    const client = await getOrCreateClient(io, name);
    await client.connect();

    const dialogs = await client.getDialogs({});
    const groups = [];

    for (const d of dialogs) {
      const ent = d.entity;
      if (!ent) continue;

      // Обычные групповые чаты
      if (ent instanceof Api.Chat) {
        groups.push({
          id: String(ent.id),
          title: ent.title || '(без названия)',
          type: 'chat',
        });
        continue;
      }

      // Супергруппы (megagroup=true)
      if (ent instanceof Api.Channel && ent.megagroup) {
        groups.push({
          id: String(ent.id),
          title: ent.title || ent.username || '(без названия)',
          type: 'supergroup',
        });
      }
    }

    res.json({ groups });
  } catch (e) {
    res.status(500).json({ error: e.message, groups: [] });
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

// GET /api/tg/notifications?limit=100
app.get('/api/tg/notifications', (req, res) => {
  const limit = Math.max(1, Math.min(1000, Number(req.query.limit) || 100));
  res.json({ notifications: tgNotifications.slice(0, limit) });
});

// DELETE /api/tg/notifications
app.delete('/api/tg/notifications', (req, res) => {
  tgNotifications.length = 0;
  res.json({ success: true });
});

// --- API для рассылки ---

// POST /api/tg/sessions/:name/broadcast — создать и запустить рассылку
app.post('/api/tg/sessions/:name/broadcast', async (req, res) => {
  const { name } = req.params;
  const { messageText, mediaType, mediaInfo, minInterval, maxInterval, scheduledTime } = req.body || {};
  
  try {
    const client = await getOrCreateClient(io, name);
    if (!client || tgSessionStatus.get(name) !== 'active') {
      return res.status(400).json({ error: 'Сессия не активна' });
    }
    
    if (!messageText && !mediaType) {
      return res.status(400).json({ error: 'Нужен текст или медиафайл' });
    }
    
    const minInt = Number(minInterval) || 5;
    const maxInt = Number(maxInterval) || 15;
    
    if (minInt < 3 || maxInt < minInt || maxInt > 3600) {
      return res.status(400).json({ error: 'Интервалы должны быть от 3 до 3600 сек, min ≤ max' });
    }
    
    const groups = await fetchGroups(io, name);
    if (groups.length === 0) {
      return res.status(400).json({ error: 'Нет групп для рассылки' });
    }
    
    // Если указано время планирования
    if (scheduledTime) {
      const scheduledTimestamp = typeof scheduledTime === 'string' ? parseScheduledTime(scheduledTime) : Number(scheduledTime);
      
      if (!scheduledTimestamp || scheduledTimestamp <= Date.now()) {
        return res.status(400).json({ error: 'Время должно быть в будущем' });
      }
      
      const scheduleId = `sched_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const delay = scheduledTimestamp - Date.now();
      
      const timerId = setTimeout(() => {
        executeScheduledBroadcast(io, scheduleId).catch(e => {
          console.error('[TG_SCHEDULED_TIMER_ERROR]', e);
        });
      }, delay);
      
      tgScheduledBroadcasts.set(scheduleId, {
        data: {
          sessionName: name,
          messageText,
          mediaType,
          mediaInfo,
          minInterval: minInt,
          maxInterval: maxInt
        },
        scheduledTime: scheduledTimestamp,
        timerId,
        userId: req.body.userId || null,
        timestamp: Date.now()
      });
      
      return res.json({
        success: true,
        scheduleId,
        scheduledTime: scheduledTimestamp,
        timeRemaining: formatTimeRemaining(delay)
      });
    }
    
    // Немедленная рассылка
    const broadcastId = Date.now().toString();
    tgBroadcasts.set(broadcastId, {
      sessionName: name,
      messageText,
      mediaType,
      mediaInfo,
      minInterval: minInt,
      maxInterval: maxInt,
      groups,
      total: groups.length,
      sent: 0,
      failed: 0,
      status: 'running',
      startTime: Date.now(),
      scheduled: false,
      userId: req.body.userId || null,
      recentResults: [],
      detailedLog: []
    });
    
    io.emit('tg_broadcast_started', { broadcastId });
    
    // Запускаем рассылку асинхронно
    runBroadcast(io, broadcastId).catch(e => {
      console.error('[TG_BROADCAST_RUN_ERROR]', e);
    });
    
    res.json({
      success: true,
      broadcastId,
      total: groups.length,
      minInterval: minInt,
      maxInterval: maxInt
    });
    
  } catch (e) {
    console.error('[TG_BROADCAST_CREATE_ERROR]', e.message);
    res.status(500).json({ error: e.message });
  }
});

// GET /api/tg/broadcasts — список всех рассылок
app.get('/api/tg/broadcasts', (req, res) => {
  try {
    const broadcasts = Array.from(tgBroadcasts.entries()).map(([id, b]) => ({
      id,
      sessionName: b.sessionName,
      status: b.status,
      total: b.total,
      sent: b.sent,
      failed: b.failed,
      startTime: b.startTime,
      completedAt: b.completedAt,
      scheduled: b.scheduled || false
    }));
    res.json({ broadcasts });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// GET /api/tg/broadcasts/:id — информация о рассылке
app.get('/api/tg/broadcasts/:id', (req, res) => {
  const { id } = req.params;
  const broadcast = tgBroadcasts.get(id);
  
  if (!broadcast) {
    return res.status(404).json({ error: 'Рассылка не найдена' });
  }
  
  res.json({ broadcast });
});

// POST /api/tg/broadcasts/:id/stop — остановить рассылку
app.post('/api/tg/broadcasts/:id/stop', (req, res) => {
  const { id } = req.params;
  const broadcast = tgBroadcasts.get(id);
  
  if (!broadcast) {
    return res.status(404).json({ error: 'Рассылка не найдена' });
  }
  
  broadcast.status = 'stopped';
  io.emit('tg_broadcast_update', { broadcastId: id, broadcast });
  res.json({ success: true });
});

// GET /api/tg/scheduled — список запланированных рассылок
app.get('/api/tg/scheduled', (req, res) => {
  try {
    const scheduled = Array.from(tgScheduledBroadcasts.entries()).map(([id, s]) => ({
      id,
      sessionName: s.data.sessionName,
      scheduledTime: s.scheduledTime,
      timeRemaining: formatTimeRemaining(s.scheduledTime - Date.now()),
      userId: s.userId
    }));
    res.json({ scheduled });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// DELETE /api/tg/scheduled/:id — отменить запланированную рассылку
app.delete('/api/tg/scheduled/:id', (req, res) => {
  const { id } = req.params;
  const scheduled = tgScheduledBroadcasts.get(id);
  
  if (!scheduled) {
    return res.status(404).json({ error: 'Запланированная рассылка не найдена' });
  }
  
  clearTimeout(scheduled.timerId);
  tgScheduledBroadcasts.delete(id);
  io.emit('tg_scheduled_broadcast_cancelled', { scheduleId: id, reason: 'Отменено пользователем' });
  res.json({ success: true });
});

// POST /api/tg/sessions/:name/groups/refresh — обновить кеш групп
app.post('/api/tg/sessions/:name/groups/refresh', async (req, res) => {
  const { name } = req.params;
  try {
    tgGroupsCache.delete(name);
    const groups = await fetchGroups(io, name);
    res.json({ success: true, groups, count: groups.length });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
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


