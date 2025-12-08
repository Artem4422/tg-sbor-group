import 'dotenv/config';
import { Telegraf, Markup } from 'telegraf';
import { makeWASocket, DisconnectReason, useMultiFileAuthState } from '@whiskeysockets/baileys';
import pino from 'pino';
import qrcode from 'qrcode';
import fs from 'fs';
import path from 'path';

if (!process.env.BOT_TOKEN) { console.error('BOT_TOKEN не задан'); process.exit(1); }
const bot = new Telegraf(process.env.BOT_TOKEN);

const SESSIONS_DIR        = '/root/rs/sessions';
const QR_TIMEOUT_CREATE   = 60;
const QR_TIMEOUT_ACTIVATE = 60;
const GROUPS_PAGE_SIZE    = 10;
const GROUPS_CACHE_TTL    = 2 * 60 * 1000;

if (!fs.existsSync(SESSIONS_DIR)) fs.mkdirSync(SESSIONS_DIR, { recursive: true });

const activeSocks   = new Map();
const sessionStatus = new Map();
const userData      = new Map();
const qrTimers      = new Map();
const qrAnchors     = new Map();
const groupsCache   = new Map();
const infoAnchors   = new Map();
const renderToggle  = new Map();
const broadcasts    = new Map();
const scheduledBroadcasts = new Map();
const usersFile = '/root/rs/users.json';

const statusEmoji = (s) => ({ inactive:'🔴', qr:'🟡', syncing:'🟠', active:'🟢', error:'⚠️' }[s] || '⚪');
const sessionHuman = (st) =>
  st === 'active'  ? 'Активна' :
  st === 'syncing' ? 'Синхронизация…' :
  st === 'qr'      ? 'Ожидает сканирования QR' :
  st === 'error'   ? 'Ошибка' : 'Не активна';


const loadUsers = () => {
  try {
    if (fs.existsSync(usersFile)) {
      const data = fs.readFileSync(usersFile, 'utf8');
      return new Map(JSON.parse(data));
    }
  } catch (e) {
    console.error('[USERS_LOAD_ERROR]', e.message);
  }
  return new Map();
};

const saveUsers = (users) => {
  try {
    fs.writeFileSync(usersFile, JSON.stringify([...users]), 'utf8');
  } catch (e) {
    console.error('[USERS_SAVE_ERROR]', e.message);
  }
};

const getAllUsers = () => {
  const usersData = loadUsers();
  return Array.from(usersData.entries()).map(([id, info]) => ({
    id: parseInt(id),
    ...info
  }));
};

const notifyAllUsers = async (message, botInstance) => {
  const allUsers = getAllUsers();
  let successCount = 0;
  let errorCount = 0;
  
  for (const user of allUsers) {
    try {
      await botInstance.telegram.sendMessage(user.id, message);
      successCount++;
    } catch (e) {
      console.error(`[NOTIFY_USER_ERROR] ${user.id}:`, e.message);
      errorCount++;
    }
  }
  
  console.log(`[NOTIFY_ALL] Sent to ${successCount} users, ${errorCount} errors`);
  return { successCount, errorCount };
};

const addUser = (userId, userInfo) => {
  const users = loadUsers();
  users.set(userId.toString(), {
    ...userInfo,
    lastSeen: Date.now(),
    username: userInfo.username || null,
    firstName: userInfo.firstName || null,
    lastName: userInfo.lastName || null,
    status: 'offline',
    currentActivity: null,
    activityStartTime: null
  });
  saveUsers(users);
  console.log(`[USER_ADDED] ${userId}: ${userInfo.firstName || userInfo.username || 'Unknown'}`);
};

const updateUserStatus = (userId, status, activity = null) => {
  const users = loadUsers();
  const userData = users.get(userId.toString());
  if (userData) {
    userData.status = status;
    userData.currentActivity = activity;
    userData.activityStartTime = status !== 'offline' ? Date.now() : null;
    userData.lastSeen = Date.now();
    users.set(userId.toString(), userData);
    saveUsers(users);
    console.log(`[USER_STATUS_UPDATED] ${userId}: ${status}${activity ? ` - ${activity}` : ''}`);
  }
};

const users = loadUsers();


const CLEANUP_RULES = {
  userData: { ttl: 2 * 60 * 60 * 1000, checkInterval: 5 * 60 * 1000 },
  qrTimers: { ttl: 5 * 60 * 1000, checkInterval: 1 * 60 * 1000 },
  groupsCache: { ttl: 10 * 60 * 1000, checkInterval: 2 * 60 * 1000 },
  infoAnchors: { ttl: 30 * 60 * 1000, checkInterval: 5 * 60 * 1000 },
  renderToggle: { ttl: 15 * 60 * 1000, checkInterval: 3 * 60 * 1000 },
  broadcasts: { ttl: 60 * 60 * 1000, checkInterval: 10 * 60 * 1000 },
  scheduledBroadcasts: { ttl: 24 * 60 * 60 * 1000, checkInterval: 30 * 60 * 1000 }
};

const cleanupMap = (mapName, mapObj, getTimestamp) => {
  const rule = CLEANUP_RULES[mapName];
  if (!rule) return;
  
  const now = Date.now();
  const toDelete = [];
  
  for (const [key, value] of mapObj.entries()) {
    let timestamp;
    
    if (mapName === 'infoAnchors') {
      timestamp = value.lastUsed;
    } else if (mapName === 'renderToggle') {
      timestamp = value.timestamp;
    } else if (mapName === 'broadcasts') {
      timestamp = value.completedAt || value.startTime;
    } else if (mapName === 'scheduledBroadcasts') {
      timestamp = value.scheduledTime; 
    } else {
      timestamp = value.timestamp || value.ts;
    }
    
    if (timestamp && (now - timestamp > rule.ttl)) {
      toDelete.push(key);
    }
  }
  
  
  toDelete.forEach(key => {
    try {
      const value = mapObj.get(key);
      
      
      if (mapName === 'broadcasts' && value.status === 'running') return;
      if (mapName === 'scheduledBroadcasts' && value.scheduledTime > now) return;
      if (mapName === 'qrTimers' && value.intervalId) {
        clearInterval(value.intervalId);
      }
      
      mapObj.delete(key);
      console.log(`[CLEANUP] ${mapName}: removed ${key}`);
    } catch (e) {
      console.error(`[CLEANUP ERROR] ${mapName}:`, e.message);
    }
  });
  
  if (toDelete.length > 0) {
    console.log(`[CLEANUP] ${mapName}: removed ${toDelete.length} items, ${mapObj.size} remaining`);
  }
};


const startCleanupSystem = () => {
  Object.entries(CLEANUP_RULES).forEach(([mapName, rule]) => {
    setInterval(() => {
      const mapObj = {
        userData,
        qrTimers,
        groupsCache,
        infoAnchors,
        renderToggle,
        broadcasts,
        scheduledBroadcasts
      }[mapName];
      
      if (mapObj && mapObj.size > 0) {
        cleanupMap(mapName, mapObj, (value) => {
          
          if (mapName === 'infoAnchors') return value.lastUsed;
          if (mapName === 'renderToggle') return value.timestamp;
          if (mapName === 'broadcasts') return value.completedAt || value.startTime;
          if (mapName === 'scheduledBroadcasts') return value.scheduledTime;
          return value.timestamp || value.ts;
        });
      }
    }, rule.checkInterval);
  });
  
  console.log('[CLEANUP] Memory cleanup system started');
};

const isValidName = (s) => /^[A-Za-z0-9][A-Za-z0-9_-]{1,63}$/.test(s);
const canonizeName = (s) => {
  let n = (s || '').trim().toLowerCase();
  
  
  const translitMap = {
    'а': 'a', 'б': 'b', 'в': 'v', 'г': 'g', 'д': 'd', 'е': 'e', 'ё': 'yo', 'ж': 'zh',
    'з': 'z', 'и': 'i', 'й': 'y', 'к': 'k', 'л': 'l', 'м': 'm', 'н': 'n', 'о': 'o',
    'п': 'p', 'р': 'r', 'с': 's', 'т': 't', 'у': 'u', 'ф': 'f', 'х': 'h', 'ц': 'ts',
    'ч': 'ch', 'ш': 'sh', 'щ': 'sch', 'ъ': '', 'ы': 'y', 'ь': '', 'э': 'e', 'ю': 'yu', 'я': 'ya'
  };
  
  n = n.replace(/[а-яё]/g, (match) => translitMap[match] || '_');
  
  
  n = n.replace(/[^a-z0-9_-]/g, '_');  
  n = n.replace(/_{2,}/g, '_');         
  n = n.replace(/^[_-]+|[_-]+$/g, '');  
  n = n.replace(/\.\./g, '_');          
  
  
  if (n.includes('../') || n.includes('..\\') || n.includes('..')) {
    n = n.replace(/\.\./g, '_');
  }
  
  
  if (!isValidName(n) || n.length < 2 || n.length > 64) {
    n = `session_${new Date().toISOString().replace(/[-:T.Z]/g,'').slice(0,14)}`;
  }
  
  return n;
};


const safeSessionPath = (name) => {
  const safeName = canonizeName(name);
  const sessionPath = path.join(SESSIONS_DIR, safeName);
  
  
  const resolvedSessionDir = path.resolve(SESSIONS_DIR);
  const resolvedSessionPath = path.resolve(sessionPath);
  
  if (!resolvedSessionPath.startsWith(resolvedSessionDir)) {
    throw new Error('Invalid session path - path traversal detected');
  }
  
  
  try {
    const realPath = fs.realpathSync(resolvedSessionPath);
    if (!realPath.startsWith(resolvedSessionDir)) {
      throw new Error('Invalid session path - symlink detected');
    }
  } catch (e) {
    
  }
  
  return { safeName, sessionPath };
};
const listSessionDirs = () =>
  fs.readdirSync(SESSIONS_DIR).filter(d => {
    try { return fs.statSync(path.join(SESSIONS_DIR, d)).isDirectory() && isValidName(d); }
    catch { return false; }
  });

const paginate = (arr, page, size) => {
  const total = arr.length, pages = Math.max(1, Math.ceil(total / size));
  const p = Math.min(Math.max(1, page), pages);
  const start = (p - 1) * size, end = Math.min(start + size, total);
  return { slice: arr.slice(start, end), page: p, pages, total };
};

const clearQrTimer = async (name, finalCaption, { deleteAfterMs, sendQrToCtx } = {}) => {
  console.log(`[QR_TIMER_CLEAR] ${name}: called with finalCaption="${finalCaption}", sendQrToCtx=${!!sendQrToCtx}`);
  
  const t = qrTimers.get(name) || qrAnchors.get(name);
  if (!t) {
    console.log(`[QR_TIMER_CLEAR] ${name}: no timer found`);
    return;
  }

  console.log(`[QR_TIMER_CLEAR] ${name}: found timer, chatId=${t.chatId}, messageId=${t.messageId}`);

  try { if (t.intervalId) clearInterval(t.intervalId); } catch {}
  

  
  if (finalCaption) {
    try { 
      await bot.telegram.editMessageCaption(t.chatId, t.messageId, undefined, finalCaption); 
      console.log(`[QR_TIMER_CLEAR] ${name}: caption updated to "${finalCaption}"`);
    } catch (e) {
      console.log(`[QR_TIMER_CLEAR] ${name}: caption update failed: ${e.message}`);
    }
  }

  
  let deleted = false;
  try {
    await bot.telegram.deleteMessage(t.chatId, t.messageId);
    deleted = true;
    console.log(`[QR_TIMER_CLEAR] ${name}: message deleted successfully`);
    
    
    if (sendQrToCtx?.telegram) {
      try {
        const sessions = listSessionDirs();
        const chatId = sendQrToCtx.chat?.id || sendQrToCtx.from?.id;
        if (sendQrToCtx.chat?.id) {
          
          if (finalCaption && finalCaption.includes('QR принят')) {
            await sendQrToCtx.telegram.sendMessage(
              chatId, 
              `✅ Сессия "${name}" успешно добавлена и готова к работе!`
            );
            console.log(`[QR_SESSION_ADDED] ${name}: success notification sent to chat ${chatId}`);
            setTimeout(() => {
              const updatedSessions = listSessionDirs();
              if (updatedSessions.length === 0) {
                sendQrToCtx.telegram.sendMessage(chatId, '❌ Нет сессий');
              } else {
                const kb = updatedSessions.map(s_name => [Markup.button.callback(`${statusEmoji(sessionStatus.get(s_name) || 'inactive')} ${s_name}`, `info_${s_name}`)]);
                kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
                sendQrToCtx.telegram.sendMessage(
                  chatId, 
                  '📋 Выберите сессию:', 
                  Markup.inlineKeyboard(kb)
                );
              }
              console.log(`[QR_MENU_AUTO_OPEN] ${name}: menu auto-opened for chat ${chatId}`);
            }, 2000);
          } else {
            if (sessions.length === 0) {
              await sendQrToCtx.telegram.sendMessage(chatId, '❌ Нет сессий');
            } else {
              const kb = sessions.map(s_name => [Markup.button.callback(`${statusEmoji(sessionStatus.get(s_name) || 'inactive')} ${s_name}`, `info_${s_name}`)]);
              kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
              await sendQrToCtx.telegram.sendMessage(
                chatId, 
                '📋 Выберите сессию:', 
                Markup.inlineKeyboard(kb)
              );
            }
          }
          console.log(`[QR_DELETE_SUCCESS] ${name}: session menu sent to chat ${chatId}`);
        }
      } catch (e) {
        console.error('[QR_DELETE_NOTIFICATION_ERROR]', { name, error: e.message });
      }
    } else {
      console.log(`[QR_TIMER_CLEAR] ${name}: no sendQrToCtx provided`);
      if (finalCaption && finalCaption.includes('QR принят')) {
        const allUsers = getAllUsers();
        for (const user of allUsers) {
          try {
            await bot.telegram.sendMessage(
              user.id, 
              `✅ Сессия "${name}" успешно добавлена и готова к работе!`
            );
            console.log(`[QR_SESSION_ADDED_BROADCAST] ${name}: notification sent to user ${user.id}`);
            setTimeout(() => {
              const updatedSessions = listSessionDirs();
              if (updatedSessions.length === 0) {
                bot.telegram.sendMessage(user.id, '❌ Нет сессий');
              } else {
                const kb = updatedSessions.map(s_name => [Markup.button.callback(`${statusEmoji(sessionStatus.get(s_name) || 'inactive')} ${s_name}`, `info_${s_name}`)]);
                kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
                bot.telegram.sendMessage(
                  user.id, 
                  '📋 Выберите сессию:', 
                  Markup.inlineKeyboard(kb)
                );
              }
              console.log(`[QR_MENU_AUTO_OPEN_BROADCAST] ${name}: menu auto-opened for user ${user.id}`);
            }, 2000);
            
break;
          } catch (e) {
            console.log(`[QR_SESSION_ADDED_BROADCAST] ${name}: failed to notify user ${user.id}: ${e.message}`);
          }
        }
      }
    }
  } catch (e) {
    console.log(`[QR_TIMER_CLEAR] ${name}: message deletion failed: ${e.message}`);
    
    if (typeof deleteAfterMs === 'number') {
      setTimeout(() => {
        bot.telegram.deleteMessage(t.chatId, t.messageId).catch(() => {});
      }, deleteAfterMs);
    }
  }

  
  qrTimers.delete(name);
  qrAnchors.delete(name);

  console.log(`[QR_TIMER_CLEAR] ${name}: cleanup completed, deleted=${deleted}`);
};

const renderInfoText = (name) =>
  `${statusEmoji(sessionStatus.get(name) || 'inactive')} Сессия: ${name}\n📊 Статус: ${sessionHuman(sessionStatus.get(name) || 'inactive')}`;

const updateInfoCard = async (name) => {
  const anchor = infoAnchors.get(name);
  if (!anchor) return;
  try {
    await bot.telegram.editMessageText(
      anchor.chatId, anchor.messageId, undefined,
      renderInfoText(name),
      { ...sessionKeyboard(name, sessionStatus.get(name) || 'inactive') }
    );
    
    anchor.lastUsed = Date.now();
  } catch (e) {
    
    if (!e.message.includes('message is not modified')) {
      console.error('[INFO_CARD_UPDATE_ERROR]', { name, error: e.message, chatId: anchor.chatId });
    }
  }
};

const setStatus = (name, s) => {
  sessionStatus.set(name, s);
  console.log(`[STATUS] ${name}: ${s}`);
  updateInfoCard(name).catch((e) => {
    console.error('[STATUS_UPDATE_ERROR]', { name, status: s, error: e.message });
  });
};


const cbList    = (name, page) => `grp:list:${name}:${page}`;
const cbRefresh = (name, page) => `grp:refresh:${name}:${page}`;
const cbNoop    = 'noop';


const connectSocket = async (name, opts = {}) => {
  const { safeName, sessionPath } = safeSessionPath(name);
  const existedBefore = fs.existsSync(sessionPath);
  if (!existedBefore && !opts.createIfMissing) throw new Error('Папка сессии не существует');
  if (!existedBefore && opts.createIfMissing) fs.mkdirSync(sessionPath, { recursive: true });

  if (activeSocks.has(name)) {
    try { 
      const sock = activeSocks.get(name);
      sock.end(); 
      console.log(`[SOCKET_CLOSED] ${name}: existing socket closed`);
    } catch (e) {
      console.error('[SOCKET_CLOSE_ERROR]', { name, error: e.message });
    }
    activeSocks.delete(name);
  }

  const { state: authState, saveCreds } = await useMultiFileAuthState(sessionPath);
  if (!sessionStatus.get(name)) setStatus(name, 'syncing');

  
  const handleSuccessfulConnection = async (sessionName, options) => {
  if (connectionHandled) return;

  connectionHandled = true;
  credsUpdateHandled = true;
  setStatus(sessionName, 'active');
  
  const sendSessionMenu = async (ctx, userId) => {
    try {
      const sessions = listSessionDirs();
      if (sessions.length === 0) {
        await ctx.telegram.sendMessage(userId, '❌ Нет сессий');
      } else {
        const kb = sessions.map(s_name => [Markup.button.callback(`${statusEmoji(sessionStatus.get(s_name) || 'inactive')} ${s_name}`, `info_${s_name}`)]);
        kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
        await ctx.telegram.sendMessage(
          userId, 
          '📋 Выберите сессию:', 
          Markup.inlineKeyboard(kb)
        );
      }
      console.log(`[SESSION_MENU_AUTO_OPEN] ${sessionName}: menu sent to user ${userId}`);
    } catch (e) {
      console.error('[SESSION_MENU_ERROR]', { sessionName, userId, error: e.message });
    }
  };
  
  if (options.sendQrToCtx?.reply) {
    await options.sendQrToCtx.reply(`🟢 Сессия "${sessionName}" активна!`);
    
    
    const chatId = options.sendQrToCtx.chat?.id || options.sendQrToCtx.from?.id;
    if (chatId) {
      setTimeout(() => {
        sendSessionMenu(bot, chatId);
      }, 2000);
    }
  } else {
    console.error('[NOTIFY_SKIP] ctx undefined');
    const allUsers = getAllUsers();
    for (const user of allUsers) {
      try {
        await bot.telegram.sendMessage(
          user.id, 
          `🟢 Сессия "${sessionName}" активна и готова к работе!`
        );
        console.log(`[SESSION_ACTIVATED_BROADCAST] ${sessionName}: notification sent to user ${user.id}`);
        

        setTimeout(() => {
          sendSessionMenu(bot, user.id);
        }, 2000);
        
        break;
      } catch (e) {
        console.log(`[SESSION_ACTIVATED_BROADCAST] ${sessionName}: failed to notify user ${user.id}: ${e.message}`);
      }
    }
  }

  
  try { await clearQrTimer(sessionName, undefined, { sendQrToCtx: options.sendQrToCtx }); } catch {}

  
};

  const sock = makeWASocket({
    auth: authState,
    printQRInTerminal: false,
    connectTimeoutMs: 60_000,
    version: [2, 3000, 1030285482], // Recommended WhatsApp Web version
    logger: pino({ level: 'silent' }), // Add logger for debugging
  });
  activeSocks.set(name, sock);

  // Handle credentials update
  sock.ev.on('creds.update', saveCreds);

  let connectionHandled = false;
  let credsUpdateHandled = false;

  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    console.log(`[CONNECTION_UPDATE] ${name}: connection=${connection}, handled=${connectionHandled}, qr=${!!qr}, lastDisconnect=${!!lastDisconnect}`);

    if (qr && opts.sendQrToCtx) {
      await clearQrTimer(name);
      setStatus(name, 'qr');
      if (opts.sendQrToCtx?.replyWithPhoto) {
        try {
          const png = await qrcode.toBuffer(qr, { width: 512 });
          const timeoutSec = Math.max(10, opts.qrTimeoutSec ?? 60);
          const sent = await opts.sendQrToCtx.replyWithPhoto({ source: png }, { caption: `📱 Сканируйте QR (осталось ${timeoutSec} c)` });

          const startedAt = Date.now();
          const intervalId = setInterval(async () => {
            const left = timeoutSec - Math.floor((Date.now() - startedAt) / 1000);
            if (left > 0) {
              try { await bot.telegram.editMessageCaption(sent.chat.id, sent.message_id, undefined, `📱 Сканируйте QR (осталось ${left} c)`); } catch {}
            } else {
              clearInterval(intervalId);
              qrTimers.delete(name);
              qrAnchors.delete(name); 
              try { await bot.telegram.editMessageCaption(sent.chat.id, sent.message_id, undefined, '⏰ Время вышло'); } catch {}
              try { sock.end(); } catch {}
              activeSocks.delete(name);
              if (!existedBefore) { 
    try { 
      fs.rmSync(sessionPath, { recursive: true, force: true }); 
      console.log(`[SESSION_CLEANUP] ${name}: removed failed session files`);
    } catch (e) {
      console.error('[SESSION_CLEANUP_ERROR]', { name, error: e.message });
    } 
  }
              setStatus(name, 'inactive');
            }
          }, 1000);

          

          qrTimers.set(name, { intervalId, chatId: sent.chat.id, messageId: sent.message_id, startedAt, timeoutSec });
          
          qrAnchors.set(name, { chatId: sent.chat.id, messageId: sent.message_id });
        } catch (e) { console.error(`[QR ERROR] ${name}:`, e?.message || e); }
      } else {
        console.error('[NOTIFY_SKIP] ctx replyWithPhoto undefined');
      }
    }

    if (connection === 'open' && !connectionHandled) {
      console.log(`[CONNECTION_OPEN] ${name}: handling connection open`);
      
      await clearQrTimer(name, '🔄 QR принят, выполняется синхронизация…', { deleteAfterMs: 800, sendQrToCtx: opts.sendQrToCtx });
      await handleSuccessfulConnection(name, opts);
    }

    if (connection === 'close') {
      const err  = lastDisconnect?.error;
      const code = err?.output?.statusCode ?? err?.status ?? err?.code ?? err?.data?.statusCode ?? null;
      const loggedOut = code === DisconnectReason.loggedOut;

      activeSocks.delete(name);
      await clearQrTimer(name);
      connectionHandled = false;
      credsUpdateHandled = false;

      if (loggedOut) {
        setStatus(name, 'inactive');
      } else {
        setStatus(name, 'syncing');
        setTimeout(() => {
          connectSocket(name).catch(e => { setStatus(name, 'error'); console.error('[RECONNECT]', e?.message || e); });
        }, 5000);
      }
    }
  });



  return sock;
};


const loadExistingSessions = async () => {
  const sessions = listSessionDirs();
  for (const name of sessions) {
    try { setStatus(name, 'syncing'); await connectSocket(name, { createIfMissing: false }); }
    catch (e) { setStatus(name, 'error'); console.log(`Failed to load ${name}:`, e.message); }
  }
};
await loadExistingSessions();

startCleanupSystem();
const sendMainMenu = (ctx) => ctx.replyWithPhoto(
  { source: './art.jpg' },
  {
    caption: 'Выберите действие:',
    ...Markup.inlineKeyboard([
      [Markup.button.callback('📋 Выбрать сессию', 'select')],
      [Markup.button.callback('➕ Добавить сессию', 'add')],
      [Markup.button.callback('📢 Рассылка', 'broadcast')],
      [Markup.button.callback('👥 Пользователи', 'users')],
      [Markup.button.callback('🗑️ Удалить сессию', 'del')],
    ])
  }
);

bot.start((ctx) => {
  addUser(ctx.from.id, {
    username: ctx.from.username,
    firstName: ctx.from.first_name,
    lastName: ctx.from.last_name
  });
  
  sendMainMenu(ctx);
});
bot.action('start', async (ctx) => { 
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[START_CB_ERROR]', { error: e.message });
  } 
  return sendMainMenu(ctx); 
});

bot.command('users', async (ctx) => {
  const allUsers = getAllUsers();
  
  if (allUsers.length === 0) {
    return ctx.reply(
      '👥 ПОЛЬЗОВАТЕЛИ БОТА\n\n' +
      'Пока нет сохраненных пользователей.',
      Markup.inlineKeyboard([
        [Markup.button.callback('⬅️ В меню', 'start')]
      ])
    );
  }
  
  let text = `👥 ПОЛЬЗОВАТЕЛИ БОТА (${allUsers.length} всего)\n\n`;
  
  allUsers.forEach((user, index) => {
    const name = user.firstName || user.username || `User${user.id}`;
    const lastSeen = user.lastSeen ? new Date(user.lastSeen).toLocaleString('ru-RU') : 'Неизвестно';
    let statusIcon = '🔴';
    let statusText = 'Не в сети';
    
    if (user.status === 'broadcasting') {
      statusIcon = '📤';
      statusText = 'Делает рассылку';
      if (user.currentActivity) {
        statusText += `: ${user.currentActivity}`;
      }
    } else if (user.status === 'setup') {
      statusIcon = '⚙️';
      statusText = 'Настраивает рассылку';
      if (user.currentActivity) {
        statusText += `: ${user.currentActivity}`;
      }
    } else if (user.status === 'idle') {
      statusIcon = '🟡';
      statusText = 'Бездействует';
    } else if (user.lastSeen && (Date.now() - user.lastSeen < 5 * 60 * 1000)) {
      statusIcon = '🟢';
      statusText = 'Онлайн';
    }
    
    let activityTime = '';
    if (user.activityStartTime && user.status !== 'offline') {
      const duration = Math.floor((Date.now() - user.activityStartTime) / 1000);
      const minutes = Math.floor(duration / 60);
      const seconds = duration % 60;
      activityTime = ` (${minutes}м ${seconds}с)`;
    }
    
    text += `${index + 1}. ${name} (@${user.username || 'no_username'})\n`;
    text += `   🆔 ID: ${user.id}\n`;
    text += `   ${statusIcon} Статус: ${statusText}${activityTime}\n`;
    text += `   🕐 Последний раз: ${lastSeen}\n\n`;
  });
  
  await ctx.reply(text, Markup.inlineKeyboard([
    [Markup.button.callback('⬅️ В меню', 'start')]
  ]));
});

bot.command('notifyall', async (ctx) => {
  
  const adminIds = process.env.ADMIN_IDS ? process.env.ADMIN_IDS.split(',').map(id => parseInt(id.trim())) : [];
  
  if (adminIds.length > 0 && !adminIds.includes(ctx.from.id)) {
    return ctx.reply('❌ У вас нет прав для выполнения этой команды');
  }
  
  const messageText = ctx.message.text.replace('/notifyall', '').trim();
  
  if (!messageText) {
    return ctx.reply(
      '📢 РАССЫЛКА ВСЕМ ПОЛЬЗОВАТЕЛЯМ\n\n' +
      'Использование: /notifyall Ваше сообщение\n\n' +
      'Пример: /notifyall Бот работает в штатном режиме!',
      Markup.inlineKeyboard([
        [Markup.button.callback('⬅️ В меню', 'start')]
      ])
    );
  }
  
  await ctx.reply('📤 Начинаю рассылку всем пользователям...');
  
  const result = await notifyAllUsers(messageText, bot);
  
  await ctx.reply(
    `✅ Рассылка завершена\n\n` +
    `📊 Статистика:\n` +
    `✅ Успешно отправлено: ${result.successCount}\n` +
    `❌ Ошибок: ${result.errorCount}\n` +
    `📝 Сообщение: "${messageText}"`,
    Markup.inlineKeyboard([
      [Markup.button.callback('⬅️ В меню', 'start')]
    ])
  );
});

bot.command('scheduled', async (ctx) => {
  const scheduledList = Array.from(scheduledBroadcasts.entries());
  
  if (scheduledList.length === 0) {
    return ctx.reply(
      '📋 ЗАПЛАНИРОВАННЫЕ РАССЫЛКИ\n\n' +
      'У вас нет запланированных рассылок.',
      Markup.inlineKeyboard([
        [Markup.button.callback('⬅️ В меню', 'start')]
      ])
    );
  }
  
  let text = '📋 ЗАПЛАНИРОВАННЫЕ РАССЫЛКИ\n\n';
  const keyboard = [];
  
  scheduledList.forEach(([scheduleId, scheduled]) => {
    const { data, scheduledTime } = scheduled;
    const remaining = formatTimeRemaining(scheduledTime - Date.now());
    
    text += `🤖 ${data.sessionName}\n`;
    text += `📅 ${new Date(scheduledTime).toLocaleString('ru-RU')}\n`;
    text += `⏳ Осталось: ${remaining}\n`;
    text += `📝 ${data.messageText ? data.messageText.substring(0, 30) + '...' : 'Медиа'}\n\n`;
    
    keyboard.push([Markup.button.callback('❌ Отменить', `scheduled_cancel_${scheduleId}`)]);
  });
  
  keyboard.push([Markup.button.callback('⬅️ В меню', 'start')]);
  
  await ctx.reply(text, Markup.inlineKeyboard(keyboard));
});

bot.action('scheduled_list', async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  ctx.message = { text: '/scheduled' };
  bot.emit('text', ctx);
});

bot.action(/scheduled_cancel_(.+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const scheduleId = ctx.match[1];
  const scheduled = scheduledBroadcasts.get(scheduleId);
  
  if (!scheduled) {
    return ctx.reply('❌ Рассылка не найдена');
  }
  
  clearTimeout(scheduled.timerId);
  scheduledBroadcasts.delete(scheduleId);
  
  await ctx.editMessageText(
    '✅ Запланированная рассылка отменена\n\n' +
    `🤖 Сессия: ${scheduled.data.sessionName}\n` +
    `📅 Время: ${new Date(scheduled.scheduledTime).toLocaleString('ru-RU')}`,
    Markup.inlineKeyboard([
      [Markup.button.callback('⬅️ В меню', 'start')]
    ])
  );
});

bot.action('add', async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[ADD_CB_ERROR]', { error: e.message });
  }
  await ctx.reply('📝 Введите название сессии:');
  userData.set(ctx.from.id, { action: 'add_name', timestamp: Date.now() });
});

bot.action('select', async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[SELECT_CB_ERROR]', { error: e.message });
  }
  const sessions = listSessionDirs();
  if (sessions.length === 0) return ctx.reply('❌ Нет сессий');
  const kb = sessions.map(name => [Markup.button.callback(`${statusEmoji(sessionStatus.get(name) || 'inactive')} ${name}`, `info_${name}`)]);
  kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
  await ctx.reply('📋 Выберите сессию:', Markup.inlineKeyboard(kb));
});

bot.action('del', async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[DELETE_CB_ERROR]', { error: e.message });
  }
  const sessions = listSessionDirs();
  if (sessions.length === 0) return ctx.reply('❌ Нет сессий');
  const kb = sessions.map(name => [Markup.button.callback(`🗑️ Удалить ${name}`, `del_${name}`)]);
  kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
  await ctx.reply('🗑️ Выберите для удаления:', Markup.inlineKeyboard(kb));
});

bot.action('users', async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[USERS_CB_ERROR]', { error: e.message });
  }
  
  const allUsers = getAllUsers();
  
  if (allUsers.length === 0) {
    return ctx.reply(
      '👥 ПОЛЬЗОВАТЕЛИ БОТА\n\n' +
      'Пока нет сохраненных пользователей.',
      Markup.inlineKeyboard([
        [Markup.button.callback('⬅️ В меню', 'start')]
      ])
    );
  }
  
  let text = `👥 ПОЛЬЗОВАТЕЛИ БОТА (${allUsers.length} всего)\n\n`;
  
  allUsers.slice(0, 10).forEach((user, index) => {
    const name = user.firstName || user.username || `User${user.id}`;
    const lastSeen = user.lastSeen ? new Date(user.lastSeen).toLocaleString('ru-RU') : 'Неизвестно';
    
    let statusIcon = '🔴';
    let statusText = 'Не в сети';
    
    if (user.status === 'broadcasting') {
      statusIcon = '📤';
      statusText = 'Делает рассылку';
      if (user.currentActivity) {
        statusText += `: ${user.currentActivity}`;
      }
    } else if (user.status === 'setup') {
      statusIcon = '⚙️';
      statusText = 'Настраивает рассылку';
      if (user.currentActivity) {
        statusText += `: ${user.currentActivity}`;
      }
    } else if (user.status === 'idle') {
      statusIcon = '🟡';
      statusText = 'Бездействует';
    } else if (user.lastSeen && (Date.now() - user.lastSeen < 5 * 60 * 1000)) {
      statusIcon = '🟢';
      statusText = 'Онлайн';
    }
    
    let activityTime = '';
    if (user.activityStartTime && user.status !== 'offline') {
      const duration = Math.floor((Date.now() - user.activityStartTime) / 1000);
      const minutes = Math.floor(duration / 60);
      const seconds = duration % 60;
      activityTime = ` (${minutes}м ${seconds}с)`;
    }
    
    text += `${index + 1}. ${name} (@${user.username || 'no_username'})\n`;
    text += `   ${statusIcon} Статус: ${statusText}${activityTime}\n`;
    text += `   🕐 Последний раз: ${lastSeen}\n\n`;
  });
  
  if (allUsers.length > 10) {
    text += `... и еще ${allUsers.length - 10} пользователей\n\n`;
  }
  
  text += `💡 Полный список: /users`;
  
  await ctx.reply(text, Markup.inlineKeyboard([
    [Markup.button.callback('⬅️ В меню', 'start')]
  ]));
});



bot.action('broadcast', async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[BROADCAST_CB_ERROR]', { error: e.message });
  }
  const sessions = listSessionDirs();
  const activeSessions = sessions.filter(name => sessionStatus.get(name) === 'active');
  
  if (activeSessions.length === 0) {
    return ctx.reply('❌ Нет активных сессий для рассылки', Markup.inlineKeyboard([
      [Markup.button.callback('⬅️ Назад', 'start')]
    ]));
  }
  
  const kb = activeSessions.map(name => [Markup.button.callback(`${statusEmoji('active')} ${name}`, `broadcast_session_${name}`)]);
  kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
  
  await ctx.reply('📢 Выберите сессию для рассылки:', Markup.inlineKeyboard(kb));
});


const sessionKeyboard = (name, st) => {
  const rows = [];
  if (st !== 'active' && st !== 'syncing') rows.push([Markup.button.callback('🔄 Активировать', `activate_${name}`)]);
  rows.push([Markup.button.callback('👥 Просмотреть группы', cbList(name, 1))]);
  rows.push([Markup.button.callback('⬅️ Назад', 'select')]);
  return Markup.inlineKeyboard(rows);
};

bot.action(/info_(.+)/, async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[INFO_CB_ERROR]', { name: ctx.match[1], error: e.message });
  }
  const name = ctx.match[1];
  const msg = await ctx.editMessageText(renderInfoText(name), { ...sessionKeyboard(name, sessionStatus.get(name) || 'inactive') });
  const chatId = ctx.chat?.id ?? ctx.callbackQuery?.message?.chat?.id;
  const messageId = (ctx.callbackQuery?.message?.message_id) ?? msg?.message_id;
  if (chatId && messageId) infoAnchors.set(name, { chatId, messageId, lastUsed: Date.now() });
});


bot.action(/activate_(.+)/, async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[ACTIVATE_CB_ERROR]', { name: ctx.match[1], error: e.message });
  }
  const name = ctx.match[1];
  let sessionPath;
  try {
    const { safeName, sessionPath: sp } = safeSessionPath(name);
    sessionPath = sp;
  } catch (e) {
    return ctx.editMessageText(`❌ Некорректное имя сессии`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'select')]]) });
  }
  if (!fs.existsSync(sessionPath)) return ctx.editMessageText(`❌ Сессии «${name}» не существует`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'select')]]) });

  setStatus(name, 'syncing');
  await ctx.editMessageText(`🔄 Активация сессии ${name}…`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]]) });

  try {
    await connectSocket(name, { sendQrToCtx: ctx, qrTimeoutSec: QR_TIMEOUT_ACTIVATE, createIfMissing: false });
  } catch (err) {
    setStatus(name, 'error');
    await ctx.editMessageText(`❌ Ошибка активации: ${err.message}`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]]) });
  }
});


bot.action(/del_(.+)/, async (ctx) => {
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[DELETE_SESSION_CB_ERROR]', { name: ctx.match[1], error: e.message });
  }
  const name = ctx.match[1];
  let sessionPath;
  try {
    const { safeName, sessionPath: sp } = safeSessionPath(name);
    sessionPath = sp;
  } catch (e) {
    return ctx.editMessageText(`❌ Некорректное имя сессии`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'select')]]) });
  }

if (activeSocks.has(name)) { 
  try { 
    const sock = activeSocks.get(name);
    await ctx.editMessageText(`🔄 Выходим из устройства сессии «${name}»…`);
    
    try {
      await sock.logout();
      console.log(`[LOGOUT_SUCCESS] ${name}: successfully logged out`);
    } catch (logoutError) {
      console.log(`[LOGOUT_FAILED] ${name}: ${logoutError.message}`);
    }
    
    sock.end(); 
    console.log(`[SOCKET_CLOSED] ${name}: socket closed during deletion`);
  } catch (e) {
    console.error('[SOCKET_CLOSE_ERROR]', { name, error: e.message });
  }
  activeSocks.delete(name);
}
  await clearQrTimer(name);
  sessionStatus.delete(name);
  groupsCache.delete(name);
  infoAnchors.delete(name);
  try { 
  fs.rmSync(sessionPath, { recursive: true, force: true }); 
  console.log(`[SESSION_DELETED] ${name}: session files removed`);
} catch (e) {
  console.error('[SESSION_DELETE_ERROR]', { name, error: e.message });
}

  await ctx.editMessageText(`✅ Удалена сессия: ${name}`);
});

bot.action(/broadcast_session_(.+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const name = ctx.match[1];
  
  const msg = await ctx.editMessageText(
    `📢 Настройка рассылки для сессии «${name}»\n\n` +
    `📎 Сначала отправьте медиафайл (фото/видео/документ) если нужен пост с медиа,\n` +
    `или отправьте текст если нужен только текстовый пост.\n\n` +
    `💡 Порядок не важен - сможете добавить текст после медиа или медиа после текста\n` +
    `⏱️ Затем настроим интервалы между отправками`,
    { ...Markup.inlineKeyboard([
      [Markup.button.callback('⬅️ Отмена', 'start')]
    ])}
  );
  
  userData.set(ctx.from.id, { 
    action: 'broadcast_setup', 
    data: { sessionName: name, setupMessageId: msg.message_id },
    timestamp: Date.now()
  });
});


bot.on('text', async (ctx) => {
  
  addUser(ctx.from.id, {
    username: ctx.from.username,
    firstName: ctx.from.first_name,
    lastName: ctx.from.last_name
  });
  
  const state = userData.get(ctx.from.id);
  if (!state) return;
  
  
  if (state.timestamp && (Date.now() - state.timestamp > CLEANUP_RULES.userData.ttl)) {
    userData.delete(ctx.from.id);
    return;
  }
  
if (state.action === 'add_name') {
      const name = canonizeName(ctx.message.text);
      const { sessionPath } = safeSessionPath(name);
      if (fs.existsSync(sessionPath)) return ctx.reply('❌ Такая сессия уже существует. Введите другое имя.');

    userData.delete(ctx.from.id);
    await ctx.reply(`🆕 Создаю сессию «${name}»…`);
    setStatus(name, 'syncing');

    try {
      await connectSocket(name, { sendQrToCtx: ctx, qrTimeoutSec: QR_TIMEOUT_CREATE, createIfMissing: true });
    } catch (err) {
      setStatus(name, 'error');
      await ctx.reply('❌ Ошибка: ' + err.message);
    }
  } else if (state.action === 'broadcast_setup') {
    
    if (state.data.action === 'waiting_custom_interval') {
      const intervalText = ctx.message.text.trim();
      const interval = parseCustomInterval(intervalText);
      
      if (!interval) {
        return ctx.reply(
          `❌ Неверный формат интервала!\n\n` +
          `Правильные форматы:\n` +
          `• 10-30 (от 10 до 30 секунд)\n` +
          `• 5 (фиксированные 5 секунд)\n` +
          `• 120-300сек (от 2 до 5 минут)\n\n` +
          `Попробуйте еще раз:`,
          Markup.inlineKeyboard([
            [Markup.button.callback('⬅️ Назад', 'broadcast_back_to_intervals')],
            [Markup.button.callback('❌ Отмена', 'start')]
          ])
        );
      }
      
      state.data.minInterval = interval.min;
      state.data.maxInterval = interval.max;
      delete state.data.action;
      state.timestamp = Date.now(); 
      
      await startBroadcast(ctx, state.data);
    } else if (state.data.action === 'waiting_schedule_time') {
      const timeText = ctx.message.text.trim();
      const scheduledTime = parseScheduledTime(timeText);
      
      if (!scheduledTime) {
        return ctx.reply(
          `❌ Неверный формат времени!\n\n` +
          `Правильные форматы:\n` +
          `• ДД.ММ.ГГГГ ЧЧ:ММ (например: 25.12.2024 15:30)\n` +
          `• сегодня ЧЧ:ММ (например: сегодня 18:00)\n` +
          `• завтра ЧЧ:ММ (например: завтра 09:00)\n` +
          `• через 2 часа\n` +
          `• через 30 минут\n\n` +
          `Попробуйте еще раз:`,
          Markup.inlineKeyboard([
            [Markup.button.callback('⬅️ Назад', 'start')],
            [Markup.button.callback('❌ Отмена', 'start')]
          ])
        );
      }
      
      
      const now = Date.now();
      if (scheduledTime <= now) {
        return ctx.reply(
          `❌ Указанное время уже прошло!\n\n` +
          `Выберите время в будущем.`,
          Markup.inlineKeyboard([
            [Markup.button.callback('⬅️ Назад', 'start')],
            [Markup.button.callback('❌ Отмена', 'start')]
          ])
        );
      }
      
      
      const scheduleId = `sched_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`;
      const delay = scheduledTime - now;
      
      const timerId = setTimeout(async () => {
        await executeScheduledBroadcast(scheduleId, ctx);
      }, delay);
      
      scheduledBroadcasts.set(scheduleId, {
        data: state.data,
        scheduledTime: scheduledTime,
        timerId: timerId,
        userId: ctx.from.id,
        timestamp: Date.now()
      });
      
      userData.delete(ctx.from.id);
      
      await ctx.reply(
        `✅ РАССЫЛКА ЗАПЛАНИРОВАНА!\n\n` +
        `🤖 Сессия: ${state.data.sessionName}\n` +
        `📅 Дата запуска: ${new Date(scheduledTime).toLocaleString('ru-RU')}\n` +
        `⏱️ Осталось времени: ${formatTimeRemaining(delay)}\n\n` +
        `📋 Все запланированные рассылки: /scheduled`,
        Markup.inlineKeyboard([
          [Markup.button.callback('📋 Мои рассылки', 'scheduled_list')],
          [Markup.button.callback('⬅️ В меню', 'start')]
        ])
      );
      
    } else {
      
      if (!state.data.messageText) {
        state.data.messageText = ctx.message.text;
        state.timestamp = Date.now(); 
        
        
        if (state.data.setupMessageId) {
          try {
            await ctx.deleteMessage(state.data.setupMessageId);
          } catch {}
        }
        
        
        await showIntervalSetup(ctx, state.data);
      } else {
        
        state.data.messageText = ctx.message.text;
        state.timestamp = Date.now(); 
        
        await ctx.reply(
          `✅ Текст обновлен!\n\n` +
          `📝 Текст: ${state.data.messageText.substring(0, 50)}${state.data.messageText.length > 50 ? '...' : ''}\n` +
          `📎 Медиа: ${state.data.mediaType ? 'Добавлено' : 'Нет'}\n\n` +
          `Нажмите "✅ Готово" когда все готово.`,
          Markup.inlineKeyboard([
            [Markup.button.callback('✅ Готово', 'broadcast_ready')],
            [Markup.button.callback('❌ Отмена', 'start')]
          ])
        );
      }
    }
  }
});

function parseCustomInterval(text) {
  const cleanText = text.toLowerCase().replace(/\s+/g, '');
  
  
  const rangeMatch = cleanText.match(/^(\d+)-(\d+)(сек)?$/);
  if (rangeMatch) {
    let min = parseInt(rangeMatch[1]);
    let max = parseInt(rangeMatch[2]);
    
    if (min >= 3 && max >= min && max <= 3600) {
      return { min, max };
    }
  }
  
  
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
  const fullDateMatch = cleanText.match(/^(\d{2})\.(\d{2})\.(\d{4})\s+(\d{2}):(\d{2})$/);
  if (fullDateMatch) {
    const [, day, month, year, hours, minutes] = fullDateMatch;
    const date = new Date(`${year}-${month}-${day}T${hours}:${minutes}:00`);
    return date.getTime();
  }
  
  
  const todayMatch = cleanText.match(/^сегодня\s+(\d{2}):(\d{2})$/);
  if (todayMatch) {
    const [, hours, minutes] = todayMatch;
    const date = new Date();
    date.setHours(parseInt(hours), parseInt(minutes), 0, 0);
    return date.getTime();
  }
  
  
  const tomorrowMatch = cleanText.match(/^завтра\s+(\d{2}):(\d{2})$/);
  if (tomorrowMatch) {
    const [, hours, minutes] = tomorrowMatch;
    const date = new Date();
    date.setDate(date.getDate() + 1);
    date.setHours(parseInt(hours), parseInt(minutes), 0, 0);
    return date.getTime();
  }
  
  
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

async function executeScheduledBroadcast(scheduleId, originalCtx) {
  const scheduled = scheduledBroadcasts.get(scheduleId);
  if (!scheduled) return;
  
  try {
    const { data } = scheduled;
    const sock = activeSocks.get(data.sessionName);
    
    if (!sock) {
      try {
        await originalCtx.telegram.sendMessage(
          scheduled.userId,
          `❌ Запланированная рассылка отменена\n\n` +
          `🤖 Сессия «${data.sessionName}» неактивна\n` +
          `📅 Время: ${new Date(scheduled.scheduledTime).toLocaleString('ru-RU')}`
        );
      } catch {}
      scheduledBroadcasts.delete(scheduleId);
      return;
    }
    
    const groups = await fetchGroups(data.sessionName);
    if (groups.length === 0) {
      try {
        await originalCtx.telegram.sendMessage(
          scheduled.userId,
          `❌ Запланированная рассылка отменена\n\n` +
          `🤖 Сессия «${data.sessionName}» не имеет групп\n` +
          `📅 Время: ${new Date(scheduled.scheduledTime).toLocaleString('ru-RU')}`
        );
      } catch {}
      scheduledBroadcasts.delete(scheduleId);
      return;
    }
    
    const broadcastId = Date.now().toString();
    broadcasts.set(broadcastId, {
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
      userId: scheduled.userId 
    });
    
    try {
      updateUserStatus(scheduled.userId, 'broadcasting', `${data.sessionName} (${groups.length} групп)`);
      
      const msg = await originalCtx.telegram.sendMessage(
        scheduled.userId,
        `🚀 ЗАПЛАНИРОВАННАЯ РАССЫЛКА ЗАПУЩЕНА!\n\n` +
        `🤖 Сессия: ${data.sessionName}\n` +
        `👥 Групп: ${groups.length}\n` +
        `⏱️ Интервал: ${data.minInterval}-${data.maxInterval} сек.\n\n` +
        `📈 Прогресс будет обновляться автоматически...`,
        { 
          reply_markup: {
            inline_keyboard: [
              [{ text: '⏸️ Остановить', callback_data: `broadcast_stop_${broadcastId}` }],
              [{ text: '📊 Статистика', callback_data: `broadcast_stats_${broadcastId}` }]
            ]
          }
        }
      );
      
      const ctx = { telegram: originalCtx.telegram, chat: { id: scheduled.userId } };
      if (msg) {
        ctx.callbackQuery = { message: { message_id: msg.message_id, chat: { id: scheduled.userId } } };
      }
      
      runBroadcast(broadcastId, ctx);
      
    } catch (error) {
      console.error('[SCHEDULED BROADCAST ERROR]', error);
      updateUserStatus(scheduled.userId, 'idle');
    }
    
  } catch (error) {
    console.error('[SCHEDULED EXECUTION ERROR]', { scheduleId, error: error.message });
  }
}


bot.on(['photo', 'video', 'document'], async (ctx) => {
  
  addUser(ctx.from.id, {
    username: ctx.from.username,
    firstName: ctx.from.first_name,
    lastName: ctx.from.last_name
  });
  
  const state = userData.get(ctx.from.id);
  if (!state || (state.action !== 'broadcast_setup' && state.action !== 'broadcast_waiting_media')) return;
  
  try {
    await ctx.reply('📥 Скачиваю медиафайл...');
    
    let mediaType, mediaInfo, fileLink;
    const caption = ctx.message.caption || '';
    
    if (ctx.message.photo) {
      mediaType = 'image';
      const photo = ctx.message.photo[ctx.message.photo.length - 1];
      fileLink = await ctx.telegram.getFileLink(photo.file_id);
      mediaInfo = {
        url: fileLink.href, 
        mimeType: 'image/jpeg'
      };
    } else if (ctx.message.video) {
      mediaType = 'video';
      fileLink = await ctx.telegram.getFileLink(ctx.message.video.file_id);
      mediaInfo = {
        url: fileLink.href,
        mimeType: ctx.message.video.mime_type
      };
    } else if (ctx.message.document) {
      mediaType = 'document';
      fileLink = await ctx.telegram.getFileLink(ctx.message.document.file_id);
      mediaInfo = {
        url: fileLink.href,
        fileName: ctx.message.document.file_name,
        mimeType: ctx.message.document.mime_type
      };
    }
    
    state.data.mediaType = mediaType;
    state.data.mediaInfo = mediaInfo;
    
    
    if (caption && !state.data.messageText) {
      state.data.messageText = caption;
    }
    
    await ctx.reply(
      `✅ Медиа для поста сохранено!\n\n` +
      `📊 Тип: ${mediaType}\n` +
      `📝 Текст: ${state.data.messageText ? state.data.messageText.substring(0, 50) + (state.data.messageText.length > 50 ? '...' : '') : 'Еще не добавлен'}\n\n` +
      `💡 ${caption ? 'Текст из подписи добавлен!' : 'Теперь отправьте текст для поста или нажмите "✅ Готово" если текст не нужен.'}`,
      Markup.inlineKeyboard([
        [Markup.button.callback('✅ Готово', 'broadcast_ready')],
        [Markup.button.callback('❌ Отмена', 'start')]
      ])
    );
    
  } catch (error) {
    console.error('[MEDIA ERROR]', error);
    await ctx.reply(
      `❌ Ошибка обработки медиафайла: ${error.message}\n\n` +
      `Попробуйте другой файл или отправьте только текст.`,
      Markup.inlineKeyboard([
        [Markup.button.callback('⏭️ Пропустить медиа', 'broadcast_skip_media')],
        [Markup.button.callback('❌ Отмена', 'start')]
      ])
    );
  }
});


async function fetchGroups(name) {
  const cached = groupsCache.get(name);
  if (cached && (Date.now() - cached.ts) < GROUPS_CACHE_TTL) return cached.list;

  const sock = activeSocks.get(name);
  if (!sock) throw new Error('Сессия не активна');

  const obj = await sock.groupFetchAllParticipating();
  const list = Object.values(obj || {}).map(g => ({
    id: g.id || g.jid,
    subject: g.subject || '(без названия)',
    size: Array.isArray(g.participants) ? g.participants.length : (g.size || 0),
  })).sort((a, b) => a.subject.localeCompare(b.subject, 'ru'));

  groupsCache.set(name, { list, ts: Date.now() });
  return list;
}


function groupsKeyboard(name, page, pages) {
  const rows = [];

  
  const window = 2;
  const maxNums = 9;
  const nums = new Set([1, pages, page]);
  for (let i = 1; i <= window; i++) { nums.add(page - i); nums.add(page + i); }
  const arr = [...nums].filter(p => p >= 1 && p <= pages).sort((a,b)=>a-b);

  
  const numericRow = [];
  let prev = 0;
  for (const p of arr) {
    if (p - prev > 1) numericRow.push(Markup.button.callback('…', cbNoop));
    numericRow.push(Markup.button.callback(p === page ? `·${p}·` : String(p), cbList(name, p)));
    prev = p;
    if (numericRow.length >= maxNums) break;
  }
  if (numericRow.length) rows.push(numericRow);

  
  const arrows = [];
  if (page > 1) arrows.push(Markup.button.callback('⏮ 1', cbList(name, 1)));
  if (page > 1) arrows.push(Markup.button.callback('⬅️', cbList(name, page - 1)));
  if (page < pages) arrows.push(Markup.button.callback('➡️', cbList(name, page + 1)));
  if (page < pages) arrows.push(Markup.button.callback(`${pages} ⏭`, cbList(name, pages)));
  if (arrows.length) rows.push(arrows);

  
  rows.push([Markup.button.callback('🔄 Обновить', cbRefresh(name, page))]);
  rows.push([Markup.button.callback('⬅️ К сессии', `info_${name}`)]);
  return Markup.inlineKeyboard(rows);
}

function withInvisibleFlip(msgId, text) {
  const prev = renderToggle.get(msgId)?.value || false;
  renderToggle.set(msgId, { value: !prev, timestamp: Date.now() });
  const pad = prev ? '\u2060' : '\u2061';
  return text + pad;
}

async function renderGroups(ctx, name, pageReq) {
  if (sessionStatus.get(name) !== 'active') {
    return ctx.editMessageText(
      `⚠️ Сессия «${name}» не активна.`,
      { ...Markup.inlineKeyboard([
        [Markup.button.callback('🔄 Активировать', `activate_${name}`)],
        [Markup.button.callback('⬅️ Назад', `info_${name}`)],
      ])}
    ).catch(() => {});
  }

  try {
    const groups = await fetchGroups(name);
    const { slice, page, pages, total } = paginate(groups, pageReq, GROUPS_PAGE_SIZE);
    const body = slice.length
      ? slice.map((g, i) => `${(page - 1) * GROUPS_PAGE_SIZE + i + 1}. ${g.subject} · 👥 ${g.size}`).join('\n')
      : 'Нет групп, где аккаунт состоит.';

    
    const msgId = ctx.callbackQuery?.message?.message_id;
    const textRaw = `👥 Группы сессии «${name}»\nВсего: ${total}\nСтр. ${page}/${pages}\n\n${body}`;
    const text = msgId ? withInvisibleFlip(msgId, textRaw) : textRaw;

    await ctx.editMessageText(text, { ...groupsKeyboard(name, page, pages) })
      .catch(async (e) => {
        const desc = e?.description || e?.message || '';
        if (/message is not modified/i.test(desc)) {
          try { await ctx.answerCbQuery('Без изменений'); } catch {}
        } else {
          throw e;
        }
      });
  } catch (e) {
    await ctx.editMessageText(`❌ Не удалось получить группы: ${e.message}`, {
      ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]])
    }).catch(()=>{});
  }
}

bot.action(/^grp:list:([^:]+):(\d+)$/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const name = ctx.match[1];
  const pageReq = parseInt(ctx.match[2], 10) || 1;
  await renderGroups(ctx, name, pageReq);
});

bot.action(/^grp:refresh:([^:]+):(\d+)$/, async (ctx) => {
  try { await ctx.answerCbQuery('Обновляю…'); } catch {}
  const name = ctx.match[1];
  const pageReq = parseInt(ctx.match[2], 10) || 1;
  groupsCache.delete(name);
  await renderGroups(ctx, name, pageReq);
});

bot.action('broadcast_skip_media', async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const state = userData.get(ctx.from.id);
  if (!state || (state.action !== 'broadcast_setup' && state.action !== 'broadcast_waiting_media')) return;
  
  state.action = 'broadcast_setup'; 
  state.data.skipMedia = true;
  await showIntervalSetup(ctx, state.data);
});

bot.action('broadcast_ready', async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const state = userData.get(ctx.from.id);
  if (!state || (state.action !== 'broadcast_setup' && state.action !== 'broadcast_waiting_media')) return;
  
  if (!state.data.messageText && !state.data.mediaType) {
    return ctx.reply(
      '❌ Нужно добавить хотя бы текст или медиафайл!',
      Markup.inlineKeyboard([
        [Markup.button.callback('⬅️ Назад', 'start')]
      ])
    );
  }
  
  state.action = 'broadcast_setup'; 
  await showIntervalSetup(ctx, state.data);
});

async function showIntervalSetup(ctx, data) {
  try {
    await ctx.editMessageText(
      `⏱️ Настройка интервалов рассылки\n\n` +
      `📊 Сессия: ${data.sessionName}\n` +
      `📝 Текст: ${data.messageText ? data.messageText.substring(0, 50) + (data.messageText.length > 50 ? '...' : '') : 'Нет'}\n` +
      `📎 Медиа: ${data.mediaType ? 'Добавлено' : 'Нет'}\n\n` +
      `⚙️ Выберите интервал между отправками:`,
      Markup.inlineKeyboard([
        [Markup.button.callback('5-15 секунд', 'broadcast_interval_5_15')],
        [Markup.button.callback('15-30 секунд', 'broadcast_interval_15_30')],
        [Markup.button.callback('30-60 секунд', 'broadcast_interval_30_60')],
        [Markup.button.callback('60-180 секунд', 'broadcast_interval_60_180')],
        [Markup.button.callback('🔧 Свой интервал', 'broadcast_custom_interval')],
        [Markup.button.callback('⬅️ Назад', 'start')]
      ])
    );
  } catch {
    await ctx.reply(
      `⏱️ Настройка интервалов рассылки\n\n` +
      `📊 Сессия: ${data.sessionName}\n` +
      `📝 Текст: ${data.messageText ? data.messageText.substring(0, 50) + (data.messageText.length > 50 ? '...' : '') : 'Нет'}\n` +
      `📎 Медиа: ${data.mediaType ? 'Добавлено' : 'Нет'}\n\n` +
      `⚙️ Выберите интервал между отправками:`,
      Markup.inlineKeyboard([
        [Markup.button.callback('5-15 секунд', 'broadcast_interval_5_15')],
        [Markup.button.callback('15-30 секунд', 'broadcast_interval_15_30')],
        [Markup.button.callback('30-60 секунд', 'broadcast_interval_30_60')],
        [Markup.button.callback('60-180 секунд', 'broadcast_interval_60_180')],
        [Markup.button.callback('🔧 Свой интервал', 'broadcast_custom_interval')],
        [Markup.button.callback('⬅️ Назад', 'start')]
      ])
    );
  }
}

bot.action(/broadcast_interval_(\d+)_(\d+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const state = userData.get(ctx.from.id);
  if (!state || state.action !== 'broadcast_setup') return;
  
  const minInterval = parseInt(ctx.match[1]);
  const maxInterval = parseInt(ctx.match[2]);
  
  state.data.minInterval = minInterval;
  state.data.maxInterval = maxInterval;
  
  await startBroadcast(ctx, state.data);
});

bot.action(/broadcast_type_immediate_(.+)_(\d+)_(\d+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const sessionName = ctx.match[1];
  const minInterval = parseInt(ctx.match[2]);
  const maxInterval = parseInt(ctx.match[3]);
  
  const state = userData.get(ctx.from.id);
  if (!state) return;
  
  const sock = activeSocks.get(sessionName);
  if (!sock) {
    return ctx.editMessageText(
      `❌ Сессия «${sessionName}» неактивна`,
      Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
    );
  }
  
  try {
    const groups = await fetchGroups(sessionName);
    
    const broadcastId = Date.now().toString();
    broadcasts.set(broadcastId, {
      sessionName,
      messageText: state.data.messageText,
      mediaType: state.data.mediaType,
      mediaInfo: state.data.mediaInfo,
      minInterval,
      maxInterval,
      groups,
      total: groups.length,
      sent: 0,
      failed: 0,
      status: 'running',
      startTime: Date.now(),
      completedAt: null,
      userId: ctx.from.id 
    });
    
    userData.delete(ctx.from.id);
    
    updateUserStatus(ctx.from.id, 'broadcasting', `${sessionName} (${groups.length} групп)`);
    
    await ctx.editMessageText(
      `🚀 ОБЫЧНАЯ РАССЫЛКА ЗАПУЩЕНА!\n\n` +
      `📊 Сессия: ${sessionName}\n` +
      `👥 Групп: ${groups.length}\n` +
      `⏱️ Интервал: ${minInterval}-${maxInterval} сек.\n\n` +
      `📈 Прогресс будет обновляться автоматически...`,
      Markup.inlineKeyboard([
        [Markup.button.callback('⏸️ Остановить', `broadcast_stop_${broadcastId}`)],
        [Markup.button.callback('📊 Статистика', `broadcast_stats_${broadcastId}`)]
      ])
    );
    
    runBroadcast(broadcastId, ctx);
    
  } catch (error) {
    await ctx.editMessageText(
      `❌ Ошибка запуска рассылки: ${error.message}`,
      Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
    );
  }
});

bot.action(/broadcast_type_scheduled_(.+)_(\d+)_(\d+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const sessionName = ctx.match[1];
  const minInterval = parseInt(ctx.match[2]);
  const maxInterval = parseInt(ctx.match[3]);
  
  const state = userData.get(ctx.from.id);
  if (!state) return;
  
  state.data.sessionName = sessionName;
  state.data.minInterval = minInterval;
  state.data.maxInterval = maxInterval;
  state.data.action = 'waiting_schedule_time';
  
  const now = new Date();
  const tomorrow = new Date(now.getTime() + 24 * 60 * 60 * 1000);
  
  await ctx.editMessageText(
    `⏰ НАСТРОЙКА ЗАПЛАНИРОВАННОЙ РАССЫЛКИ\n\n` +
    `🤖 Сессия: ${sessionName}\n` +
    `📝 Пост: ${state.data.messageText ? state.data.messageText.substring(0, 50) + (state.data.messageText.length > 50 ? '...' : '') : 'Медиа'}\n` +
    `⏱️ Интервал: ${minInterval}-${maxInterval} сек.\n\n` +
    `📅 Введите дату и время запуска:\n\n` +
    `📝 Форматы:\n` +
    `• ` + `ДД.ММ.ГГГГ ЧЧ:ММ` + ` (например: 25.12.2024 15:30)\n` +
    `• ` + `сегодня ЧЧ:ММ` + ` (например: сегодня 18:00)\n` +
    `• ` + `завтра ЧЧ:ММ` + ` (например: завтра 09:00)\n` +
    `• ` + `через 2 часа` + `\n` +
    `• ` + `через 30 минут` + `\n\n` +
    `⏰ Текущее время: ${now.toLocaleString('ru-RU')}`,
    Markup.inlineKeyboard([
      [Markup.button.callback('⬅️ Назад', 'start')],
      [Markup.button.callback('❌ Отмена', 'start')]
    ])
  );
});

bot.action(/broadcast_groups_(.+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const sessionName = ctx.match[1];
  
  try {
    const groups = await fetchGroups(sessionName);
    const sortedGroups = groups.sort((a, b) => b.size - a.size);
    
    
    const topGroups = sortedGroups.slice(0, 20);
    const groupsList = topGroups.map((g, i) => 
      `${i + 1}. ${g.subject} - ${g.size} 👤`
    ).join('\n');
    
    const totalMembers = groups.reduce((sum, g) => sum + g.size, 0);
    
    await ctx.editMessageText(
      `📋 СПИСОК ГРУПП (${groups.length} всего)\n\n` +
      `👤 Участников всего: ${totalMembers.toLocaleString('ru-RU')}\n\n` +
      `🔝 Топ-20 групп:\n${groupsList}` +
      (groups.length > 20 ? `\n\n... и еще ${groups.length - 20} групп` : ''),
      Markup.inlineKeyboard([
        [Markup.button.callback('⬅️ Назад', 'start')]
      ])
    );
    
  } catch (error) {
    await ctx.editMessageText(
      `❌ Ошибка загрузки групп: ${error.message}`,
      Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
    );
  }
});

bot.action('broadcast_custom_interval', async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const state = userData.get(ctx.from.id);
  if (!state || state.action !== 'broadcast_setup') return;
  
  state.data.action = 'waiting_custom_interval';
  
  await ctx.editMessageText(
    `🔧 Настройка своего интервала\n\n` +
    `📊 Сессия: ${state.data.sessionName}\n` +
    `📝 Текст: ${state.data.messageText ? state.data.messageText.substring(0, 50) + (state.data.messageText.length > 50 ? '...' : '') : 'Нет'}\n` +
    `📎 Медиа: ${state.data.mediaType ? 'Добавлено' : 'Нет'}\n\n` +
    `⏱️ Введите интервал в формате:\n` +
    `• 10-30 (от 10 до 30 секунд)\n` +
    `• 5 (фиксированные 5 секунд)\n` +
`• 120-300сек (от 2 до 5 минут)\n\n` +
     `💡 Используйте только секунды`,
    Markup.inlineKeyboard([
      [Markup.button.callback('⬅️ Назад', 'broadcast_back_to_intervals')],
      [Markup.button.callback('❌ Отмена', 'start')]
    ])
  );
});

bot.action('broadcast_back_to_intervals', async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const state = userData.get(ctx.from.id);
  if (!state || state.action !== 'broadcast_setup') return;
  
  delete state.data.action;
  await showIntervalSetup(ctx, state.data);
});

async function startBroadcast(ctx, data) {
  const { sessionName, messageText, minInterval, maxInterval, mediaType, mediaInfo } = data;
  const sock = activeSocks.get(sessionName);
  
  
  updateUserStatus(ctx.from.id, 'setup', `настройка для ${sessionName}`);
  
  if (!sock) {
    try {
      return await ctx.editMessageText(
        `❌ Сессия «${sessionName}» неактивна`,
        Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
      );
    } catch {
      return await ctx.reply(
        `❌ Сессия «${sessionName}» неактивна`,
        Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
      );
    }
  }
  
  try {
    const groups = await fetchGroups(sessionName);
    if (groups.length === 0) {
      try {
        return await ctx.editMessageText(
          `❌ Нет групп для рассылки в сессии «${sessionName}»`,
          Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
        );
      } catch {
        return await ctx.reply(
          `❌ Нет групп для рассылки в сессии «${sessionName}»`,
          Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
        );
      }
    }
    
    
    const sortedGroups = groups.sort((a, b) => b.size - a.size);
    const totalMembers = groups.reduce((sum, g) => sum + g.size, 0);
    const avgGroupSize = Math.round(totalMembers / groups.length);
    
    
    const largeGroups = groups.filter(g => g.size >= 100).length;
    const mediumGroups = groups.filter(g => g.size >= 20 && g.size < 100).length;
    const smallGroups = groups.filter(g => g.size < 20).length;
    
    const previewText = messageText ? messageText.substring(0, 100) + (messageText.length > 100 ? '...' : '') : 'Нет текста';
    
    try {
      await ctx.editMessageText(
        `📊 СТАТИСТИКА РАССЫЛКИ\n\n` +
        `🤖 Сессия: ${sessionName}\n` +
        `👥 Групп всего: ${groups.length}\n` +
        `👤 Участников всего: ${totalMembers.toLocaleString('ru-RU')}\n` +
        `📊 Средний размер группы: ${avgGroupSize} человек\n\n` +
        `📈 Распределение групп:\n` +
        `• Крупные (100+): ${largeGroups}\n` +
        `• Средние (20-99): ${mediumGroups}\n` +
        `• Маленькие (<20): ${smallGroups}\n\n` +
        `📝 Тип поста: ${mediaType ? getMediaTypeName(mediaType) : 'Текст'}\n` +
        `📄 Текст: ${previewText}\n` +
        `⏱️ Интервал: ${minInterval}-${maxInterval} сек.\n\n` +
        `⏱️ Примерное время: ${estimateBroadcastTime(groups.length, minInterval, maxInterval)}\n\n` +
        `🚀 Выберите тип рассылки:`,
        Markup.inlineKeyboard([
          [Markup.button.callback('🚀 Обычная рассылка', `broadcast_type_immediate_${sessionName}_${minInterval}_${maxInterval}`)],
          [Markup.button.callback('⏰ Запланированная рассылка', `broadcast_type_scheduled_${sessionName}_${minInterval}_${maxInterval}`)],
          [Markup.button.callback('📋 Список групп', `broadcast_groups_${sessionName}`)],
          [Markup.button.callback('⬅️ Назад', 'start')]
        ])
      );
    } catch (editError) {
      await ctx.reply(
        `📊 СТАТИСТИКА РАССЫЛКИ\n\n` +
        `🤖 Сессия: ${sessionName}\n` +
        `👥 Групп всего: ${groups.length}\n` +
        `👤 Участников всего: ${totalMembers.toLocaleString('ru-RU')}\n` +
        `📊 Средний размер группы: ${avgGroupSize} человек\n\n` +
        `📈 Распределение групп:\n` +
        `• Крупные (100+): ${largeGroups}\n` +
        `• Средние (20-99): ${mediumGroups}\n` +
        `• Маленькие (<20): ${smallGroups}\n\n` +
        `📝 Тип поста: ${mediaType ? getMediaTypeName(mediaType) : 'Текст'}\n` +
        `📄 Текст: ${previewText}\n` +
        `⏱️ Интервал: ${minInterval}-${maxInterval} сек.\n\n` +
        `⏱️ Примерное время: ${estimateBroadcastTime(groups.length, minInterval, maxInterval)}\n\n` +
        `🚀 Выберите тип рассылки:`,
        Markup.inlineKeyboard([
          [Markup.button.callback('🚀 Обычная рассылка', `broadcast_type_immediate_${sessionName}_${minInterval}_${maxInterval}`)],
          [Markup.button.callback('⏰ Запланированная рассылка', `broadcast_type_scheduled_${sessionName}_${minInterval}_${maxInterval}`)],
          [Markup.button.callback('📋 Список групп', `broadcast_groups_${sessionName}`)],
          [Markup.button.callback('⬅️ Назад', 'broadcast_back_to_intervals')]
        ])
      );
    }
    
  } catch (error) {
    try {
      await ctx.editMessageText(
        `❌ Ошибка анализа групп: ${error.message}`,
        Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'broadcast_back_to_intervals')]])
      );
    } catch {
      await ctx.reply(
        `❌ Ошибка анализа групп: ${error.message}`,
        Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'broadcast_back_to_intervals')]])
      );
    }
  }
}

function getMediaTypeName(type) {
  const names = {
    'image': 'Фото + текст',
    'video': 'Видео + текст', 
    'document': 'Документ + текст'
  };
  return names[type] || 'Медиа';
}

function estimateBroadcastTime(groupCount, minInterval, maxInterval) {
  const avgInterval = (minInterval + maxInterval) / 2;
  const totalSeconds = groupCount * avgInterval;
  
  if (totalSeconds < 60) {
    return `~${Math.round(totalSeconds)} сек.`;
  } else if (totalSeconds < 3600) {
    return `~${Math.round(totalSeconds / 60)} мин.`;
  } else {
    const hours = Math.floor(totalSeconds / 3600);
    const minutes = Math.round((totalSeconds % 3600) / 60);
    return `~${hours}ч ${minutes}мин.`;
  }
}

async function runBroadcast(broadcastId, ctx) {
  const broadcast = broadcasts.get(broadcastId);
  if (!broadcast) return;
  
  const sock = activeSocks.get(broadcast.sessionName);
  if (!sock) {
    broadcast.status = 'error';
    if (broadcast.userId) {
      updateUserStatus(broadcast.userId, 'idle');
    }
    return;
  }
  
  
  broadcast.recentResults = [];
  broadcast.detailedLog = [];
  
  for (let i = 0; i < broadcast.groups.length; i++) {
    if (broadcast.status !== 'running') break;
    
    const group = broadcast.groups[i];
    const delay = Math.random() * (broadcast.maxInterval - broadcast.minInterval) + broadcast.minInterval;
    
    try {
      let messageContent;
      
      if (broadcast.mediaType && broadcast.mediaInfo) {
        
        if (broadcast.mediaType === 'image') {
          messageContent = {
            image: { url: broadcast.mediaInfo.url },
            caption: broadcast.messageText
          };
        } else if (broadcast.mediaType === 'video') {
          messageContent = {
            video: { url: broadcast.mediaInfo.url },
            caption: broadcast.messageText
          };
        } else if (broadcast.mediaType === 'document') {
          messageContent = {
            document: { url: broadcast.mediaInfo.url },
            caption: broadcast.messageText,
            fileName: broadcast.mediaInfo.fileName
          };
        }
      } else {
        
        messageContent = { text: broadcast.messageText };
      }
      
      const startTime = Date.now();
      await sock.sendMessage(group.id, messageContent);
      const sendTime = Date.now() - startTime;
      
      broadcast.sent++;
      const result = {
        groupName: group.subject,
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
      
      
      if (broadcast.sent === 1 || broadcast.sent % 3 === 0 || i === broadcast.groups.length - 1) {
        await updateBroadcastProgress(ctx, broadcastId);
      }
      
    } catch (error) {
      broadcast.failed++;
      
      
      const result = {
        groupName: group.subject,
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
      
      console.error(`[BROADCAST ERROR] ${group.id}:`, error.message);
    }
    
    
    if (i < broadcast.groups.length - 1 && broadcast.status === 'running') {
      await new Promise(resolve => setTimeout(resolve, delay * 1000));
    }
  }
  
  broadcast.status = 'completed';
  broadcast.completedAt = Date.now();
  
  
  if (broadcast.userId) {
    updateUserStatus(broadcast.userId, 'idle');
  }
  
  await updateBroadcastProgress(ctx, broadcastId);
}

async function updateBroadcastProgress(ctx, broadcastId) {
  const broadcast = broadcasts.get(broadcastId);
  if (!broadcast) return;
  
  const progress = Math.round((broadcast.sent / broadcast.total) * 100);
  const elapsed = Math.floor((Date.now() - broadcast.startTime) / 1000);
  const minutes = Math.floor(elapsed / 60);
  const seconds = elapsed % 60;
  
  const statusEmoji = broadcast.status === 'running' ? '🚀' : 
                     broadcast.status === 'completed' ? '✅' : '⏸️';
  
  
  const recentResults = broadcast.recentResults || [];
  const lastResult = recentResults[recentResults.length - 1];
  
  let statusDetails = '';
  if (lastResult && broadcast.status === 'running') {
    const waitTime = lastResult.nextDelay ? Math.round(lastResult.nextDelay) : 0;
    statusDetails = `\n🔄 Последняя отправка: ${lastResult.success ? '✅ Успешно' : '❌ Ошибка'}\n` +
                   `📩 Группа: ${lastResult.groupName}\n` +
                   `⏳ Ожидание: ${waitTime} сек.\n`;
  }
  
  try {
    await ctx.telegram.editMessageText(
      ctx.chat?.id,
      ctx.callbackQuery?.message?.message_id,
      undefined,
      `${statusEmoji} РАССЫЛКА В ПРОЦЕССЕ\n\n` +
      `🤖 Сессия: ${broadcast.sessionName}\n` +
      `📊 Статус: ${broadcast.status === 'running' ? 'Выполняется' : 
                     broadcast.status === 'completed' ? 'Завершена' : 'Остановлена'}\n` +
      `📈 Прогресс: ${broadcast.sent}/${broadcast.total} (${progress}%)\n` +
      `✅ Успешно: ${broadcast.sent}\n` +
      `❌ Ошибок: ${broadcast.failed}\n` +
      `⏱️ Время работы: ${minutes}:${seconds.toString().padStart(2, '0')}\n` +
      `⏱️ Интервал: ${broadcast.minInterval}-${broadcast.maxInterval} сек.` +
      statusDetails +
      `\n💡 Нажмите "📋 Детали" для просмотра лога отправки`,
      Markup.inlineKeyboard([
        broadcast.status === 'running' ? 
          [Markup.button.callback('⏸️ Остановить', `broadcast_stop_${broadcastId}`)] : [],
        [Markup.button.callback('📋 Детали', `broadcast_details_${broadcastId}`)],
        [Markup.button.callback('🔄 Обновить', `broadcast_stats_${broadcastId}`)],
        [Markup.button.callback('⬅️ В меню', 'start')]
      ].filter(Boolean))
    );
  } catch (error) {
    
    if (!error.message.includes('message is not modified')) {
      console.error('[BROADCAST_PROGRESS_UPDATE_ERROR]', { 
        broadcastId, 
        error: error.message 
      });
    }
  }
}

bot.action(/broadcast_stop_(.+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const broadcastId = ctx.match[1];
  const broadcast = broadcasts.get(broadcastId);
  
  if (broadcast) {
    broadcast.status = 'stopped';
    
    
    if (broadcast.userId) {
      updateUserStatus(broadcast.userId, 'idle');
    }
    
    await updateBroadcastProgress(ctx, broadcastId);
  }
});

bot.action(/broadcast_stats_(.+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const broadcastId = ctx.match[1];
  await updateBroadcastProgress(ctx, broadcastId);
});

bot.action(/broadcast_details_(.+)/, async (ctx) => {
  try { await ctx.answerCbQuery(); } catch {}
  const broadcastId = ctx.match[1];
  const broadcast = broadcasts.get(broadcastId);
  
  if (!broadcast) {
    return ctx.reply('❌ Рассылка не найдена');
  }
  
  const log = broadcast.detailedLog || [];
  const recentLog = log.slice(-10); 
  
  if (recentLog.length === 0) {
    return ctx.editMessageText(
      '📋 ЛОГ ОТПРАВКИ ПУСТОЙ\n\n' +
      'Рассылка еще не началась или нет данных для отображения.',
      Markup.inlineKeyboard([
        [Markup.button.callback('⬅️ Назад', `broadcast_stats_${broadcastId}`)]
      ])
    );
  }
  
  const logText = recentLog.map((entry, index) => {
    const status = entry.success ? '✅' : '❌';
    const time = entry.timestamp;
    const name = entry.groupName.length > 25 ? entry.groupName.substring(0, 25) + '...' : entry.groupName;
    const size = entry.groupSize ? ` (${entry.groupSize}👤)` : '';
    const waitTime = entry.nextDelay ? ` ⏳${Math.round(entry.nextDelay)}с` : '';
    const error = entry.error ? `\n   Ошибка: ${entry.error}` : '';
    
    return `${index + 1}. ${status} ${time} - ${name}${size}${waitTime}${error}`;
  }).join('\n');
  
  const header = `📋 ДЕТАЛЬНЫЙ ЛОГ ОТПРАВКИ\n\n` +
                `🤖 Сессия: ${broadcast.sessionName}\n` +
                `📊 Статус: ${broadcast.status === 'running' ? 'Выполняется' : 
                               broadcast.status === 'completed' ? 'Завершена' : 'Остановлена'}\n` +
                `📈 Прогресс: ${broadcast.sent}/${broadcast.total}\n\n` +
                `📝 Последние ${recentLog.length} записей:\n\n`;
  
  await ctx.editMessageText(
    header + logText + 
    (log.length > recentLog.length ? `\n\n... и еще ${log.length - recentLog.length} записей` : ''),
    Markup.inlineKeyboard([
      [Markup.button.callback('🔄 Обновить', `broadcast_details_${broadcastId}`)],
      [Markup.button.callback('⬅️ Назад', `broadcast_stats_${broadcastId}`)]
    ])
  );
});


bot.use((ctx, next) => {
  if (ctx.from && ctx.from.id) {
    
    setImmediate(() => {
      addUser(ctx.from.id, {
        username: ctx.from.username,
        firstName: ctx.from.first_name,
        lastName: ctx.from.last_name
      });
      
      
      const users = loadUsers();
      const userData = users.get(ctx.from.id.toString());
      if (userData && userData.status !== 'broadcasting' && userData.status !== 'setup') {
        updateUserStatus(ctx.from.id, 'idle');
      }
    });
  }
  return next();
});

bot.action(cbNoop, async (ctx) => { 
  try { 
    await ctx.answerCbQuery(); 
  } catch (e) {
    console.error('[NOOP_CB_ERROR]', { error: e.message });
  } 
});

bot.catch((e) => console.error('[BOT ERROR]', e));
bot.launch().then(() => console.log('Bot started'));
process.on('unhandledRejection', (e) => console.error('[UNHANDLED]', e));
process.on('SIGINT', () => process.exit(0));
process.on('SIGTERM', () => process.exit(0));
