

import 'dotenv/config';
import { Telegraf, Markup } from 'telegraf';
import makeWASocket, {
  DisconnectReason,
  useMultiFileAuthState,
  fetchLatestBaileysVersion,
  Browsers,
} from '@whiskeysockets/baileys';
import qrcode from 'qrcode';
import fs from 'fs';
import path from 'path';
import express from 'express';
import { createServer } from 'http';
import { Server } from 'socket.io';


if (!process.env.BOT_TOKEN) { console.error('BOT_TOKEN не задан'); process.exit(1); }
const bot = new Telegraf(process.env.BOT_TOKEN);

const SESSIONS_DIR        = path.join(process.cwd(), 'sessions');
const LINKS_DIR           = path.join(process.cwd(), 'links');
const QR_TIMEOUT_CREATE   = 60;
const QR_TIMEOUT_ACTIVATE = 60;
const GROUPS_PAGE_SIZE    = 10;
const LINKS_PAGE_SIZE      = 8;
const GROUPS_CACHE_TTL    = 2 * 60 * 1000;
const usersFile           = './users.json';

const WA_PHONE = (process.env.WA_PHONE || '').replace(/\D/g, '');

fs.mkdirSync(SESSIONS_DIR, { recursive: true });
fs.mkdirSync(LINKS_DIR,    { recursive: true });


const activeSocks      = new Map();
const sessionStatus    = new Map();
const userData         = new Map();
const qrTimers         = new Map();
const qrAnchors        = new Map();
const groupsCache      = new Map();
const infoAnchors      = new Map();
const renderToggle     = new Map();
const savedLinks       = new Map();
const joinQueue        = new Map();
const sessionIntervals = new Map();
const lastJoinInfo     = new Map();
const processingQueue  = new Map();
const notificationsHistory = []; // Общая история уведомлений для всех пользователей
const manualGroupsLists = new Map(); // Списки групп для ручного добавления по сессиям


const statusEmoji = (s) => ({ inactive:'🔴', qr:'🟡', syncing:'🟠', active:'🟢', error:'⚠️' }[s] || '⚪');
const sessionHuman = (st) =>
  st === 'active'  ? 'Активна' :
  st === 'syncing' ? 'Синхронизация…' :
  st === 'qr'      ? 'Ожидает сканирования QR' :
  st === 'error'   ? 'Ошибка' : 'Не активна';

const isValidName = (s) => /^[A-Za-z0-9][A-Za-z0-9_-]{1,63}$/.test(s);
const canonizeName = (s) => {
  let n = (s || '').trim().toLowerCase();
  const translit = {'а':'a','б':'b','в':'v','г':'g','д':'d','е':'e','ё':'yo','ж':'zh','з':'z','и':'i','й':'y','к':'k','л':'l','м':'m','н':'n','о':'o','п':'p','р':'r','с':'s','т':'t','у':'u','ф':'f','х':'h','ц':'ts','ч':'ch','ш':'sh','щ':'sch','ъ':'','ы':'y','ь':'','э':'e','ю':'yu','я':'ya'};
  n = n.replace(/[а-яё]/g, m => translit[m] || '_').replace(/[^a-z0-9_-]/g,'_').replace(/_{2,}/g,'_').replace(/^[_-]+|[_-]+$/g,'');
  if (n.includes('..')) n = n.replace(/\.\./g,'_');
  if (!isValidName(n) || n.length < 2 || n.length > 64) n = `session_${new Date().toISOString().replace(/[-:T.Z]/g,'').slice(0,14)}`;
  return n;
};
const safeSessionPath = (name) => {
  const safeName = canonizeName(name);
  const sessionPath = path.join(SESSIONS_DIR, safeName);
  const base = path.resolve(SESSIONS_DIR), real = path.resolve(sessionPath);
  if (!real.startsWith(base)) throw new Error('Invalid session path');
  try { const rp = fs.realpathSync(real); if (!rp.startsWith(base)) throw new Error('Symlink out of base'); } catch {}
  return { safeName, sessionPath };
};
const listSessionDirs = () =>
  fs.readdirSync(SESSIONS_DIR).filter(d => { try { return fs.statSync(path.join(SESSIONS_DIR, d)).isDirectory() && isValidName(d); } catch { return false; }});
const paginate = (arr, page, size) => {
  const total = arr.length, pages = Math.max(1, Math.ceil(total/size));
  const p = Math.min(Math.max(1,page), pages);
  const start = (p-1)*size, end = Math.min(start+size, total);
  return { slice: arr.slice(start,end), page:p, pages, total };
};
const setStatusOriginal = (name, s) => { sessionStatus.set(name, s); console.log(`[STATUS] ${name}: ${s}`); updateInfoCard(name).catch(()=>{}); };
const setStatus = (name, s) => { 
  setStatusOriginal(name, s); 
  if (typeof io !== 'undefined') {
    io.emit('session_status', { name, status: s, statusText: sessionHuman(s) });
  }
};


const loadUsers = () => { try { if (fs.existsSync(usersFile)) return new Map(JSON.parse(fs.readFileSync(usersFile,'utf8'))); } catch(e){ console.error('[USERS_LOAD_ERROR]', e.message);} return new Map(); };
const saveUsers = (users) => { try { fs.writeFileSync(usersFile, JSON.stringify([...users]), 'utf8'); } catch(e){ console.error('[USERS_SAVE_ERROR]', e.message);} };
const getAllUsers = () => { const m = loadUsers(); return Array.from(m.entries()).map(([id,info]) => ({ id: parseInt(id), ...info })); };
const addUser = (id, info) => { const m = loadUsers(); m.set(String(id), { ...info, lastSeen: Date.now(), username: info.username||null, firstName: info.firstName||null, lastName: info.lastName||null, status:'offline', currentActivity:null, activityStartTime:null }); saveUsers(m); };
const updateUserStatus = (id, status, activity=null) => { const m = loadUsers(); const u = m.get(String(id)); if (!u) return; u.status=status; u.currentActivity=activity; u.activityStartTime = status!=='offline'?Date.now():null; u.lastSeen=Date.now(); m.set(String(id),u); saveUsers(m); };


const CLEANUP_RULES = {
  userData:{ ttl:2*60*60*1000, checkInterval:5*60*1000 },
  qrTimers:{ ttl:5*60*1000,    checkInterval:60*1000 },
  groupsCache:{ ttl:10*60*1000, checkInterval:2*60*1000 },
  infoAnchors:{ ttl:30*60*1000, checkInterval:5*60*1000 },
  renderToggle:{ ttl:15*60*1000, checkInterval:3*60*1000 },
  savedLinks:{ ttl:7*24*60*60*1000, checkInterval:60*60*1000 },
  joinQueue:{ ttl:24*60*60*1000, checkInterval:30*60*1000 },
  lastJoinInfo:{ ttl:7*24*60*60*1000, checkInterval:60*60*1000 },
  processingQueue:{ ttl:2*60*60*1000, checkInterval:30*60*1000 },
  notificationsHistory:{ ttl:30*24*60*60*1000, checkInterval:24*60*60*1000 }, // 30 дней
};
const cleanupMap = (name, mapObj) => {
  const rule = CLEANUP_RULES[name]; if (!rule) return;
  const now = Date.now(); const toDel = [];
  for (const [k,v] of mapObj.entries()) {
    let ts = v.timestamp || v.ts;
    if (name==='infoAnchors') ts = v.lastUsed;
    if (name==='renderToggle') ts = v.timestamp;
    if (name==='savedLinks')   ts = v.addedAt;
    if (name==='lastJoinInfo') ts = v.timestamp;
    if (name==='joinQueue') { v.forEach((it,i)=>{ if (it.timestamp && (now-it.timestamp>rule.ttl)) toDel.push({k,i}); }); continue; }
    if (ts && (now-ts>rule.ttl)) toDel.push(k);
  }
  toDel.forEach(key=>{
    try{
      if (typeof key==='object' && key.k!==undefined){ const q = mapObj.get(key.k); if (q) q.splice(key.i,1); }
      else { if (name==='joinQueue'){ const q = mapObj.get(key); if (q?.some(it=>it.processing)) return; } mapObj.delete(key); }
    }catch(e){ console.error(`[CLEANUP ERROR] ${name}:`, e.message); }
  });
  if (toDel.length) console.log(`[CLEANUP] ${name}: removed ${toDel.length}, left ${mapObj.size}`);
};
const startCleanupSystem = () => {
  Object.entries(CLEANUP_RULES).forEach(([n,r])=>{
    setInterval(()=>{ const m = {userData,qrTimers,groupsCache,infoAnchors,renderToggle,savedLinks,joinQueue,lastJoinInfo,processingQueue}[n]; if (m?.size) cleanupMap(n,m); }, r.checkInterval);
  });
  console.log('[CLEANUP] Memory cleanup system started');
};


const extractLinks = (text) => (text.match(/https?:\/\/(?:chat\.whatsapp\.com|t\.me)\/[A-Za-z0-9_-]+/g) || []);

// Функции для управления списками групп
const getManualGroupsList = (sessionId) => {
  if (!manualGroupsLists.has(sessionId)) {
    manualGroupsLists.set(sessionId, []);
  }
  return manualGroupsLists.get(sessionId);
};

const addManualGroup = (sessionId, link) => {
  const list = getManualGroupsList(sessionId);
  // Проверяем дубликаты
  if (list.some(g => g.link === link)) {
    return { success: false, error: 'Группа уже в списке' };
  }
  
  const group = {
    link,
    type: link.includes('chat.whatsapp.com') ? 'whatsapp' : 'telegram',
    addedAt: Date.now(),
    added: false // Флаг, добавлена ли в очередь
  };
  
  list.push(group);
  return { success: true, group };
};

const removeManualGroup = (sessionId, link) => {
  const list = getManualGroupsList(sessionId);
  const index = list.findIndex(g => g.link === link);
  if (index > -1) {
    list.splice(index, 1);
    return { success: true };
  }
  return { success: false, error: 'Группа не найдена' };
};

const addManualGroupsToQueue = (sessionId) => {
  const list = getManualGroupsList(sessionId);
  let added = 0;
  
  for (const group of list) {
    if (!group.added) {
      const id = saveLink(group.link, sessionId, 'Ручное добавление');
      if (id) {
        addToJoinQueue(sessionId, group.link, group.type);
        group.added = true;
        added++;
      }
    }
  }
  
  return added;
};
const isLinkDuplicate = (link) => { for (const [,v] of savedLinks.entries()) if (v.url===link) return true; return false; };
const saveLink = (link, sessionId, groupName=null) => {
  if (isLinkDuplicate(link)) return false;
  const id = `link_${Date.now()}_${Math.random().toString(36).slice(2,9)}`;
  const type = link.includes('chat.whatsapp.com') ? 'whatsapp' : 'telegram';
  savedLinks.set(id, { url:link, type, addedAt:Date.now(), sessionId, groupName, status:'pending' });
  fs.writeFileSync(path.join(LINKS_DIR, `${id}.json`), JSON.stringify(savedLinks.get(id), null, 2));
  return id;
};
const notifyNewLink = async (sessionId, link, type) => {
  const intervals = sessionIntervals.get(sessionId) || { min:5, max:30 };
  const queue = joinQueue.get(sessionId) || [];
const positionInQueue = queue.length;
   
  let estimatedWaitTime = 0;
  if (positionInQueue > 0) {
    const avgInterval = Math.floor((intervals.min + intervals.max) / 2);
    estimatedWaitTime = positionInQueue * avgInterval;
  } else {
    estimatedWaitTime = Math.floor(Math.random()*(intervals.max-intervals.min)+intervals.min);
  }
  
  const icon = type === 'whatsapp' ? '📱' : '📲';
const queueText = positionInQueue > 0 ? `\n📋 Позиция в очереди: ${positionInQueue}` : '';
  
  // Создаем объект уведомления
  const notificationData = {
    type: 'new_link',
    sessionId,
    link,
    icon,
    estimatedWaitTime,
    positionInQueue,
    queueText: positionInQueue > 0 ? `Позиция в очереди: ${positionInQueue}` : '',
    message: `${icon} Найдена новая ссылка!\n\n🔗 ${link}\n⏱️ Примерное время ожидания: ${estimatedWaitTime} сек.${queueText}\n🤖 Сессия: ${sessionId}`,
    timestamp: Date.now(),
    id: Date.now() + Math.random(),
    displayTime: new Date().toLocaleString('ru-RU')
  };
  
  // Сохраняем в общую историю на сервере
  notificationsHistory.unshift(notificationData);
  if (notificationsHistory.length > 1000) {
    notificationsHistory.splice(1000); // Ограничиваем 1000 записями
  }
  
  // Send to WebSocket - отправляем всем подключенным пользователям веб-интерфейса
  if (typeof io !== 'undefined') {
    io.emit('notification', notificationData);
  }
  
  for (const [name, anchor] of infoAnchors.entries()) {
    if (name === sessionId) {
      try {
        await bot.telegram.sendMessage(anchor.chatId, 
          `${icon} Найдена новая ссылка!\n\n🔗 ${link}\n⏱️ Примерное время ожидания: ${estimatedWaitTime} сек.${queueText}\n🤖 Сессия: ${sessionId}`
        );
      } catch (e) {
        console.error(`[NOTIFY_ERROR] ${sessionId}:`, e.message);
      }
    }
  }
};
const addToJoinQueue = (sessionId, link, type) => { 
  if (!joinQueue.has(sessionId)) joinQueue.set(sessionId, []); 
  
  // Проверяем, нет ли уже такой ссылки в очереди
  const queue = joinQueue.get(sessionId);
  const exists = queue.some(item => item.link === link && !item.processing);
  if (exists) {
    console.log(`[QUEUE_DUPLICATE] ${sessionId}: ссылка ${link} уже в очереди, пропускаю`);
    return;
  }
  
  queue.push({ link, type, timestamp:Date.now(), attempts:0, processing:false });
  console.log(`[QUEUE_ADDED] ${sessionId}: добавлена ссылка ${link}, всего в очереди: ${queue.length}`);
  
  notifyNewLink(sessionId, link, type).catch(e => console.error(`[NOTIFY_ERROR] ${sessionId}:`, e.message));
  
  // Запускаем обработку очереди, если сессия активна
  if (sessionStatus.get(sessionId) === 'active') {
    const isProcessing = processingQueue.get(sessionId);
    if (!isProcessing) {
      console.log(`[QUEUE_START] ${sessionId}: запускаю обработку очереди`);
      setTimeout(() => processJoinQueue(sessionId), 1000);
    } else {
      console.log(`[QUEUE_BUSY] ${sessionId}: очередь уже обрабатывается, ссылка добавлена в очередь`);
    }
  } else {
    console.log(`[QUEUE_WAIT] ${sessionId}: сессия не активна (${sessionStatus.get(sessionId)}), ссылка добавлена в очередь`);
  }
};
const notifyJoinResult = async (sessionId, link, success, error = null) => {
  const icon = success ? '✅' : '❌';
  const statusText = success ? 'Успешно вступил!' : 'Ошибка вступления';
  const errorText = error ? `\n🚨 Ошибка: ${error}` : '';
  
  // Создаем объект уведомления
  const notificationData = {
    type: 'join_result',
    sessionId,
    link,
    success,
    error,
    icon,
    statusText,
    message: `${icon} ${statusText}\n\n🔗 ${link}\n🤖 Сессия: ${sessionId}${errorText}`,
    timestamp: Date.now(),
    id: Date.now() + Math.random(),
    displayTime: new Date().toLocaleString('ru-RU')
  };
  
  // Сохраняем в общую историю на сервере
  notificationsHistory.unshift(notificationData);
  if (notificationsHistory.length > 1000) {
    notificationsHistory.splice(1000); // Ограничиваем 1000 записями
  }
  
  // Send to WebSocket - отправляем всем подключенным пользователям веб-интерфейса
  if (typeof io !== 'undefined') {
    io.emit('notification', notificationData);
  }
  
  for (const [name, anchor] of infoAnchors.entries()) {
    if (name === sessionId) {
      try {
        await bot.telegram.sendMessage(anchor.chatId, 
          `${icon} ${statusText}\n\n🔗 ${link}\n🤖 Сессия: ${sessionId}${errorText}`
        );
      } catch (e) {
        console.error(`[NOTIFY_ERROR] ${sessionId}:`, e.message);
      }
    }
  }
};

const notifySessionCreated = async (sessionName, ctx = null, creatorInfo = null) => {
  const caption = `🟢 Сессия "${sessionName}" успешно создана и активна!`;
  console.log(`[NOTIFY_SESSION_CREATED] ${sessionName}: ctx=${!!ctx}, creatorInfo=${!!creatorInfo}, infoAnchors.size=${infoAnchors.size}`);
  
  // Создаем объект уведомления
  const notificationData = {
    type: 'session_created',
    sessionName,
    message: caption,
    timestamp: Date.now(),
    id: Date.now() + Math.random(),
    displayTime: new Date().toLocaleString('ru-RU')
  };
  
  // Сохраняем в общую историю на сервере
  notificationsHistory.unshift(notificationData);
  if (notificationsHistory.length > 1000) {
    notificationsHistory.splice(1000); // Ограничиваем 1000 записями
  }
  
  // Send to WebSocket
  if (typeof io !== 'undefined') {
    io.emit('notification', notificationData);
  }
   
  let notificationSent = false;
  if (ctx) {
    try {
      await ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: caption,
        ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
      });
notificationSent = true;
      console.log(`[NOTIFY_SESSION_CREATED] ${sessionName}: sent via ctx`);
    } catch (e) {
      console.error(`[SESSION_CREATED_NOTIFY_ERROR] ${sessionName}:`, e.message);
    }
  }
  if (creatorInfo && !notificationSent) {
    try {
      await bot.telegram.sendPhoto(creatorInfo.chatId || creatorInfo.userId, { source: './taygeta.png' }, {
        caption: caption,
        ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
      });
notificationSent = true;
      console.log(`[NOTIFY_SESSION_CREATED] ${sessionName}: sent via creatorInfo`);
    } catch (e) {
      console.error(`[SESSION_CREATED_NOTIFY_ERROR] ${sessionName}:`, e.message);
    }
  }
  for (const [name, anchor] of infoAnchors.entries()) {
    if (name === sessionName) {
      try {
        await bot.telegram.sendPhoto(anchor.chatId, { source: './taygeta.png' }, {
          caption: caption,
          ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
        });
notificationSent = true;
        console.log(`[NOTIFY_SESSION_CREATED] ${sessionName}: sent via infoAnchors`);
      } catch (e) {
        console.error(`[SESSION_CREATED_NOTIFY_ERROR] ${sessionName}:`, e.message);
      }
    }
  }
  if (!notificationSent) {
    for (const [userId, userState] of userData.entries()) {
      if (userState.data?.sessionName === sessionName || userState.action === 'session_created') {
        try {
          await bot.telegram.sendPhoto(userId, { source: './taygeta.png' }, {
            caption: caption,
            ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
          });
          notificationSent = true;
          console.log(`[NOTIFY_SESSION_CREATED] ${sessionName}: sent via userData`);
          break;
        } catch (e) {
          console.error(`[SESSION_CREATED_NOTIFY_ERROR] ${sessionName}:`, e.message);
        }
      }
    }
  }
  
console.log(`[NOTIFY_SESSION_CREATED] ${sessionName}: notificationSent=${notificationSent}`);
};
const processJoinQueue = async (sessionId) => {
  console.log(`[PROCESS_QUEUE_START] ${sessionId}: проверка очереди`);
  
  const sock = activeSocks.get(sessionId); 
  if (!sock) {
    console.log(`[PROCESS_QUEUE_STOP] ${sessionId}: сокет не найден`);
    processingQueue.set(sessionId, false);
    return;
  }
  
  const status = sessionStatus.get(sessionId);
  if (status !== 'active') {
    console.log(`[PROCESS_QUEUE_STOP] ${sessionId}: статус не активен (${status})`);
    processingQueue.set(sessionId, false);
    return;
  }
   
  const q = joinQueue.get(sessionId); 
  if (!q?.length) {
    console.log(`[PROCESS_QUEUE_EMPTY] ${sessionId}: очередь пуста`);
    processingQueue.set(sessionId, false);
    return;
  }
  
  // Проверяем, не обрабатывается ли уже очередь
  if (processingQueue.get(sessionId)) {
    console.log(`[PROCESS_QUEUE_BUSY] ${sessionId}: очередь уже обрабатывается`);
    return;
  }
  
  processingQueue.set(sessionId, true);
  console.log(`[PROCESS_QUEUE_PROCESSING] ${sessionId}: начинаю обработку, в очереди ${q.length} ссылок`);
   
  const intervals = sessionIntervals.get(sessionId) || { min:5, max:30 };
  const next = q.find(it=>!it.processing); 
  if (!next) {
    console.log(`[PROCESS_QUEUE_NO_NEXT] ${sessionId}: нет доступных ссылок для обработки`);
    processingQueue.set(sessionId, false);
    return;
  }
   
  next.processing = true;
  console.log(`[PROCESS_QUEUE_NEXT] ${sessionId}: обрабатываю ссылку ${next.link}, попытка ${next.attempts + 1}`);
   
  try{
    const delay = Math.floor(Math.random()*(intervals.max-intervals.min)+intervals.min);
    console.log(`[JOIN_DELAY] ${sessionId}: жду ${delay} сек перед вступлением в ${next.link}`);
    
    await new Promise(resolve => setTimeout(resolve, delay * 1000));
    
    // Проверяем статус после задержки
    if (!activeSocks.get(sessionId) || sessionStatus.get(sessionId)!=='active') {
      console.log(`[JOIN_CANCELLED] ${sessionId}: сессия стала неактивной во время ожидания`);
      next.processing = false;
      processingQueue.set(sessionId, false);
      return;
    }
    
    if (next.type==='whatsapp'){
      const code = next.link.replace('https://chat.whatsapp.com/','').split('/')[0].split('?')[0];
      console.log(`[JOIN_ATTEMPT] ${sessionId}: вступление в группу ${code}`);
      
      try {
        await sock.groupAcceptInvite(code);
        lastJoinInfo.set(sessionId, { groupId:code, groupName:'WhatsApp Group', timestamp:Date.now() });
        console.log(`[JOIN_SUCCESS] ${sessionId}: успешно вступил в группу ${code}`);
        await notifyJoinResult(sessionId, next.link, true);
        
        const i = q.indexOf(next); 
        if (i>-1) {
          q.splice(i,1);
          console.log(`[JOIN_REMOVED] ${sessionId}: ссылка удалена из очереди, осталось ${q.length}`);
        }
      } catch (joinError) {
        throw joinError; // Пробрасываем ошибку для обработки ниже
      }
    } else {
      console.log(`[JOIN_TG_PLACEHOLDER] ${sessionId}: ${next.link} (Telegram пока не поддерживается)`);
      const i = q.indexOf(next); 
      if (i>-1) {
        q.splice(i,1);
        console.log(`[JOIN_REMOVED] ${sessionId}: Telegram ссылка удалена, осталось ${q.length}`);
      }
    }
  }catch(e){
    console.error(`[JOIN_ERROR] ${sessionId}: ${next.link} - ${e.message}`);
    console.error(`[JOIN_ERROR_DETAILS] ${sessionId}:`, {
      error: e.message,
      code: e.code,
      statusCode: e.output?.statusCode,
      attempts: next.attempts + 1,
      stack: e.stack
    });
    
    await notifyJoinResult(sessionId, next.link, false, e.message);
     
    next.attempts++; 
    next.processing = false;
    
    if (e.message?.includes('already') || e.message?.includes('not found') || e.message?.includes('invalid') || e.message?.includes('bad-request') || e.message?.includes('forbidden')) {
      console.log(`[JOIN_REMOVE] ${sessionId}: удаление ссылки из-за ошибки: ${e.message}`);
      const i = q.indexOf(next); 
      if (i>-1) {
        q.splice(i,1);
        console.log(`[JOIN_REMOVED] ${sessionId}: проблемная ссылка удалена, осталось ${q.length}`);
      }
    } else if (next.attempts >= 3) {
      console.log(`[JOIN_REMOVE_MAX_ATTEMPTS] ${sessionId}: удаление после 3 попыток`);
      const i = q.indexOf(next); 
      if (i>-1) {
        q.splice(i,1);
        console.log(`[JOIN_REMOVED] ${sessionId}: ссылка удалена после 3 попыток, осталось ${q.length}`);
      }
    } else {
      console.log(`[JOIN_RETRY] ${sessionId}: будет повторная попытка (${next.attempts}/3)`);
    }
  }
  
  // Продолжаем обработку очереди, если есть еще ссылки
  if (q.length > 0) {
    console.log(`[PROCESS_QUEUE_CONTINUE] ${sessionId}: продолжаю обработку, осталось ${q.length} ссылок`);
    setTimeout(() => {
      processingQueue.set(sessionId, false); // Сбрасываем флаг перед следующей итерацией
      processJoinQueue(sessionId);
    }, 2000); // Небольшая задержка перед следующей ссылкой
  } else {
    console.log(`[PROCESS_QUEUE_DONE] ${sessionId}: очередь обработана полностью`);
    processingQueue.set(sessionId, false);
  }
};


const setupMessageTracking = (sock, sessionId) => {
  sock.ev.on('messages.upsert', async ({ messages, type }) => {
    if (type!=='notify') return;
    for (const m of messages) {
      try {
        const mc = m.message; if (!mc) continue;
        const text = mc.conversation || mc?.extendedTextMessage?.text || '';
        if (text){
          const links = extractLinks(text);
          for (const link of links){
            const id = saveLink(link, sessionId);
            if (id) addToJoinQueue(sessionId, link, link.includes('chat.whatsapp.com') ? 'whatsapp' : 'telegram');
          }
        }
      } catch (e) { console.error(`[MSG_TRACK_ERROR] ${sessionId}:`, e.message); }
    }
  });
};


const renderInfoText = (name) => {
  const lastJoin = lastJoinInfo.get(name);
  const qLen = joinQueue.get(name)?.length || 0;
  const itv = sessionIntervals.get(name) || { min:5, max:30 };
  let t = `${statusEmoji(sessionStatus.get(name)||'inactive')} Сессия: ${name}\n📊 Статус: ${sessionHuman(sessionStatus.get(name)||'inactive')}`;
  if (lastJoin){ const d = Math.floor((Date.now()-lastJoin.timestamp)/1000); t += `\n🔗 Последнее вступление: ${Math.floor(d/60)}м ${d%60}с назад`; }
  if (qLen>0) t += `\n⏳ Очередь вступлений: ${qLen}`;
  t += `\n⏱️ Интервалы: ${itv.min}-${itv.max} сек`;
  return t;
};
const updateInfoCard = async (name) => {
  const a = infoAnchors.get(name); if (!a) return;
  try{
    let msg;
    try {
      msg = await bot.telegram.getMessage(a.chatId, a.messageId);
    } catch (e) {
      console.log(`[INFO_CARD_MSG_NOT_FOUND] ${name}: removing anchor, message not found`);
      infoAnchors.delete(name);
return;
    }
    if (msg.photo) {
      const sent = await bot.telegram.sendMessage(a.chatId, renderInfoText(name), { ...sessionKeyboard(name, sessionStatus.get(name)||'inactive') });
      infoAnchors.set(name, { chatId: sent.chat.id, messageId: sent.message_id, lastUsed: Date.now() });
      try {
        await bot.telegram.deleteMessage(a.chatId, a.messageId);
      } catch (e) {
        console.log(`[INFO_CARD_DELETE_OLD] ${name}: could not delete old photo message`);
      }
    } else {
      await bot.telegram.editMessageText(a.chatId, a.messageId, undefined, renderInfoText(name), { ...sessionKeyboard(name, sessionStatus.get(name)||'inactive') });
      a.lastUsed = Date.now();
    }
  } catch(e){ 
    if (!/message is not modified/i.test(e.message) && !/there is no text in the message to edit/i.test(e.message)) {
      console.error('[INFO_CARD_UPDATE_ERROR]', { name, error:e.message }); 
    }
  }
};

const cbList    = (n,p)=>`grp:list:${n}:${p}`;
const cbRefresh = (n,p)=>`grp:refresh:${n}:${p}`;
const cbLinksList = (p)=>`links:list:${p}`;
const cbNoop    = 'noop';

const groupsKeyboard = (name, page, pages) => {
  const rows = [];
  const window=2, maxNums=9;
  const nums = new Set([1,pages,page]); for(let i=1;i<=window;i++){ nums.add(page-i); nums.add(page+i); }
  const arr = [...nums].filter(p=>p>=1&&p<=pages).sort((a,b)=>a-b);
  const numericRow = []; let prev=0;
  for (const p of arr){ if (p-prev>1) numericRow.push(Markup.button.callback('…', cbNoop)); numericRow.push(Markup.button.callback(p===page?`·${p}·`:`${p}`, cbList(name,p))); prev=p; if (numericRow.length>=maxNums) break; }
  if (numericRow.length) rows.push(numericRow);
  const arrows=[]; if (page>1){ arrows.push(Markup.button.callback('⏮ 1', cbList(name,1))); arrows.push(Markup.button.callback('⬅️', cbList(name,page-1))); }
  if (page<pages){ arrows.push(Markup.button.callback('➡️', cbList(name,page+1))); arrows.push(Markup.button.callback(`${pages} ⏭`, cbList(name,pages))); }
  if (arrows.length) rows.push(arrows);
  rows.push([Markup.button.callback('🔄 Обновить', cbRefresh(name,page))]);
  rows.push([Markup.button.callback('⬅️ К сессии', `info_${name}`)]);
  return Markup.inlineKeyboard(rows);
};

const linksKeyboard = (page, pages) => {
  const rows = [];
  const window=2, maxNums=9;
  const nums = new Set([1,pages,page]); for(let i=1;i<=window;i++){ nums.add(page-i); nums.add(page+i); }
  const arr = [...nums].filter(p=>p>=1&&p<=pages).sort((a,b)=>a-b);
  const numericRow = []; let prev=0;
  for (const p of arr){ if (p-prev>1) numericRow.push(Markup.button.callback('…', cbNoop)); numericRow.push(Markup.button.callback(p===page?`·${p}·`:`${p}`, cbLinksList(p))); prev=p; if (numericRow.length>=maxNums) break; }
  if (numericRow.length) rows.push(numericRow);
  const arrows=[]; if (page>1){ arrows.push(Markup.button.callback('⏮ 1', cbLinksList(1))); arrows.push(Markup.button.callback('⬅️', cbLinksList(page-1))); }
  if (page<pages){ arrows.push(Markup.button.callback('➡️', cbLinksList(page+1))); arrows.push(Markup.button.callback(`${pages} ⏭`, cbLinksList(pages))); }
  if (arrows.length) rows.push(arrows);
  rows.push([Markup.button.callback('📋 Копировать ссылки', 'links_copy')]);
  rows.push([Markup.button.callback('🗑️ Очистить старые', 'links_cleanup')]);
  rows.push([Markup.button.callback('⬅️ В меню', 'start')]);
  return Markup.inlineKeyboard(rows);
};
const withInvisibleFlip = (msgId, text) => { const prev = renderToggle.get(msgId)?.value||false; renderToggle.set(msgId,{value:!prev,timestamp:Date.now()}); return text + (prev?'\u2060':'\u2061'); };
const fetchGroups = async (name) => {
  const c = groupsCache.get(name); if (c && (Date.now()-c.ts)<GROUPS_CACHE_TTL) return c.list;
  const sock = activeSocks.get(name); if (!sock) throw new Error('Сессия не активна');
  const obj = await sock.groupFetchAllParticipating();
  const list = Object.values(obj||{}).map(g=>({ id:g.id||g.jid, subject:g.subject||'(без названия)', size:Array.isArray(g.participants)?g.participants.length:(g.size||0) })).sort((a,b)=>a.subject.localeCompare(b.subject,'ru'));
  groupsCache.set(name,{list,ts:Date.now()}); return list;
};
const renderGroups = async (ctx, name, pageReq) => {
  if (sessionStatus.get(name)!=='active'){
    return ctx.editMessageText(`⚠️ Сессия «${name}» не активна.`, { ...Markup.inlineKeyboard([[Markup.button.callback('🔄 Активировать', `activate_${name}`)],[Markup.button.callback('⬅️ Назад', `info_${name}`)]])}).catch(()=>{});
  }
  try{
    const groups = await fetchGroups(name);
    const { slice, page, pages, total } = paginate(groups, pageReq, GROUPS_PAGE_SIZE);
    const body = slice.length ? slice.map((g,i)=>`${(page-1)*GROUPS_PAGE_SIZE+i+1}. ${g.subject} · 👥 ${g.size}`).join('\n') : 'Нет групп, где аккаунт состоит.';
    const msgId = ctx.callbackQuery?.message?.message_id;
    const textRaw = `👥 Группы сессии «${name}»\nВсего: ${total}\nСтр. ${page}/${pages}\n\n${body}`;
    const text = msgId ? withInvisibleFlip(msgId, textRaw) : textRaw;
    await ctx.editMessageText(text, { ...groupsKeyboard(name, page, pages) }).catch(async (e)=>{ const d = e?.description||e?.message||''; if (/message is not modified/i.test(d)){ try{ await ctx.answerCbQuery('Без изменений'); }catch{} } else throw e; });
  }catch(e){
    await ctx.editMessageText(`❌ Не удалось получить группы: ${e.message}`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]]) }).catch(()=>{});
  }
};

const renderLinks = async (ctx, pageReq = 1) => {
  try{
    console.log(`[renderLinks] savedLinks.size=${savedLinks.size}`);
    
    const arr = Array.from(savedLinks.entries()).sort((a,b) => b[1].addedAt - a[1].addedAt);
    console.log(`[renderLinks] Sorted array length: ${arr.length}`);
    
    if (!arr || arr.length === 0) {
      console.log('[renderLinks] No links found, showing empty message');
      return ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: '🔗 Ссылок нет', 
        ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
      });
    }
    
    const { slice, page, pages, total } = paginate(arr, pageReq, LINKS_PAGE_SIZE);
    console.log(`[renderLinks] Pagination: page=${page}, pages=${pages}, total=${total}, slice.length=${slice.length}`);
    
    const body = slice.length ? slice.map(([id, d], i) => {
      const added = new Date(d.addedAt).toLocaleString('ru-RU');
      const icon = d.type === 'whatsapp' ? '📱' : '📲';
      const short = d.url.length > 40 ? d.url.slice(0, 40) + '…' : d.url;
      return `${(page-1)*LINKS_PAGE_SIZE+i+1}. ${icon} ${short}\n   🤖 ${d.sessionId}\n   📅 ${added}\n   📋 ${d.status}`;
    }).join('\n\n') : 'Нет ссылок на этой странице.';
    
    const textRaw = `🔗 ССЫЛКИ (${total})\nСтр. ${page}/${pages}\n\n${body}`;
    
    console.log('[renderLinks] Sending new message with links');
    try {
      await ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: textRaw,
        ...linksKeyboard(page, pages)
      });
    } catch (photoError) {
      console.error('[renderLinks] Photo send failed:', photoError.message);
      // Если фото не отправляется, отправляем просто текст
      await ctx.reply(textRaw, { ...linksKeyboard(page, pages) });
    }
    
  }catch(e){
    console.error('[renderLinks] Error:', e.message);
    await ctx.replyWithPhoto({ source: './taygeta.png' }, {
      caption: `❌ Не удалось загрузить ссылки: ${e.message}`, 
      ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]]) 
    });
  }
};


const clearQrTimer = async (name, finalCaption, { deleteAfterMs, sendQrToCtx } = {}) => {
  const t = qrTimers.get(name) || qrAnchors.get(name); if (!t) return;
try{ if (t.intervalId) clearInterval(t.intervalId); }catch{}
  if (finalCaption){ try{ await bot.telegram.editMessageCaption(t.chatId, t.messageId, undefined, finalCaption); }catch{} }
  try{
    await bot.telegram.deleteMessage(t.chatId, t.messageId);
    if (finalCaption && /QR принят|Синхронизац/i.test(finalCaption)) {
      const chatId = sendQrToCtx?.chat?.id || sendQrToCtx?.from?.id || t.chatId;
      if (chatId) {
        try {
          await bot.telegram.sendPhoto(chatId, { source: './taygeta.png' }, {
            caption: `🟢 Сессия "${name}" успешно создана и активна!`,
            ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
          });
          console.log(`[SESSION_CREATED_ON_QR_CLEAR] ${name}: notification sent when QR cleared`);
        } catch (e) {
          console.error(`[SESSION_CREATED_ON_QR_CLEAR_ERROR] ${name}:`, e.message);
        }
      }
    }
    
    if (sendQrToCtx?.telegram){
      const sessions = listSessionDirs();
      const chatId = sendQrToCtx.chat?.id || sendQrToCtx.from?.id;
      if (chatId){
        const kb = sessions.length
          ? Markup.inlineKeyboard([...sessions.map(s=>[Markup.button.callback(`${statusEmoji(sessionStatus.get(s)||'inactive')} ${s}`, `info_${s}`)]), [Markup.button.callback('⬅️ Назад', 'start')]])
          : Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]]);
        if (sessions.length) {
          setTimeout(async () => {
            await sendQrToCtx.telegram.sendPhoto(chatId, { source: './taygeta.png' }, {
              caption: '📋 Выберите сессию:',
              ...kb
            });
          }, 2000); // Показываем меню через 2 секунды
        } else {
          await sendQrToCtx.telegram.sendMessage(chatId, '❌ Нет сессий', kb);
        }
      }
    }
  }catch{
    if (typeof deleteAfterMs==='number'){ setTimeout(()=>{ bot.telegram.deleteMessage(t.chatId, t.messageId).catch(()=>{}); }, deleteAfterMs); }
  }
  qrTimers.delete(name); qrAnchors.delete(name);
};


const connectSocket = async (name, opts = {}) => {
  console.log(`[CONNECT_SOCKET] ${name}: opts=${Object.keys(opts)}`);
  const { sessionPath } = safeSessionPath(name);
  let existedBefore = fs.existsSync(sessionPath);

  if (existedBefore && opts.forceNewQR) { try{ fs.rmSync(sessionPath, { recursive:true, force:true }); }catch{} existedBefore = false; }
  if (!existedBefore && (opts.createIfMissing || opts.forceNewQR)) fs.mkdirSync(sessionPath, { recursive:true });
  if (!fs.existsSync(sessionPath)) throw new Error('Папка сессии не существует');

  if (activeSocks.has(name)) { try{ activeSocks.get(name).end(); }catch{} activeSocks.delete(name); }

  const { state: authState, saveCreds } = await useMultiFileAuthState(sessionPath);
  const { version } = await fetchLatestBaileysVersion().catch(()=>({ version: undefined }));
  setStatus(name, 'syncing');

  let connectionHandled = false;
  let pairingShown = false;

  const sock = makeWASocket({
    version,               // критично для 405
    auth: authState,
    printQRInTerminal: false,
    connectTimeoutMs: 60_000,
    browser: Browsers.macOS('Chrome', `Session-${name}-${Date.now()}`),
    markOnlineOnConnect: false,
    retryRequestDelayMs: 5000,
    maxMsgRetryCount: 3,
  });
  activeSocks.set(name, sock);
  sock.ev.on('creds.update', async () => { await saveCreds(); console.log(`[CREDS_UPDATE] ${name}: registered=${!!authState.creds?.registered}`); });
  setupMessageTracking(sock, name);
  const tryShowPairingCode = async () => {
    if (pairingShown) return;
    if (!WA_PHONE) return;
    try {
      pairingShown = true;
setStatus(name, 'qr');
      if (opts.creatingMessageId) {
        try{ await opts.sendQrToCtx.deleteMessage(opts.creatingMessageId); }catch(e){ console.error('[DELETE_CREATING_MESSAGE_ERROR]', e.message); }
      }
      
      const code = await sock.requestPairingCode(WA_PHONE); // например "123-456"
      const timeoutSec = Math.max(10, opts.qrTimeoutSec ?? 60);
      
      // Send pairing code to WebSocket
      if (typeof io !== 'undefined') {
        io.emit('pairing_code', { name, code, timeout: timeoutSec });
      }
      
      const sent = await opts.sendQrToCtx?.replyWithHTML(
        `🔢 Пэйринг-код для <b>${name}</b>: <b>${code}</b>\n`+
        `⏳ Осталось: ${timeoutSec} c\n\n`+
        `📱 На телефоне: WhatsApp → Связанные устройства → Привязать устройство → <b>Ввести код</b>`,
      );
      if (sent){
        const startedAt = Date.now();
        const intervalId = setInterval(async ()=>{
          const left = timeoutSec - Math.floor((Date.now()-startedAt)/1000);
          if (left>0){ try{ await bot.telegram.editMessageText(sent.chat.id, sent.message_id, undefined,
              `🔢 Пэйринг-код: ${code}\n⏳ Осталось: ${left} c\n\n📱 Откройте на телефоне: Связанные устройства → Ввести код`,
            ); }catch{} }
          else {
            clearInterval(intervalId);
            qrTimers.delete(name); qrAnchors.delete(name);
            try{ await bot.telegram.editMessageText(sent.chat.id, sent.message_id, undefined, '⏰ Время вышло, запросите новый код/QR'); }catch{}
            try{ sock.end(); }catch{}
            activeSocks.delete(name);
            if (!existedBefore){ try{ fs.rmSync(sessionPath, { recursive:true, force:true }); }catch{} }
            setStatus(name, 'inactive');
          }
        }, 1000);
        qrTimers.set(name, { intervalId, chatId: sent.chat.id, messageId: sent.message_id, startedAt, timeoutSec });
        qrAnchors.set(name, { chatId: sent.chat.id, messageId: sent.message_id });
      }
    } catch (e) {
      console.error(`[PAIRING_CODE_ERROR] ${name}: ${e.message}`);
    }
  };
  sock.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect, qr } = update;
    console.log(`[CONNECTION_UPDATE] ${name}: connection=${connection} qr=${!!qr}`);
    if (qr) {
      await clearQrTimer(name).catch(()=>{});
      setStatus(name, 'qr');
      
      // Send QR to Telegram if context available
      if (opts.sendQrToCtx?.replyWithPhoto) {
        if (opts.creatingMessageId) {
          try{ await opts.sendQrToCtx.deleteMessage(opts.creatingMessageId); }catch(e){ console.error('[DELETE_CREATING_MESSAGE_ERROR]', e.message); }
        }
        
        try {
          const png = await qrcode.toBuffer(qr, { width: 512 });
          const timeoutSec = Math.max(10, opts.qrTimeoutSec ?? 60);
          const sent = await opts.sendQrToCtx.replyWithPhoto({ source: png }, { caption: `📱 Сканируйте QR (осталось ${timeoutSec} c)` });
          const startedAt = Date.now();
          const intervalId = setInterval(async ()=>{
            const left = timeoutSec - Math.floor((Date.now()-startedAt)/1000);
            if (left>0){ try{ await bot.telegram.editMessageCaption(sent.chat.id, sent.message_id, undefined, `📱 Сканируйте QR (осталось ${left} c)`); }catch{} }
            else {
              clearInterval(intervalId);
              qrTimers.delete(name); qrAnchors.delete(name);
              try{ await bot.telegram.editMessageCaption(sent.chat.id, sent.message_id, undefined, '⏰ Время вышло'); }catch{}
              try{ sock.end(); }catch{}
              activeSocks.delete(name);
              if (!existedBefore){ try{ fs.rmSync(sessionPath, { recursive:true, force:true }); }catch{} }
              setStatus(name, 'inactive');
            }
          }, 1000);
          qrTimers.set(name, { intervalId, chatId: sent.chat.id, messageId: sent.message_id, startedAt, timeoutSec });
          qrAnchors.set(name, { chatId: sent.chat.id, messageId: sent.message_id });
        } catch (e) { console.error(`[QR_SEND_ERROR] ${name}: ${e.message}`); }
      }
      
      // Send QR to WebSocket for web interface
      try {
        const qrDataUrl = await qrcode.toDataURL(qr, { width: 512 });
        if (typeof io !== 'undefined') {
          io.emit('qr_code', { name, qr: qrDataUrl, timeout: opts.qrTimeoutSec ?? 60 });
        }
      } catch (e) {
        console.error(`[QR_WS_ERROR] ${name}: ${e.message}`);
      }
        const checkConnection = setInterval(async () => {
          if (sessionStatus.get(name) === 'active' && !connectionHandled) {
            clearInterval(checkConnection);
            console.log(`[QR_DISAPPEARED_DETECTED] ${name}: QR disappeared and session is active`);
            
            const chatId = opts.sendQrToCtx?.chat?.id || opts.sendQrToCtx?.from?.id;
            if (chatId && opts.sendQrToCtx) {
              try {
                await opts.sendQrToCtx.replyWithPhoto({ source: './taygeta.png' }, {
                  caption: `🟢 Сессия "${name}" успешно создана и активна!`,
                  ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
                });
                console.log(`[SESSION_CREATED_ON_QR_DISAPPEAR] ${name}: notification sent`);
              } catch (e) {
                console.error(`[SESSION_CREATED_ON_QR_DISAPPEAR_ERROR] ${name}:`, e.message);
              }
            }
          }
}, 500);
        setTimeout(() => clearInterval(checkConnection), 30000);
    }
    if (connection === 'connecting' && !qr && WA_PHONE && !pairingShown) {
      setTimeout(()=>{ tryShowPairingCode(); }, 1200);
    }

    if (connection === 'open' && !connectionHandled) {
connectionHandled = true;
      console.log(`[CONNECTION_OPEN_SUCCESS] ${name}: connection opened successfully`);
      const chatId = opts.sendQrToCtx?.chat?.id || opts.sendQrToCtx?.from?.id;
      console.log(`[CONNECTION_OPEN] ${name}: chatId=${chatId}, sendQrToCtx=${!!opts.sendQrToCtx}`);
      
      if (chatId && opts.sendQrToCtx) {
        try {
          await opts.sendQrToCtx.replyWithPhoto({ source: './taygeta.png' }, {
            caption: `🟢 Сессия "${name}" успешно создана и активна!`,
            ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
          });
          console.log(`[SESSION_CREATED_IMMEDIATE] ${name}: notification sent immediately`);
        } catch (e) {
          console.error(`[SESSION_CREATED_IMMEDIATE_ERROR] ${name}:`, e.message);
        }
      } else {
        console.log(`[CONNECTION_OPEN] ${name}: no chatId or sendQrToCtx available`);
      }
      
      await clearQrTimer(name, '🔄 QR принят, выполняется синхронизация…', { deleteAfterMs: 800, sendQrToCtx: opts.sendQrToCtx }).catch(()=>{});
      setStatus(name, 'active');
      setTimeout(async ()=>{
        if (chatId) {
          const sessions = listSessionDirs();
          const kb = sessions.length
            ? Markup.inlineKeyboard([...sessions.map(s=>[Markup.button.callback(`${statusEmoji(sessionStatus.get(s)||'inactive')} ${s}`, `info_${s}`)]), [Markup.button.callback('⬅️ Назад', 'start')]])
            : Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]]);
          if (sessions.length) {
            await bot.telegram.sendPhoto(chatId, { source: './taygeta.png' }, {
              caption: '📋 Выберите сессию:',
              ...kb
            });
          } else {
            await bot.telegram.sendMessage(chatId, '❌ Нет сессий', kb);
          }
        }
      }, 2000);

      // Запускаем обработку очереди после активации сессии
      setTimeout(() => {
        const q = joinQueue.get(name);
        if (q?.length > 0) {
          console.log(`[SESSION_ACTIVATED] ${name}: запускаю обработку очереди (${q.length} ссылок)`);
          processJoinQueue(name);
        }
      }, 5000);
    }

    if (connection === 'close') {
      const err  = lastDisconnect?.error;
      const code = err?.output?.statusCode ?? err?.status ?? err?.code ?? err?.data?.statusCode ?? null;
      const loggedOut = code === DisconnectReason.loggedOut;
      const isConflict = code === 440 || err?.message?.includes('conflict');
      console.log(`[CONNECTION_CLOSE] ${name}: code=${code} loggedOut=${loggedOut} conflict=${isConflict}`);

      activeSocks.delete(name);
      await clearQrTimer(name).catch(()=>{});
      connectionHandled = false;
      pairingShown = false;

      if (loggedOut) {
        setStatus(name, 'inactive');
      } else if (isConflict) {
        console.log(`[CONFLICT] ${name}: waiting before reconnect...`);
        setStatus(name, 'syncing');
        // При конфликте ждем дольше перед переподключением
        setTimeout(()=>{ connectSocket(name).catch(e=>{ setStatus(name,'error'); console.error('[RECONNECT]', e?.message||e); }); }, 30_000);
      } else {
        setStatus(name, 'syncing');
        setTimeout(()=>{ connectSocket(name).catch(e=>{ setStatus(name,'error'); console.error('[RECONNECT]', e?.message||e); }); }, 10_000);
      }
    }
  });

  return sock;
};


const loadSavedLinks = () => {
  try {
    const files = fs.readdirSync(LINKS_DIR).filter(f=>f.endsWith('.json'));
    console.log(`[loadSavedLinks] Found ${files.length} JSON files in ${LINKS_DIR}`);
    files.forEach(f=>{ 
      try{ 
        const data = JSON.parse(fs.readFileSync(path.join(LINKS_DIR,f),'utf8')); 
        const id = f.replace('.json','');
        savedLinks.set(id, data); 
        console.log(`[loadSavedLinks] Loaded link ${id}: ${data.url}`);
      }catch(e){ 
        console.error('[LINK_LOAD_ERROR]', f, e.message); 
      }
    });
    console.log(`[LINKS_LOADED] ${savedLinks.size} links total`);
  } catch (e) { 
    console.error('[LINKS_DIR_ERROR]', e.message); 
  }
};
const loadExistingSessions = async () => { for (const name of listSessionDirs()){ try{ setStatus(name, 'inactive'); }catch(e){ setStatus(name,'error'); console.log(`Failed to load ${name}:`, e.message);} } };

const sendMainMenu = (ctx) => ctx.replyWithPhoto({ source: './taygeta.png' }, {
  caption: 'Выберите действие:',
  ...Markup.inlineKeyboard([
    [Markup.button.callback('📋 Выбрать сессию', 'select')],
    [Markup.button.callback('➕ Добавить сессию', 'add')],
    [Markup.button.callback('🔗 Сохраненные ссылки', 'links')],
    [Markup.button.callback('👥 Пользователи', 'users')],
    [Markup.button.callback('🗑️ Удалить сессию', 'del')],
  ])
});

bot.start(async (ctx)=>{ addUser(ctx.from.id,{ username:ctx.from.username, firstName:ctx.from.first_name, lastName:ctx.from.last_name }); return sendMainMenu(ctx); });
bot.action('start', async (ctx)=>{ try{ await ctx.answerCbQuery(); }catch{} return sendMainMenu(ctx); });

bot.action('select', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const sessions = listSessionDirs();
  if (!sessions.length) return ctx.reply('❌ Нет сессий');
  const kb = sessions.map(n=>[Markup.button.callback(`${statusEmoji(sessionStatus.get(n)||'inactive')} ${n}`, `info_${n}`)]);
  kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: '📋 Выберите сессию:',
    ...Markup.inlineKeyboard(kb)
  });
});

bot.action('add', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const msg = await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: '➕ ДОБАВИТЬ СЕССИЮ\n\n📝 Введите имя сессии:\n• Только буквы, цифры, _ -\n• 2-64 символа\n• Пример: MySession, worker_1',
    ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'start')]])
  });
  userData.set(ctx.from.id, { action:'add_name', data:{}, timestamp:Date.now(), messageId: msg.message_id });
});

const sessionKeyboard = (name, st) => {
  const rows = [];
  if (st!=='active' && st!=='syncing'){ 
    rows.push([Markup.button.callback('🔄 Активировать', `activate_${name}`)]); 
    rows.push([Markup.button.callback('🔁 Новый QR/код', `newqr_${name}`)]); 
  }
  rows.push([Markup.button.callback('👥 Просмотреть группы', cbList(name,1))]);
  rows.push([Markup.button.callback('⏱️ Интервалы', `intervals_${name}`)]);
  rows.push([Markup.button.callback('⬅️ Назад', 'select')]);
  return Markup.inlineKeyboard(rows);
};

bot.action(/info_(.+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  const msg = await ctx.editMessageText(renderInfoText(name), { ...sessionKeyboard(name, sessionStatus.get(name)||'inactive') }).catch(async ()=> ctx.reply(renderInfoText(name), sessionKeyboard(name, sessionStatus.get(name)||'inactive')));
  const chatId = ctx.chat?.id ?? ctx.callbackQuery?.message?.chat?.id;
  const messageId = (ctx.callbackQuery?.message?.message_id) ?? msg?.message_id;
  if (chatId && messageId) infoAnchors.set(name, { chatId, messageId, lastUsed:Date.now() });
});

bot.action(/activate_(.+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  let sessionPath; try{ ({ sessionPath } = safeSessionPath(name)); }catch{ return ctx.editMessageText(`❌ Некорректное имя сессии`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад','select')]])}); }
  if (!fs.existsSync(sessionPath)) return ctx.editMessageText(`❌ Сессии «${name}» не существует`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад','select')]])});
  setStatus(name,'syncing');
  await ctx.editMessageText(`🔄 Активация сессии ${name}…`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]]) });
  try{ await connectSocket(name, { sendQrToCtx: ctx, qrTimeoutSec: QR_TIMEOUT_ACTIVATE, createIfMissing:true }); }
  catch(err){ setStatus(name,'error'); await ctx.editMessageText(`❌ Ошибка активации: ${err.message}`, Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)] ])); }
});

bot.action(/newqr_(.+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  setStatus(name,'syncing');
  await ctx.editMessageText(`🔁 Генерация нового QR/кода для ${name}…`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]]) });
  try{ await connectSocket(name, { sendQrToCtx: ctx, qrTimeoutSec: QR_TIMEOUT_ACTIVATE, createIfMissing:true, forceNewQR:true }); }
  catch(err){ setStatus(name,'error'); await ctx.editMessageText(`❌ Ошибка: ${err.message}`, Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)] ])); }
});

bot.action('del', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const sessions = listSessionDirs();
  if (!sessions.length) return ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: '❌ Нет сессий',
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
  });
  const kb = sessions.map(n=>[Markup.button.callback(`🗑️ Удалить ${n}`, `del_${n}`)]);
  kb.push([Markup.button.callback('⬅️ Назад', 'start')]);
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: '🗑️ Выберите для удаления:',
    ...Markup.inlineKeyboard(kb)
  });
});
bot.action(/del_(.+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  let sessionPath; try{ ({ sessionPath } = safeSessionPath(name)); }catch{ return ctx.editMessageText(`❌ Некорректное имя сессии`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад','select')]])}); }
  
  let logoutSuccess = false;
  
  // Если сессия активна, выходим из устройства
  if (activeSocks.has(name)){ 
    try{ 
      const sock = activeSocks.get(name); 
      await ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: `🔄 Выходим из устройства «${name}»…`,
        ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]])
      }); 
      try{ 
        await sock.logout(); 
        console.log(`[LOGOUT_SUCCESS] ${name}: успешно вышли из устройства`);
        logoutSuccess = true;
      }catch(e){ 
        console.error('[LOGOUT_ERROR]', { name, error:e.message }); 
      } 
      sock.end(); 
    }catch(e){ 
      console.error('[SOCKET_CLOSE_ERROR]', { name, error:e.message }); 
    } 
    activeSocks.delete(name); 
  } else {
    // Если сессия неактивна, пытаемся подключиться для выхода
    try {
      await ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: `🔄 Подключаемся для выхода из устройства «${name}»…`,
        ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]])
      });
      
      const { state: authState, saveCreds } = await useMultiFileAuthState(sessionPath);
      const { version } = await fetchLatestBaileysVersion().catch(()=>({ version: undefined }));
      
      const sock = makeWASocket({
        version,
        auth: authState,
        printQRInTerminal: false,
        connectTimeoutMs: 30_000,
        browser: Browsers.macOS('Chrome', `Session-${name}-${Date.now()}`),
        markOnlineOnConnect: false,
      });
      
      // Ждем подключения
      await new Promise((resolve, reject) => {
        const timeout = setTimeout(() => reject(new Error('Timeout')), 25000);
        
        sock.ev.on('connection.update', (update) => {
          if (update.connection === 'open') {
            clearTimeout(timeout);
            resolve();
          } else if (update.connection === 'close') {
            clearTimeout(timeout);
            reject(new Error('Connection closed'));
          }
        });
      });
      
      // Выходим из устройства
      await sock.logout();
      console.log(`[LOGOUT_SUCCESS] ${name}: успешно вышли из устройства после подключения`);
      logoutSuccess = true;
      sock.end();
      
    } catch (e) {
      console.error('[TEMP_CONNECT_LOGOUT_ERROR]', { name, error: e.message });
      
      // Спрашиваем пользователя, продолжать ли удаление
      await ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: `⚠️ Не удалось подключиться к сессии «${name}» для выхода из устройства.\n\nВозможные причины:\n• Сессия уже неактивна в WhatsApp\n• Проблемы с сетью\n• Устройство уже отключено\n\nУдалить сессию anyway?`,
        ...Markup.inlineKeyboard([
          [Markup.button.callback('🗑️ Удалить anyway', `force_del_${name}`)],
          [Markup.button.callback('❌ Отмена', 'select')]
        ])
      });
      return; // Не продолжаем удаление
    }
  }
  
  await clearQrTimer(name);
  sessionStatus.delete(name); groupsCache.delete(name); infoAnchors.delete(name); joinQueue.delete(name); sessionIntervals.delete(name); lastJoinInfo.delete(name); processingQueue.delete(name);
  try{ fs.rmSync(sessionPath, { recursive:true, force:true }); }catch(e){ console.error('[SESSION_DELETE_ERROR]', { name, error:e.message }); }
  
  const statusText = logoutSuccess ? '✅ Удалена сессия и вышли из устройства' : '✅ Удалена сессия';
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: `${statusText}: ${name}`,
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
  }).catch(()=>{});
});

// Обработчик принудительного удаления
bot.action(/force_del_(.+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  let sessionPath; try{ ({ sessionPath } = safeSessionPath(name)); }catch{ return ctx.editMessageText(`❌ Некорректное имя сессии`, { ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад','select')]])}); }
  
  await clearQrTimer(name);
  sessionStatus.delete(name); groupsCache.delete(name); infoAnchors.delete(name); joinQueue.delete(name); sessionIntervals.delete(name); lastJoinInfo.delete(name); processingQueue.delete(name);
  try{ fs.rmSync(sessionPath, { recursive:true, force:true }); }catch(e){ console.error('[SESSION_DELETE_ERROR]', { name, error:e.message }); }
  
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: `✅ Принудительно удалена сессия: ${name}\n\n⚠️ Выход из устройства не был выполнен`,
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню', 'start')]])
  }).catch(()=>{});
});

// Интервалы
bot.action(/intervals_(.+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  const cur = sessionIntervals.get(name) || { min:5, max:30 };
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: `⏱️ НАСТРОЙКА ИНТЕРВАЛОВ\n\n🤖 Сессия: ${name}\n📊 Текущие интервалы: ${cur.min}-${cur.max} сек.\n\n⚙️ Выберите:`,
    ...Markup.inlineKeyboard([
      [Markup.button.callback('5-30', `interval_set_${name}_5_30`)],
      [Markup.button.callback('30-60', `interval_set_${name}_30_60`)],
      [Markup.button.callback('60-180', `interval_set_${name}_60_180`)],
      [Markup.button.callback('180-600', `interval_set_${name}_180_600`)],
      [Markup.button.callback('🔧 Свои', `interval_custom_${name}`)],
      [Markup.button.callback('⬅️ Назад', `info_${name}`)],
    ])
  });
});
bot.action(/interval_set_(.+)_(\d+)_(\d+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1], min = parseInt(ctx.match[2]), max = parseInt(ctx.match[3]);
  if (isNaN(min) || isNaN(max) || min < 3 || max < min || max > 3600) {
    return ctx.replyWithPhoto({ source: './taygeta.png' }, {
      caption: `❌ Некорректные интервалы\n\n🤖 Сессия: ${name}\n⏱️ Требуется: 3-3600 сек, min ≤ max`,
      ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `intervals_${name}`)]])
    });
  }
  sessionIntervals.set(name, { min, max });
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
      caption: `✅ Интервалы обновлены\n\n🤖 Сессия: ${name}\n⏱️ Новые: ${min}-${max} сек.`,
      ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${name}`)]])
    });
});
bot.action(/interval_custom_(.+)/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  userData.set(ctx.from.id, { action:'interval_custom', data:{ sessionName:name }, timestamp:Date.now() });
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: `🔧 СВОИ ИНТЕРВАЛЫ\n\n🤖 Сессия: ${name}\n\n⏱️ Введите формат:\n• 10-30\n• 5\n• 120-300сек`,
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `intervals_${name}`)]])
  });
});
function parseCustomInterval(text){
  const clean = text.toLowerCase().replace(/\s+/g,'');
  const range = clean.match(/^(\d+)-(\d+)(сек)?$/); if (range){ const min=+range[1], max=+range[2]; if (min>=3 && max>=min && max<=3600) return {min,max}; }
  const single = clean.match(/^(\d+)(сек)?$/); if (single){ const v=+single[1]; if (v>=3 && v<=3600) return {min:v,max:v}; }
  return null;
}
bot.on('text', async (ctx)=>{
  addUser(ctx.from.id, { username:ctx.from.username, firstName:ctx.from.first_name, lastName:ctx.from.last_name });
  const state = userData.get(ctx.from.id); if (!state) return;
  if (state.action==='add_name'){
    const name = canonizeName(ctx.message.text);
    const { sessionPath } = safeSessionPath(name);
    
    // Удаляем предыдущее сообщение
    if (state.messageId) {
      try{ await ctx.deleteMessage(state.messageId); }catch(e){ console.error('[DELETE_MESSAGE_ERROR]', e.message); }
    }
    
    if (fs.existsSync(sessionPath)) {
      const msg = await ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: '❌ Такая сессия уже существует. Введите другое имя.',
        ...Markup.inlineKeyboard([[Markup.button.callback('❌ Отмена', 'start')]])
      });
      userData.set(ctx.from.id, { action:'add_name', data:{}, timestamp:Date.now(), messageId: msg.message_id });
      return;
    }
    
    const creatingMsg = await ctx.replyWithPhoto({ source: './taygeta.png' }, {
      caption: `🆕 Создаю сессию «${name}»…`,
      ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
    });
    
    userData.delete(ctx.from.id);
    setStatus(name,'syncing');
    
    try{ 
      await connectSocket(name, { 
        sendQrToCtx: ctx, 
        qrTimeoutSec: QR_TIMEOUT_CREATE, 
        createIfMissing:true,
        creatingMessageId: creatingMsg.message_id // Передаем ID сообщения для удаления
      }); 
    } catch(err){ 
      setStatus(name,'error'); 
      try{ await ctx.deleteMessage(creatingMsg.message_id); }catch(e){ console.error('[DELETE_MESSAGE_ERROR]', e.message); }
      await ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: '❌ Ошибка: '+err.message,
        ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'start')]])
      }); 
    }
  } else if (state.action==='interval_custom'){
    const parsed = parseCustomInterval(ctx.message.text.trim());
    if (!parsed){
      return ctx.replyWithPhoto({ source: './taygeta.png' }, {
        caption: `❌ Неверный формат. Примеры: 10-30, 5, 120-300сек`,
        ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `intervals_${state.data.sessionName}`)],[Markup.button.callback('❌ Отмена', 'start')]])
      });
    }
    sessionIntervals.set(state.data.sessionName, parsed);
    userData.delete(ctx.from.id);
    await ctx.replyWithPhoto({ source: './taygeta.png' }, {
      caption: `✅ Интервалы обновлены\n🤖 ${state.data.sessionName}: ${parsed.min}-${parsed.max} сек.`,
      ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', `info_${state.data.sessionName}`)]])
    });
  }
});


bot.command('users', async (ctx)=>{
  const list = getAllUsers();
  if (!list.length) return ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: '👥 ПОЛЬЗОВАТЕЛИ: пока пусто',
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню','start')]])
  });
  let t = `👥 ПОЛЬЗОВАТЕЛИ БОТА (${list.length})\n\n`;
  list.forEach((u,i)=>{ const name=u.firstName||u.username||`User${u.id}`; const last=u.lastSeen?new Date(u.lastSeen).toLocaleString('ru-RU'):'—'; let icon='🔴', st='Не в сети';
    if (u.status==='joining'){icon='🔗';st='Вступает';} else if(u.status==='setup'){icon='⚙️';st='Настраивает';} else if(u.status==='idle'){icon='🟡';st='Бездействует';} else if(u.lastSeen && (Date.now()-u.lastSeen<5*60*1000)){icon='🟢';st='Онлайн';}
    let dur=''; if (u.activityStartTime && u.status!=='offline'){ const d=Math.floor((Date.now()-u.activityStartTime)/1000); dur=` (${Math.floor(d/60)}м ${d%60}с)`; }
    t+=`${i+1}. ${name} (@${u.username||'no_username'})\n   ${icon} ${st}${dur}\n   🕐 ${last}\n\n`;
  });
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: t,
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню','start')]])
  });
});
bot.action('users', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const list = getAllUsers();
  if (!list.length) return ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: '👥 ПОЛЬЗОВАТЕЛИ: пока пусто',
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню','start')]])
  });
  let t = `👥 ПОЛЬЗОВАТЕЛИ БОТА (${list.length})\n\n`;
  list.forEach((u,i)=>{ const name=u.firstName||u.username||`User${u.id}`; const last=u.lastSeen?new Date(u.lastSeen).toLocaleString('ru-RU'):'—'; let icon='🔴', st='Не в сети';
    if (u.status==='joining'){icon='🔗';st='Вступает';} else if(u.status==='setup'){icon='⚙️';st='Настраивает';} else if(u.status==='idle'){icon='🟡';st='Бездействует';} else if(u.lastSeen && (Date.now()-u.lastSeen<5*60*1000)){icon='🟢';st='Онлайн';}
    let dur=''; if (u.activityStartTime && u.status!=='offline'){ const d=Math.floor((Date.now()-u.activityStartTime)/1000); dur=` (${Math.floor(d/60)}м ${d%60}с)`; }
    t+=`${i+1}. ${name} (@${u.username||'no_username'})\n   ${icon} ${st}${dur}\n   🕐 ${last}\n\n`;
  });
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: t,
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню','start')]])
  });
});

bot.action('links', async (ctx)=>{
  try{ 
    await ctx.answerCbQuery(); 
  }catch(e){ 
    console.error('[links_action] answerCbQuery error:', e.message); 
  }
  console.log('[links_action] User clicked links button');
  console.log('[links_action] savedLinks.size=', savedLinks.size);
  try {
    await renderLinks(ctx, 1);
    console.log('[links_action] renderLinks completed successfully');
  } catch (e) {
    console.error('[links_action] renderLinks error:', e.message);
    console.error('[links_action] renderLinks stack:', e.stack);
  }
});

bot.action('links_copy', async (ctx)=>{
  try{ 
    if (!savedLinks.size) {
      await ctx.answerCbQuery('Ссылок нет');
      return;
    }
    const links = Array.from(savedLinks.values()).map(d => d.url).join('\n');
    await ctx.answerCbQuery('Отправляю ссылки файлом');
    
    // Создаем временный файл с ссылками
    const fileName = `links_${new Date().toISOString().slice(0,10)}.txt`;
    const filePath = `/tmp/${fileName}`;
    fs.writeFileSync(filePath, links, 'utf8');
    
    // Отправляем файл с ссылками
    await ctx.replyWithDocument({
      source: filePath,
      filename: `ссылки_${new Date().toLocaleDateString('ru-RU')}.txt`
    }, {
      caption: `📋 Файл с ${savedLinks.size} ссылками`,
      ...Markup.inlineKeyboard([
        [Markup.button.callback('📝 Текстом', 'links_copy_text')],
        [Markup.button.callback('⬅️ Назад', 'links')]
      ])
    });
    
    // Удаляем временный файл
    setTimeout(() => {
      try { fs.unlinkSync(filePath); } catch(e) {}
    }, 5000);
    
  }catch(e){
    console.error('[links_copy] Error:', e.message);
  }
});

// Отдельный обработчик для вывода ссылок текстом
bot.action('links_copy_text', async (ctx)=>{
  try{ 
    const links = Array.from(savedLinks.values()).map(d => d.url).join('\n');
    await ctx.answerCbQuery('Отправляю текстом');
    
    await ctx.reply(`📋 Ссылки для копирования:\n\n${links}`, {
      ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ Назад', 'links')]])
    });
  }catch(e){
    console.error('[links_copy_text] Error:', e.message);
  }
});
bot.action('links_cleanup', async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const now=Date.now(), weekAgo=now-7*24*60*60*1000; let del=0;
  for (const [id,d] of savedLinks.entries()){ if (d.addedAt<weekAgo){ try{ fs.unlinkSync(path.join(LINKS_DIR, `${id}.json`)); }catch{} savedLinks.delete(id); del++; } }
  await ctx.replyWithPhoto({ source: './taygeta.png' }, {
    caption: `✅ Удалено: ${del}\nОсталось: ${savedLinks.size}`,
    ...Markup.inlineKeyboard([[Markup.button.callback('⬅️ В меню','start')]])
  });
});


bot.use((ctx,next)=>{
  if (ctx.from?.id){
    setImmediate(()=>{
      addUser(ctx.from.id, { username:ctx.from.username, firstName:ctx.from.first_name, lastName:ctx.from.last_name });
      if (!fs.existsSync(usersFile)) saveUsers(new Map());
      const m = loadUsers(); const u=m.get(String(ctx.from.id));
      if (u && u.status!=='joining' && u.status!=='setup') updateUserStatus(ctx.from.id,'idle');
    });
  }
  return next();
});
bot.action(/^grp:list:([^:]+):(\d+)$/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  const page = parseInt(ctx.match[2]);
  await renderGroups(ctx, name, page);
});

bot.action(/^grp:refresh:([^:]+):(\d+)$/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const name = ctx.match[1];
  const page = parseInt(ctx.match[2]);
  groupsCache.delete(name); // Clear cache to force refresh
  await renderGroups(ctx, name, page);
});
bot.action(/^links:list:(\d+)$/, async (ctx)=>{
  try{ await ctx.answerCbQuery(); }catch{}
  const page = parseInt(ctx.match[1]);
  await renderLinks(ctx, page);
});

bot.action(cbNoop, async (ctx)=>{ try{ await ctx.answerCbQuery(); }catch{} });


// Web Server Setup
const app = express();
const httpServer = createServer(app);
const io = new Server(httpServer, { cors: { origin: '*' } });

app.use(express.json());
app.use(express.static('public'));

// API: Get all sessions
app.get('/api/sessions', (req, res) => {
  const sessions = listSessionDirs().map(name => ({
    name,
    status: sessionStatus.get(name) || 'inactive',
    statusText: sessionHuman(sessionStatus.get(name) || 'inactive'),
    lastJoin: lastJoinInfo.get(name) || null,
    queueLength: joinQueue.get(name)?.length || 0,
    intervals: sessionIntervals.get(name) || { min: 5, max: 30 }
  }));
  res.json(sessions);
});

// API: Get session info
app.get('/api/sessions/:name', async (req, res) => {
  const { name } = req.params;
  try {
    const sock = activeSocks.get(name);
    const status = sessionStatus.get(name) || 'inactive';
    const lastJoin = lastJoinInfo.get(name);
    const queue = joinQueue.get(name) || [];
    const intervals = sessionIntervals.get(name) || { min: 5, max: 30 };
    
    let groups = [];
    if (status === 'active' && sock) {
      try {
        groups = await fetchGroups(name);
      } catch (e) {
        console.error(`[API_GROUPS_ERROR] ${name}:`, e.message);
      }
    }
    
    res.json({
      name,
      status,
      statusText: sessionHuman(status),
      lastJoin,
      queueLength: queue.length,
      queue: queue.map(q => ({ link: q.link, type: q.type, timestamp: q.timestamp, attempts: q.attempts })),
      intervals,
      groups,
      isActive: status === 'active'
    });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// API: Create new session
app.post('/api/sessions', async (req, res) => {
  const { name } = req.body;
  if (!name) return res.status(400).json({ error: 'Name required' });
  
  try {
    const safeName = canonizeName(name);
    const { sessionPath } = safeSessionPath(safeName);
    
    if (fs.existsSync(sessionPath)) {
      return res.status(400).json({ error: 'Session already exists' });
    }
    
    setStatus(safeName, 'syncing');
    io.emit('session_status', { name: safeName, status: 'syncing' });
    
    // Start connection (will generate QR)
    connectSocket(safeName, { 
      qrTimeoutSec: QR_TIMEOUT_CREATE, 
      createIfMissing: true 
    }).catch(err => {
      setStatus(safeName, 'error');
      io.emit('session_status', { name: safeName, status: 'error' });
    });
    
    res.json({ success: true, name: safeName });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// API: Activate session
app.post('/api/sessions/:name/activate', async (req, res) => {
  const { name } = req.params;
  try {
    const { sessionPath } = safeSessionPath(name);
    if (!fs.existsSync(sessionPath)) {
      return res.status(404).json({ error: 'Session not found' });
    }
    
    setStatus(name, 'syncing');
    io.emit('session_status', { name, status: 'syncing' });
    
    connectSocket(name, { 
      qrTimeoutSec: QR_TIMEOUT_ACTIVATE, 
      createIfMissing: true 
    }).catch(err => {
      setStatus(name, 'error');
      io.emit('session_status', { name, status: 'error' });
    });
    
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// API: Generate new QR
app.post('/api/sessions/:name/newqr', async (req, res) => {
  const { name } = req.params;
  try {
    setStatus(name, 'syncing');
    io.emit('session_status', { name, status: 'syncing' });
    
    connectSocket(name, { 
      qrTimeoutSec: QR_TIMEOUT_ACTIVATE, 
      createIfMissing: true, 
      forceNewQR: true 
    }).catch(err => {
      setStatus(name, 'error');
      io.emit('session_status', { name, status: 'error' });
    });
    
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// API: Get QR code
app.get('/api/sessions/:name/qr', async (req, res) => {
  const { name } = req.params;
  const qrAnchor = qrAnchors.get(name);
  const qrTimer = qrTimers.get(name);
  
  if (!qrTimer && !qrAnchor) {
    return res.status(404).json({ error: 'No QR code available' });
  }
  
  // QR code is stored in memory, we need to get it from the socket
  // For now, return status
  res.json({ 
    hasQR: true, 
    status: sessionStatus.get(name),
    timeout: qrTimer?.timeoutSec || 60
  });
});

// API: Get groups
app.get('/api/sessions/:name/groups', async (req, res) => {
  const { name } = req.params;
  try {
    const status = sessionStatus.get(name) || 'inactive';
    if (status !== 'active') {
      return res.status(400).json({ error: 'Сессия не активна', groups: [] });
    }
    
    const sock = activeSocks.get(name);
    if (!sock) {
      return res.status(400).json({ error: 'Сессия не подключена', groups: [] });
    }
    
    const groups = await fetchGroups(name);
    if (!Array.isArray(groups)) {
      return res.json({ error: 'Неверный формат данных', groups: [] });
    }
    
    res.json(groups);
  } catch (e) {
    console.error(`[API_GROUPS_ERROR] ${name}:`, e.message);
    res.status(500).json({ error: e.message, groups: [] });
  }
});

// API: Update intervals
app.put('/api/sessions/:name/intervals', (req, res) => {
  const { name } = req.params;
  const { min, max } = req.body;
  
  if (isNaN(min) || isNaN(max) || min < 3 || max < min || max > 3600) {
    return res.status(400).json({ error: 'Invalid intervals' });
  }
  
  sessionIntervals.set(name, { min, max });
  io.emit('session_update', { name, intervals: { min, max } });
  res.json({ success: true, intervals: { min, max } });
});

// API: Delete session
app.delete('/api/sessions/:name', async (req, res) => {
  const { name } = req.params;
  try {
    const { sessionPath } = safeSessionPath(name);
    
    // Logout if active
    if (activeSocks.has(name)) {
      try {
        const sock = activeSocks.get(name);
        await sock.logout();
        sock.end();
      } catch (e) {
        console.error('[DELETE_LOGOUT_ERROR]', e.message);
      }
      activeSocks.delete(name);
    }
    
    await clearQrTimer(name);
    sessionStatus.delete(name);
    groupsCache.delete(name);
    infoAnchors.delete(name);
    joinQueue.delete(name);
    sessionIntervals.delete(name);
    lastJoinInfo.delete(name);
    processingQueue.delete(name);
    
    try {
      fs.rmSync(sessionPath, { recursive: true, force: true });
    } catch (e) {
      console.error('[SESSION_DELETE_ERROR]', e.message);
    }
    
    io.emit('session_deleted', { name });
    res.json({ success: true });
  } catch (e) {
    res.status(500).json({ error: e.message });
  }
});

// API: Get saved links
app.get('/api/links', (req, res) => {
  const page = parseInt(req.query.page) || 1;
  const arr = Array.from(savedLinks.entries()).sort((a, b) => b[1].addedAt - a[1].addedAt);
  const { slice, page: p, pages, total } = paginate(arr, page, LINKS_PAGE_SIZE);
  
  res.json({
    links: slice.map(([id, d]) => ({ id, ...d })),
    page: p,
    pages,
    total
  });
});

// API: Get users
app.get('/api/users', (req, res) => {
  const users = getAllUsers();
  res.json(users);
});

// API: Get notifications history
app.get('/api/notifications', (req, res) => {
  const limit = parseInt(req.query.limit) || 500;
  const type = req.query.type; // Фильтр по типу
  
  let filtered = notificationsHistory;
  if (type && type !== 'all') {
    filtered = notificationsHistory.filter(n => n.type === type);
  }
  
  const result = filtered.slice(0, limit);
  res.json({
    notifications: result,
    total: filtered.length,
    allTotal: notificationsHistory.length
  });
});

// API: Clear notifications history
app.delete('/api/notifications', (req, res) => {
  notificationsHistory.length = 0;
  if (typeof io !== 'undefined') {
    io.emit('notifications_cleared');
  }
  res.json({ success: true, message: 'История уведомлений очищена' });
});

// WebSocket: Real-time updates
io.on('connection', (socket) => {
  console.log('[WS] Client connected');
  
  socket.on('disconnect', () => {
    console.log('[WS] Client disconnected');
  });
});

// Status updates are already handled in setStatus function above

// Initialize and start
(async () => {
  try {
    console.log('[INIT] Loading saved links...');
    loadSavedLinks();
    console.log('[INIT] Starting cleanup system...');
    startCleanupSystem();
    console.log('[INIT] Loading existing sessions...');
    await loadExistingSessions();
    console.log('[INIT] Sessions loaded');

    // Start web server
    const WEB_PORT = process.env.WEB_PORT || 3000;
    httpServer.listen(WEB_PORT, '0.0.0.0', () => {
      console.log(`🌐 Web panel started on http://localhost:${WEB_PORT}`);
      console.log(`🌐 Web panel also available on http://0.0.0.0:${WEB_PORT}`);
    });

    bot.catch((e)=>console.error('[BOT ERROR]', e));
    bot.launch().then(()=>console.log('✅ Bot started'));
  } catch (error) {
    console.error('[INIT ERROR]', error);
    process.exit(1);
  }
})();

process.on('unhandledRejection', (e)=>console.error('[UNHANDLED]', e));
process.on('SIGINT', ()=>process.exit(0));
process.on('SIGTERM', ()=>process.exit(0));
