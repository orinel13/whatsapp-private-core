require('dotenv').config();

const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const pino = require('pino');
const { Server } = require('socket.io');
const { io: createSocketClient } = require('socket.io-client');
const {
  default: makeWASocket,
  Browsers,
  DisconnectReason,
  fetchLatestBaileysVersion,
  makeCacheableSignalKeyStore,
  useMultiFileAuthState
} = require('@whiskeysockets/baileys');

const PRIVATE_PORT = Number(process.env.PRIVATE_PORT || 4000);
const PUBLIC_GATEWAY_URL = (process.env.PUBLIC_GATEWAY_URL || '').replace(/\/$/, '');
const GATEWAY_SECRET = process.env.GATEWAY_SECRET || '';
const SESSIONS_DIR = path.resolve(process.env.SESSIONS_DIR || './sessions');
const PRIVATE_UI_TOKEN = process.env.PRIVATE_UI_TOKEN || '';
const MAX_STORED_MESSAGES_PER_CHAT = Number(process.env.MAX_STORED_MESSAGES_PER_CHAT || 500);

if (!PUBLIC_GATEWAY_URL) {
  throw new Error('PUBLIC_GATEWAY_URL is required');
}

if (!GATEWAY_SECRET || GATEWAY_SECRET.length < 32) {
  throw new Error('GATEWAY_SECRET must be set and at least 32 characters long');
}

fs.mkdirSync(SESSIONS_DIR, { recursive: true });

const logger = pino({
  level: process.env.LOG_LEVEL || 'debug',
  redact: ['req.headers["x-gateway-secret"]', '*.auth', '*.creds']
});

const app = express();
const server = http.createServer(app);
const privateIo = new Server(server, {
  cors: {
    origin: false
  }
});

const activeSockets = new Map();
const accountStatuses = new Map();
const latestChats = new Map();
const latestMessages = new Map();
let baileysVersionPromise = null;

const socketClientToGateway = createSocketClient(PUBLIC_GATEWAY_URL, {
  auth: {
    role: 'private-core',
    secret: GATEWAY_SECRET
  },
  autoConnect: true,
  reconnection: true,
  reconnectionDelayMax: 10000,
  timeout: 10000
});

app.use(express.json({ limit: '256kb' }));
app.use('/ui', express.static(path.join(__dirname, 'private-ui')));

function normalizePhoneNumber(phoneNumber) {
  return String(phoneNumber || '').replace(/\D/g, '');
}

function sessionPath(phoneNumber) {
  return path.join(SESSIONS_DIR, phoneNumber);
}

function hasExistingAuth(phoneNumber) {
  return fs.existsSync(path.join(sessionPath(phoneNumber), 'creds.json'));
}

function hasRegisteredAuth(phoneNumber) {
  try {
    const credsPath = path.join(sessionPath(phoneNumber), 'creds.json');
    if (!fs.existsSync(credsPath)) return false;
    const creds = JSON.parse(fs.readFileSync(credsPath, 'utf8'));
    return Boolean(creds.registered);
  } catch (error) {
    logger.warn({ phoneNumber, error: error.message }, 'failed to read auth registration state');
    return false;
  }
}

function removeSessionFiles(phoneNumber) {
  const targetPath = sessionPath(phoneNumber);
  const resolvedTarget = path.resolve(targetPath);
  const resolvedRoot = path.resolve(SESSIONS_DIR);

  if (!resolvedTarget.startsWith(resolvedRoot + path.sep)) {
    throw new Error('Refusing to remove a session outside SESSIONS_DIR');
  }

  fs.rmSync(resolvedTarget, { recursive: true, force: true });
}

async function stopSession(phoneNumber) {
  const sock = activeSockets.get(phoneNumber);
  if (!sock) return;

  activeSockets.delete(phoneNumber);

  try {
    sock.ev.removeAllListeners();
    if (sock.ws && typeof sock.ws.close === 'function') {
      sock.ws.close();
    }
  } catch (error) {
    logger.warn({ phoneNumber, error: error.message }, 'failed to close WhatsApp socket cleanly');
  }
}

function setStatus(phoneNumber, status, extra = {}) {
  const payload = {
    phoneNumber,
    status,
    lastUpdate: new Date().toISOString(),
    ...extra
  };

  accountStatuses.set(phoneNumber, payload);
  emitGateway('account-status', payload);
  logger.info({ phoneNumber, status, extra }, 'account status changed');
}

function emitGateway(eventName, payload) {
  socketClientToGateway.emit(eventName, payload);
  privateIo.to('public-gateway').emit(eventName, payload);
  privateIo.to('private-ui').emit(eventName, payload);
}

function isPrivateUiTokenValid(token) {
  return !PRIVATE_UI_TOKEN || token === PRIVATE_UI_TOKEN;
}

function privateUiAuth(req, res, next) {
  const token = req.header('X-Private-Ui-Token') || req.query.token || '';
  if (!isPrivateUiTokenValid(token)) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  return next();
}

function gatewayAuth(req, res, next) {
  if (req.header('X-Gateway-Secret') !== GATEWAY_SECRET) {
    return res.status(401).json({ error: 'Unauthorized' });
  }
  return next();
}

function asUnixTimestamp(value) {
  if (!value) return Math.floor(Date.now() / 1000);
  if (typeof value === 'number') return value;
  if (typeof value === 'object' && typeof value.low === 'number') return value.low;
  return Number(value) || Math.floor(Date.now() / 1000);
}

function getMessageText(message) {
  if (!message) return '';
  const content = message.message || {};

  if (content.conversation) return content.conversation;
  if (content.extendedTextMessage && content.extendedTextMessage.text) return content.extendedTextMessage.text;
  if (content.imageMessage && content.imageMessage.caption) return content.imageMessage.caption;
  if (content.videoMessage && content.videoMessage.caption) return content.videoMessage.caption;
  if (content.documentMessage && content.documentMessage.caption) return content.documentMessage.caption;
  if (content.buttonsResponseMessage && content.buttonsResponseMessage.selectedDisplayText) return content.buttonsResponseMessage.selectedDisplayText;
  if (content.listResponseMessage && content.listResponseMessage.title) return content.listResponseMessage.title;
  if (content.templateButtonReplyMessage && content.templateButtonReplyMessage.selectedDisplayText) return content.templateButtonReplyMessage.selectedDisplayText;

  return '';
}

function toPublicMessage(message) {
  const remoteJid = message.key && message.key.remoteJid;
  return {
    id: message.key && message.key.id,
    chatId: remoteJid,
    fromMe: Boolean(message.key && message.key.fromMe),
    participant: message.key && message.key.participant,
    pushName: message.pushName || '',
    text: getMessageText(message),
    timestamp: asUnixTimestamp(message.messageTimestamp),
    messageType: Object.keys(message.message || {})[0] || 'unknown'
  };
}

function toPublicChat(chat) {
  return {
    id: chat.id,
    name: chat.name || chat.subject || chat.verifiedName || chat.notify || chat.id,
    subject: chat.subject,
    unreadCount: chat.unreadCount || 0,
    timestamp: asUnixTimestamp(chat.conversationTimestamp || chat.t),
    archived: Boolean(chat.archived),
    pinned: Boolean(chat.pinned),
    lastMessage: chat.lastMessage || ''
  };
}

function storePublicMessage(phoneNumber, chatId, message) {
  if (!phoneNumber || !chatId || !message) return;

  const key = `${phoneNumber}::${chatId}`;
  const messages = latestMessages.get(key) || [];

  if (message.id && messages.some((item) => item.id === message.id)) {
    return;
  }

  messages.push(message);
  messages.sort((a, b) => Number(a.timestamp || 0) - Number(b.timestamp || 0));

  if (messages.length > MAX_STORED_MESSAGES_PER_CHAT) {
    messages.splice(0, messages.length - MAX_STORED_MESSAGES_PER_CHAT);
  }

  latestMessages.set(key, messages);
}

function getMessagesSnapshot(phoneNumber, chatId) {
  return latestMessages.get(`${phoneNumber}::${chatId}`) || [];
}

function getStateSnapshot() {
  const chatsByAccount = {};

  for (const [phoneNumber, chats] of latestChats.entries()) {
    chatsByAccount[phoneNumber] = Array.from(chats.values()).sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));
  }

  return {
    accounts: Array.from(accountStatuses.values()).sort((a, b) => a.phoneNumber.localeCompare(b.phoneNumber)),
    chatsByAccount,
    publicGatewayConnected: socketClientToGateway.connected
  };
}

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function requestPairingCodeWithRetry(sock, phoneNumber) {
  let lastError = null;

  for (let attempt = 1; attempt <= 3; attempt += 1) {
    try {
      await delay(1000 * attempt);
      const code = await sock.requestPairingCode(phoneNumber);
      return String(code).replace(/\s/g, '');
    } catch (error) {
      lastError = error;
      logger.warn({ phoneNumber, attempt, error: error.message }, 'pairing code request failed');
    }
  }

  throw lastError || new Error('Failed to request pairing code');
}

function normalizeRecipient(to) {
  const value = String(to || '').trim();
  if (!value) return '';
  if (value.includes('@')) return value;

  const digits = value.replace(/\D/g, '');
  if (!digits) return '';
  return `${digits}@s.whatsapp.net`;
}

async function getBaileysVersion() {
  if (!baileysVersionPromise) {
    baileysVersionPromise = fetchLatestBaileysVersion()
      .then((result) => result.version)
      .catch((error) => {
        logger.warn({ error: error.message }, 'failed to fetch latest Baileys version, using bundled default');
        return undefined;
      });
  }

  return baileysVersionPromise;
}

async function createSession(phoneNumber, options = {}) {
  const normalizedPhone = normalizePhoneNumber(phoneNumber);
  if (!/^\d{8,15}$/.test(normalizedPhone)) {
    throw new Error('phoneNumber must contain 8-15 digits in international format without +');
  }

  if (shouldResetUnregisteredSession(normalizedPhone, options)) {
    await stopSession(normalizedPhone);
    removeSessionFiles(normalizedPhone);
  }

  const existingSocket = activeSockets.get(normalizedPhone);
  if (existingSocket && existingSocket.ws && existingSocket.ws.isOpen) {
    logger.info({ phoneNumber: normalizedPhone }, 'session already active');
    return existingSocket;
  }

  const shouldRequestPairing = options.requestPairing !== false;
  const authExists = hasExistingAuth(normalizedPhone);
  let pairingCodeRequested = false;

  setStatus(normalizedPhone, authExists ? 'connecting' : 'pending');

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath(normalizedPhone));
  const version = await getBaileysVersion();

  const sock = makeWASocket({
    auth: {
      creds: state.creds,
      keys: makeCacheableSignalKeyStore(state.keys, logger.child({ account: normalizedPhone, component: 'signal-key-store' }))
    },
    version,
    logger: logger.child({ account: normalizedPhone }),
    printQRInTerminal: false,
    browser: Browsers.ubuntu('Chrome')
  });

  activeSockets.set(normalizedPhone, sock);

  sock.ev.on('creds.update', saveCreds);

  sock.ev.on('connection.update', async (data) => {
    logger.debug({ phoneNumber: normalizedPhone, data }, 'connection update');

    if (shouldRequestPairing && !sock.authState.creds.registered && !pairingCodeRequested && data.qr) {
      pairingCodeRequested = true;

      requestPairingCodeWithRetry(sock, normalizedPhone)
        .then((code) => {
          emitGateway('pairing-code', {
            phoneNumber: normalizedPhone,
            code
          });
          setStatus(normalizedPhone, 'pending');
        })
        .catch((error) => {
          setStatus(normalizedPhone, 'error', { error: error.message });
          logger.error({ phoneNumber: normalizedPhone, error: error.message }, 'failed to request pairing code');
        });
    }

    if (data.pairingCode) {
      emitGateway('pairing-code', {
        phoneNumber: normalizedPhone,
        code: String(data.pairingCode).replace(/\s/g, '')
      });
    }

    if (data.connection === 'open') {
      setStatus(normalizedPhone, 'active');
      return;
    }

    if (data.connection === 'connecting') {
      setStatus(normalizedPhone, authExists ? 'connecting' : 'pending');
      return;
    }

    if (data.connection === 'close') {
      const statusCode = data.lastDisconnect && data.lastDisconnect.error && data.lastDisconnect.error.output
        ? data.lastDisconnect.error.output.statusCode
        : undefined;

      activeSockets.delete(normalizedPhone);

      if (statusCode === DisconnectReason.loggedOut) {
        setStatus(normalizedPhone, 'error', { error: 'WhatsApp session logged out. Remove session folder and pair again.' });
        return;
      }

      setStatus(normalizedPhone, 'disconnected', { error: data.lastDisconnect && data.lastDisconnect.error ? data.lastDisconnect.error.message : undefined });
      setTimeout(() => {
        createSession(normalizedPhone, { requestPairing: false }).catch((error) => {
          setStatus(normalizedPhone, 'error', { error: error.message });
        });
      }, 5000);
    }
  });

  sock.ev.on('messages.upsert', ({ messages }) => {
    for (const message of messages || []) {
      if (!message.message || !message.key || !message.key.remoteJid) continue;

      const publicMessage = toPublicMessage(message);
      storePublicMessage(normalizedPhone, publicMessage.chatId, publicMessage);
      emitGateway('new-message', {
        phoneNumber: normalizedPhone,
        chatId: publicMessage.chatId,
        message: publicMessage
      });

      const accountChats = latestChats.get(normalizedPhone) || new Map();
      const previousChat = accountChats.get(publicMessage.chatId) || { id: publicMessage.chatId };
      accountChats.set(publicMessage.chatId, {
        ...previousChat,
        id: publicMessage.chatId,
        name: publicMessage.pushName || previousChat.name || publicMessage.chatId,
        lastMessage: publicMessage.text,
        timestamp: publicMessage.timestamp
      });
      latestChats.set(normalizedPhone, accountChats);
    }

    sendChatsSnapshot(normalizedPhone);
  });

  sock.ev.on('chats.upsert', (chats) => {
    mergeAndSendChats(normalizedPhone, chats);
  });

  sock.ev.on('chats.update', (chats) => {
    mergeAndSendChats(normalizedPhone, chats);
  });

  sock.ev.on('messaging-history.set', ({ chats, messages }) => {
    mergeAndSendChats(normalizedPhone, chats || []);

    for (const message of messages || []) {
      if (!message.message || !message.key || !message.key.remoteJid) continue;
      const publicMessage = toPublicMessage(message);
      storePublicMessage(normalizedPhone, publicMessage.chatId, publicMessage);
      emitGateway('new-message', {
        phoneNumber: normalizedPhone,
        chatId: publicMessage.chatId,
        message: publicMessage
      });
    }
  });

  return sock;
}

function shouldResetUnregisteredSession(phoneNumber, options) {
  const shouldRequestPairing = options.requestPairing !== false;
  return shouldRequestPairing && hasExistingAuth(phoneNumber) && !hasRegisteredAuth(phoneNumber);
}

function mergeAndSendChats(phoneNumber, chats) {
  const accountChats = latestChats.get(phoneNumber) || new Map();

  for (const chat of chats || []) {
    const publicChat = toPublicChat(chat);
    if (!publicChat.id) continue;
    accountChats.set(publicChat.id, {
      ...(accountChats.get(publicChat.id) || {}),
      ...publicChat
    });
  }

  latestChats.set(phoneNumber, accountChats);
  sendChatsSnapshot(phoneNumber);
}

function sendChatsSnapshot(phoneNumber) {
  const accountChats = latestChats.get(phoneNumber) || new Map();
  const chats = Array.from(accountChats.values()).sort((a, b) => Number(b.timestamp || 0) - Number(a.timestamp || 0));
  emitGateway('chats-update', { phoneNumber, chats });
}

app.get('/health', (req, res) => {
  res.json({
    ok: true,
    role: 'private-core',
    publicGatewayConnected: socketClientToGateway.connected,
    accounts: accountStatuses.size
  });
});

app.get('/ui', (req, res) => {
  res.sendFile(path.join(__dirname, 'private-ui', 'index.html'));
});

app.get('/ui/api/state', privateUiAuth, (req, res) => {
  res.json(getStateSnapshot());
});

app.get('/ui/api/messages/:phoneNumber/:chatId', privateUiAuth, (req, res) => {
  const phoneNumber = normalizePhoneNumber(req.params.phoneNumber);
  const chatId = String(req.params.chatId || '');

  if (!phoneNumber || !chatId) {
    return res.status(400).json({ error: 'phoneNumber and chatId are required' });
  }

  return res.json({
    phoneNumber,
    chatId,
    messages: getMessagesSnapshot(phoneNumber, chatId)
  });
});

app.post('/api/add-account', gatewayAuth, async (req, res) => {
  const phoneNumber = normalizePhoneNumber(req.body.phoneNumber);

  if (!/^\d{8,15}$/.test(phoneNumber)) {
    return res.status(400).json({ error: 'phoneNumber must contain 8-15 digits in international format without +' });
  }

  try {
    await createSession(phoneNumber, { requestPairing: true });
    const status = accountStatuses.get(phoneNumber);
    return res.json({ success: true, phoneNumber, status: status ? status.status : 'pending' });
  } catch (error) {
    logger.error({ error: error.message, phoneNumber }, 'failed to add account');
    return res.status(500).json({ error: error.message });
  }
});

app.get('/api/accounts', gatewayAuth, (req, res) => {
  const accounts = Array.from(accountStatuses.values()).sort((a, b) => a.phoneNumber.localeCompare(b.phoneNumber));
  res.json({ accounts });
});

app.delete('/api/accounts/:phoneNumber', gatewayAuth, async (req, res) => {
  const phoneNumber = normalizePhoneNumber(req.params.phoneNumber);

  if (!/^\d{8,15}$/.test(phoneNumber)) {
    return res.status(400).json({ error: 'phoneNumber must contain 8-15 digits in international format without +' });
  }

  try {
    await stopSession(phoneNumber);
    removeSessionFiles(phoneNumber);
    accountStatuses.delete(phoneNumber);
    latestChats.delete(phoneNumber);

    for (const key of latestMessages.keys()) {
      if (key.startsWith(`${phoneNumber}::`)) {
        latestMessages.delete(key);
      }
    }

    emitGateway('account-status', { phoneNumber, status: 'removed', lastUpdate: new Date().toISOString() });
    return res.json({ success: true, phoneNumber });
  } catch (error) {
    logger.error({ phoneNumber, error: error.message }, 'failed to remove account');
    return res.status(500).json({ error: error.message });
  }
});

privateIo.on('connection', (socket) => {
  const auth = socket.handshake.auth || {};

  if (auth.role === 'private-ui') {
    if (!isPrivateUiTokenValid(auth.token || '')) {
      socket.emit('private-core-error', { error: 'Unauthorized' });
      socket.disconnect(true);
      return;
    }

    socket.join('private-ui');
    socket.emit('private-ui-ready', getStateSnapshot());
    return;
  }

  if (auth.role !== 'public-gateway' || auth.secret !== GATEWAY_SECRET) {
    socket.emit('private-core-error', { error: 'Unauthorized' });
    socket.disconnect(true);
    return;
  }

  socket.join('public-gateway');
  socket.emit('private-core-ready', { ok: true });

  socket.on('send-message', handleSendMessage);
});

socketClientToGateway.on('connect', () => {
  logger.info({ url: PUBLIC_GATEWAY_URL }, 'connected to public gateway');
  for (const status of accountStatuses.values()) {
    socketClientToGateway.emit('account-status', status);
  }
});

socketClientToGateway.on('connect_error', (error) => {
  logger.warn({ error: error.message }, 'failed to connect to public gateway');
});

socketClientToGateway.on('gateway-ready', (payload) => {
  logger.info({ payload }, 'public gateway ready');
});

socketClientToGateway.on('send-message', handleSendMessage);

async function handleSendMessage(payload, ack) {
  const accountId = normalizePhoneNumber(payload && payload.accountId);
  const to = normalizeRecipient(payload && payload.to);
  const text = String(payload && payload.message ? payload.message : '').trim();

  if (!accountId || !to || !text) {
    const response = { ok: false, error: 'accountId, to and message are required' };
    if (typeof ack === 'function') ack(response);
    return;
  }

  const sock = activeSockets.get(accountId);
  const status = accountStatuses.get(accountId);

  if (!sock || !status || status.status !== 'active') {
    const response = { ok: false, error: `Account ${accountId} is not active` };
    if (typeof ack === 'function') ack(response);
    return;
  }

  try {
    const sent = await sock.sendMessage(to, { text });
    const publicMessage = {
      id: sent && sent.key ? sent.key.id : `sent-${Date.now()}`,
      chatId: to,
      fromMe: true,
      text,
      timestamp: Math.floor(Date.now() / 1000),
      messageType: 'conversation'
    };

    storePublicMessage(accountId, to, publicMessage);
    emitGateway('new-message', {
      phoneNumber: accountId,
      chatId: to,
      message: publicMessage
    });

    if (typeof ack === 'function') ack({ ok: true, messageId: publicMessage.id });
  } catch (error) {
    logger.error({ error: error.message, accountId, to }, 'failed to send message');
    if (typeof ack === 'function') ack({ ok: false, error: error.message });
  }
}

async function restoreSessions() {
  const entries = fs.readdirSync(SESSIONS_DIR, { withFileTypes: true });
  const sessionDirs = entries.filter((entry) => entry.isDirectory()).map((entry) => entry.name);

  for (const phoneNumber of sessionDirs) {
    if (!/^\d{8,15}$/.test(phoneNumber) || !hasExistingAuth(phoneNumber)) continue;

    createSession(phoneNumber, { requestPairing: false }).catch((error) => {
      setStatus(phoneNumber, 'error', { error: error.message });
    });
  }
}

server.listen(PRIVATE_PORT, () => {
  logger.info({ port: PRIVATE_PORT, sessionsDir: SESSIONS_DIR }, 'private core listening');
  restoreSessions().catch((error) => {
    logger.error({ error: error.message }, 'failed to restore sessions');
  });
});

process.on('SIGINT', () => {
  logger.info('shutting down');
  socketClientToGateway.close();
  privateIo.close();
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  logger.info('shutting down');
  socketClientToGateway.close();
  privateIo.close();
  server.close(() => process.exit(0));
});
