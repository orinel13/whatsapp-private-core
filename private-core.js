require('dotenv').config();

const fs = require('fs');
const path = require('path');
const http = require('http');
const express = require('express');
const pino = require('pino');
const Database = require('better-sqlite3');
const { Server } = require('socket.io');
const { io: createSocketClient } = require('socket.io-client');
const {
  default: makeWASocket,
  Browsers,
  DisconnectReason,
  downloadMediaMessage,
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
const DB_FILE = path.resolve(process.env.DB_FILE || './data/private-core.sqlite');
const MEDIA_DIR = path.resolve(process.env.MEDIA_DIR || './data/media');
const SYNC_FULL_HISTORY = process.env.SYNC_FULL_HISTORY !== 'false';

if (!PUBLIC_GATEWAY_URL) {
  throw new Error('PUBLIC_GATEWAY_URL is required');
}

if (!GATEWAY_SECRET || GATEWAY_SECRET.length < 32) {
  throw new Error('GATEWAY_SECRET must be set and at least 32 characters long');
}

fs.mkdirSync(SESSIONS_DIR, { recursive: true });
fs.mkdirSync(path.dirname(DB_FILE), { recursive: true });
fs.mkdirSync(MEDIA_DIR, { recursive: true });

const logger = pino({
  level: process.env.LOG_LEVEL || 'debug',
  redact: ['req.headers["x-gateway-secret"]', '*.auth', '*.creds']
});

const db = new Database(DB_FILE);
db.pragma('journal_mode = WAL');
db.pragma('foreign_keys = ON');
db.exec(`
  CREATE TABLE IF NOT EXISTS accounts (
    phone_number TEXT PRIMARY KEY,
    status TEXT NOT NULL,
    error TEXT,
    last_update TEXT NOT NULL
  );

  CREATE TABLE IF NOT EXISTS chats (
    phone_number TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    name TEXT,
    subject TEXT,
    unread_count INTEGER NOT NULL DEFAULT 0,
    timestamp INTEGER NOT NULL DEFAULT 0,
    archived INTEGER NOT NULL DEFAULT 0,
    pinned INTEGER NOT NULL DEFAULT 0,
    last_message TEXT,
    raw_json TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (phone_number, chat_id)
  );

  CREATE TABLE IF NOT EXISTS messages (
    phone_number TEXT NOT NULL,
    chat_id TEXT NOT NULL,
    message_id TEXT NOT NULL,
    from_me INTEGER NOT NULL DEFAULT 0,
    participant TEXT,
    push_name TEXT,
    text TEXT,
    timestamp INTEGER NOT NULL DEFAULT 0,
    message_type TEXT NOT NULL DEFAULT 'unknown',
    media_path TEXT,
    media_mime TEXT,
    media_file_name TEXT,
    media_size INTEGER,
    raw_json TEXT,
    created_at TEXT NOT NULL,
    PRIMARY KEY (phone_number, chat_id, message_id)
  );

  CREATE TABLE IF NOT EXISTS contacts (
    phone_number TEXT NOT NULL,
    jid TEXT NOT NULL,
    name TEXT,
    notify TEXT,
    verified_name TEXT,
    raw_json TEXT,
    updated_at TEXT NOT NULL,
    PRIMARY KEY (phone_number, jid)
  );

  CREATE INDEX IF NOT EXISTS idx_chats_account_timestamp ON chats (phone_number, timestamp DESC);
  CREATE INDEX IF NOT EXISTS idx_messages_chat_timestamp ON messages (phone_number, chat_id, timestamp ASC);
  CREATE INDEX IF NOT EXISTS idx_messages_text ON messages (phone_number, text);
`);

function ensureColumn(tableName, columnName, definition) {
  const columns = db.prepare(`PRAGMA table_info(${tableName})`).all();
  if (!columns.some((column) => column.name === columnName)) {
    db.exec(`ALTER TABLE ${tableName} ADD COLUMN ${columnName} ${definition}`);
  }
}

ensureColumn('messages', 'media_path', 'TEXT');
ensureColumn('messages', 'media_mime', 'TEXT');
ensureColumn('messages', 'media_file_name', 'TEXT');
ensureColumn('messages', 'media_size', 'INTEGER');

const statements = {
  upsertAccount: db.prepare(`
    INSERT INTO accounts (phone_number, status, error, last_update)
    VALUES (@phoneNumber, @status, @error, @lastUpdate)
    ON CONFLICT(phone_number) DO UPDATE SET
      status = excluded.status,
      error = excluded.error,
      last_update = excluded.last_update
  `),
  deleteAccount: db.prepare('DELETE FROM accounts WHERE phone_number = ?'),
  upsertChat: db.prepare(`
    INSERT INTO chats (
      phone_number, chat_id, name, subject, unread_count, timestamp,
      archived, pinned, last_message, raw_json, updated_at
    )
    VALUES (
      @phoneNumber, @id, @name, @subject, @unreadCount, @timestamp,
      @archived, @pinned, @lastMessage, @rawJson, @updatedAt
    )
    ON CONFLICT(phone_number, chat_id) DO UPDATE SET
      name = COALESCE(NULLIF(excluded.name, ''), chats.name),
      subject = COALESCE(NULLIF(excluded.subject, ''), chats.subject),
      unread_count = excluded.unread_count,
      timestamp = MAX(excluded.timestamp, chats.timestamp),
      archived = excluded.archived,
      pinned = excluded.pinned,
      last_message = CASE
        WHEN excluded.timestamp >= chats.timestamp THEN COALESCE(NULLIF(excluded.last_message, ''), chats.last_message)
        ELSE chats.last_message
      END,
      raw_json = excluded.raw_json,
      updated_at = excluded.updated_at
  `),
  upsertMessage: db.prepare(`
    INSERT INTO messages (
      phone_number, chat_id, message_id, from_me, participant, push_name,
      text, timestamp, message_type, media_path, media_mime, media_file_name,
      media_size, raw_json, created_at
    )
    VALUES (
      @phoneNumber, @chatId, @id, @fromMe, @participant, @pushName,
      @text, @timestamp, @messageType, @mediaPath, @mediaMime, @mediaFileName,
      @mediaSize, @rawJson, @createdAt
    )
    ON CONFLICT(phone_number, chat_id, message_id) DO UPDATE SET
      from_me = excluded.from_me,
      participant = excluded.participant,
      push_name = excluded.push_name,
      text = excluded.text,
      timestamp = excluded.timestamp,
      message_type = excluded.message_type,
      media_path = COALESCE(excluded.media_path, messages.media_path),
      media_mime = COALESCE(excluded.media_mime, messages.media_mime),
      media_file_name = COALESCE(excluded.media_file_name, messages.media_file_name),
      media_size = COALESCE(excluded.media_size, messages.media_size),
      raw_json = excluded.raw_json
  `),
  upsertContact: db.prepare(`
    INSERT INTO contacts (phone_number, jid, name, notify, verified_name, raw_json, updated_at)
    VALUES (@phoneNumber, @jid, @name, @notify, @verifiedName, @rawJson, @updatedAt)
    ON CONFLICT(phone_number, jid) DO UPDATE SET
      name = COALESCE(NULLIF(excluded.name, ''), contacts.name),
      notify = COALESCE(NULLIF(excluded.notify, ''), contacts.notify),
      verified_name = COALESCE(NULLIF(excluded.verified_name, ''), contacts.verified_name),
      raw_json = excluded.raw_json,
      updated_at = excluded.updated_at
  `),
  deleteAccountChats: db.prepare('DELETE FROM chats WHERE phone_number = ?'),
  deleteAccountMessages: db.prepare('DELETE FROM messages WHERE phone_number = ?'),
  deleteAccountContacts: db.prepare('DELETE FROM contacts WHERE phone_number = ?'),
  selectAccounts: db.prepare('SELECT phone_number, status, error, last_update FROM accounts ORDER BY phone_number ASC'),
  selectChats: db.prepare('SELECT * FROM chats WHERE phone_number = ? ORDER BY timestamp DESC'),
  selectContact: db.prepare('SELECT * FROM contacts WHERE phone_number = ? AND jid = ?'),
  selectMessages: db.prepare(`
    SELECT * FROM (
      SELECT * FROM messages
      WHERE phone_number = ? AND chat_id = ?
      ORDER BY timestamp DESC
      LIMIT ?
    )
    ORDER BY timestamp ASC
  `),
  selectOldestMessage: db.prepare('SELECT * FROM messages WHERE phone_number = ? AND chat_id = ? ORDER BY timestamp ASC LIMIT 1'),
  selectRawMessageById: db.prepare('SELECT raw_json FROM messages WHERE phone_number = ? AND chat_id = ? AND message_id = ? LIMIT 1'),
  searchMessages: db.prepare(`
    SELECT m.*, c.name AS chat_name
    FROM messages m
    LEFT JOIN chats c ON c.phone_number = m.phone_number AND c.chat_id = m.chat_id
    WHERE m.phone_number = @phoneNumber
      AND (@chatId = '' OR m.chat_id = @chatId)
      AND (
        LOWER(COALESCE(m.text, '')) LIKE @query
        OR LOWER(COALESCE(m.push_name, '')) LIKE @query
        OR LOWER(COALESCE(c.name, '')) LIKE @query
        OR LOWER(m.chat_id) LIKE @query
      )
    ORDER BY m.timestamp DESC
    LIMIT @limit
  `),
  exportMessages: db.prepare(`
    SELECT m.*, c.name AS chat_name
    FROM messages m
    LEFT JOIN chats c ON c.phone_number = m.phone_number AND c.chat_id = m.chat_id
    WHERE m.phone_number = @phoneNumber
      AND (@chatId = '' OR m.chat_id = @chatId)
      AND (@query = '' OR LOWER(COALESCE(m.text, '')) LIKE @query)
    ORDER BY m.timestamp ASC
  `)
};

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
const chatMessageCursors = new Map();
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
app.use('/ui/media', privateUiAuth, express.static(MEDIA_DIR));

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
  statements.upsertAccount.run({
    phoneNumber,
    status,
    error: payload.error || null,
    lastUpdate: payload.lastUpdate
  });
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

function csvEscape(value) {
  const text = String(value == null ? '' : value);
  if (/[",\r\n]/.test(text)) {
    return `"${text.replace(/"/g, '""')}"`;
  }
  return text;
}

function exportRowsToCsv(rows) {
  const header = ['timestamp', 'account', 'chat_id', 'chat_name', 'from_me', 'push_name', 'message_type', 'text', 'media_path'];
  const lines = [header.join(',')];

  for (const row of rows) {
    lines.push([
      new Date(Number(row.timestamp || 0) * 1000).toISOString(),
      row.phone_number,
      row.chat_id,
      row.chat_name || '',
      row.from_me ? 'true' : 'false',
      row.push_name || '',
      row.message_type || '',
      row.text || '',
      row.media_path || ''
    ].map(csvEscape).join(','));
  }

  return lines.join('\n');
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

function unwrapMessageContent(content) {
  let current = content || {};

  for (let index = 0; index < 5; index += 1) {
    if (current.ephemeralMessage && current.ephemeralMessage.message) {
      current = current.ephemeralMessage.message;
      continue;
    }

    if (current.viewOnceMessage && current.viewOnceMessage.message) {
      current = current.viewOnceMessage.message;
      continue;
    }

    if (current.viewOnceMessageV2 && current.viewOnceMessageV2.message) {
      current = current.viewOnceMessageV2.message;
      continue;
    }

    if (current.documentWithCaptionMessage && current.documentWithCaptionMessage.message) {
      current = current.documentWithCaptionMessage.message;
      continue;
    }

    if (current.editedMessage && current.editedMessage.message) {
      current = current.editedMessage.message;
      continue;
    }

    return current;
  }

  return current;
}

function getMessageContent(message) {
  return unwrapMessageContent(message && message.message ? message.message : {});
}

function getMessageText(message) {
  if (!message) return '';
  const content = getMessageContent(message);

  if (content.conversation) return content.conversation;
  if (content.extendedTextMessage && content.extendedTextMessage.text) return content.extendedTextMessage.text;
  if (content.imageMessage && content.imageMessage.caption) return content.imageMessage.caption;
  if (content.videoMessage && content.videoMessage.caption) return content.videoMessage.caption;
  if (content.documentMessage && content.documentMessage.caption) return content.documentMessage.caption;
  if (content.audioMessage) return '';
  if (content.stickerMessage) return '';
  if (content.buttonsResponseMessage && content.buttonsResponseMessage.selectedDisplayText) return content.buttonsResponseMessage.selectedDisplayText;
  if (content.listResponseMessage && content.listResponseMessage.title) return content.listResponseMessage.title;
  if (content.templateButtonReplyMessage && content.templateButtonReplyMessage.selectedDisplayText) return content.templateButtonReplyMessage.selectedDisplayText;

  return '';
}

function toPublicMessage(message) {
  const remoteJid = message.key && message.key.remoteJid;
  const content = getMessageContent(message);
  const mediaInfo = getMediaInfo(content);
  return {
    id: message.key && message.key.id,
    chatId: remoteJid,
    fromMe: Boolean(message.key && message.key.fromMe),
    participant: message.key && message.key.participant,
    pushName: message.pushName || '',
    text: getMessageText(message),
    timestamp: asUnixTimestamp(message.messageTimestamp),
    messageType: Object.keys(content || {})[0] || 'unknown',
    media: mediaInfo
  };
}

function shouldExposeMessage(publicMessage) {
  if (!publicMessage || !publicMessage.chatId) return false;
  if (publicMessage.messageType === 'protocolMessage') return false;
  if (publicMessage.messageType === 'senderKeyDistributionMessage') return false;
  if (publicMessage.messageType === 'messageContextInfo') return false;
  if (!publicMessage.text && publicMessage.messageType === 'unknown') return false;
  return Boolean(publicMessage.text || publicMessage.fromMe || publicMessage.messageType !== 'unknown');
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

function getMediaInfo(content) {
  const mediaTypes = [
    ['imageMessage', 'image'],
    ['videoMessage', 'video'],
    ['audioMessage', 'audio'],
    ['documentMessage', 'document'],
    ['stickerMessage', 'sticker']
  ];

  for (const [key, kind] of mediaTypes) {
    if (!content || !content[key]) continue;
    const media = content[key];
    return {
      kind,
      mimeType: media.mimetype || '',
      fileName: media.fileName || media.title || '',
      fileLength: Number(media.fileLength && media.fileLength.low ? media.fileLength.low : media.fileLength || 0),
      caption: media.caption || ''
    };
  }

  return null;
}

function rowToAccount(row) {
  return {
    phoneNumber: row.phone_number,
    status: row.status,
    error: row.error || undefined,
    lastUpdate: row.last_update
  };
}

function rowToChat(row) {
  const contact = statements.selectContact.get(row.phone_number, row.chat_id);
  const contactName = contact ? (contact.name || contact.notify || contact.verified_name) : '';
  return {
    id: row.chat_id,
    name: contactName || row.name || row.chat_id,
    subject: row.subject || undefined,
    unreadCount: row.unread_count || 0,
    timestamp: row.timestamp || 0,
    archived: Boolean(row.archived),
    pinned: Boolean(row.pinned),
    lastMessage: row.last_message || ''
  };
}

function rowToMessage(row) {
  return {
    id: row.message_id,
    chatId: row.chat_id,
    fromMe: Boolean(row.from_me),
    participant: row.participant || undefined,
    pushName: row.push_name || '',
    text: row.text || '',
    timestamp: row.timestamp || 0,
    messageType: row.message_type || 'unknown',
    media: row.media_path ? {
      url: `/ui/media/${encodeMediaPath(row.media_path)}`,
      path: row.media_path,
      mimeType: row.media_mime || '',
      fileName: row.media_file_name || '',
      size: row.media_size || 0
    } : null
  };
}

function rowToSearchMessage(row) {
  return {
    ...rowToMessage(row),
    accountId: row.phone_number,
    chatName: row.chat_name || row.chat_id
  };
}

function encodeMediaPath(relativePath) {
  return String(relativePath || '').split(/[\\/]/).map(encodeURIComponent).join('/');
}

function jidToSafeName(jid) {
  return String(jid || 'unknown').replace(/[^a-zA-Z0-9@._-]/g, '_');
}

function fileExtensionFromMime(mimeType, fallback = 'bin') {
  const map = {
    'image/jpeg': 'jpg',
    'image/png': 'png',
    'image/webp': 'webp',
    'video/mp4': 'mp4',
    'audio/ogg': 'ogg',
    'audio/mpeg': 'mp3',
    'application/pdf': 'pdf'
  };

  return map[mimeType] || fallback;
}

function storeContact(phoneNumber, contact) {
  if (!phoneNumber || !contact || !contact.id) return;

  statements.upsertContact.run({
    phoneNumber,
    jid: contact.id,
    name: contact.name || '',
    notify: contact.notify || '',
    verifiedName: contact.verifiedName || contact.verified_name || '',
    rawJson: JSON.stringify(contact),
    updatedAt: new Date().toISOString()
  });
}

function storePublicChat(phoneNumber, chat) {
  if (!phoneNumber || !chat || !chat.id) return;

  const now = new Date().toISOString();
  const publicChat = {
    id: chat.id,
    name: chat.name || chat.subject || chat.id,
    subject: chat.subject || '',
    unreadCount: Number(chat.unreadCount || 0),
    timestamp: Number(chat.timestamp || 0),
    archived: chat.archived ? 1 : 0,
    pinned: chat.pinned ? 1 : 0,
    lastMessage: chat.lastMessage || '',
    rawJson: JSON.stringify(chat),
    updatedAt: now
  };

  statements.upsertChat.run({
    phoneNumber,
    ...publicChat
  });
}

async function downloadAndStoreMedia(phoneNumber, publicMessage, rawMessage) {
  if (!publicMessage.media || !rawMessage || !rawMessage.message) return null;

  const accountDir = path.join(MEDIA_DIR, phoneNumber);
  const chatDir = path.join(accountDir, jidToSafeName(publicMessage.chatId));
  fs.mkdirSync(chatDir, { recursive: true });

  const extension = fileExtensionFromMime(publicMessage.media.mimeType, publicMessage.media.kind || 'bin');
  const fileName = `${jidToSafeName(publicMessage.id || String(Date.now()))}.${extension}`;
  const absolutePath = path.join(chatDir, fileName);
  const relativePath = path.relative(MEDIA_DIR, absolutePath).replace(/\\/g, '/');

  if (fs.existsSync(absolutePath)) {
    return relativePath;
  }

  try {
    const buffer = await downloadMediaMessage(
      rawMessage,
      'buffer',
      {},
      {
        logger: logger.child({ account: phoneNumber, component: 'media-download' }),
        reuploadRequest: (msg) => {
          const sock = activeSockets.get(phoneNumber);
          return sock && typeof sock.updateMediaMessage === 'function' ? sock.updateMediaMessage(msg) : undefined;
        }
      }
    );

    fs.writeFileSync(absolutePath, buffer);
    return relativePath;
  } catch (error) {
    logger.warn({ phoneNumber, chatId: publicMessage.chatId, messageId: publicMessage.id, error: error.message }, 'failed to download media');
    return null;
  }
}

function storePublicMessage(phoneNumber, chatId, message, rawMessage = null) {
  if (!phoneNumber || !chatId || !message) return;
  if (!shouldExposeMessage(message)) return;

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
  statements.upsertMessage.run({
    phoneNumber,
    chatId,
    id: message.id || `${message.timestamp || Date.now()}-${message.fromMe ? 'out' : 'in'}`,
    fromMe: message.fromMe ? 1 : 0,
    participant: message.participant || '',
    pushName: message.pushName || '',
    text: message.text || '',
    timestamp: Number(message.timestamp || 0),
    messageType: message.messageType || 'unknown',
    mediaPath: message.media && message.media.path ? message.media.path : null,
    mediaMime: message.media && message.media.mimeType ? message.media.mimeType : null,
    mediaFileName: message.media && message.media.fileName ? message.media.fileName : null,
    mediaSize: message.media && message.media.size ? Number(message.media.size) : (message.media && message.media.fileLength ? Number(message.media.fileLength) : null),
    rawJson: JSON.stringify(rawMessage || message),
    createdAt: new Date().toISOString()
  });

  if (message.id) {
    chatMessageCursors.set(key, {
      id: message.id,
      fromMe: Boolean(message.fromMe)
    });
  }
}

function getMessagesSnapshot(phoneNumber, chatId) {
  return statements.selectMessages
    .all(phoneNumber, chatId, MAX_STORED_MESSAGES_PER_CHAT)
    .map(rowToMessage);
}

function getStateSnapshot() {
  const chatsByAccount = {};
  const accounts = statements.selectAccounts.all().map(rowToAccount);

  for (const account of accounts) {
    chatsByAccount[account.phoneNumber] = statements.selectChats
      .all(account.phoneNumber)
      .map(rowToChat);
  }

  return {
    accounts,
    chatsByAccount,
    publicGatewayConnected: socketClientToGateway.connected
  };
}

function hydrateCachesFromDatabase() {
  accountStatuses.clear();
  latestChats.clear();

  for (const account of statements.selectAccounts.all().map(rowToAccount)) {
    accountStatuses.set(account.phoneNumber, account);

    const chats = new Map();
    for (const chat of statements.selectChats.all(account.phoneNumber).map(rowToChat)) {
      chats.set(chat.id, chat);
    }
    latestChats.set(account.phoneNumber, chats);
  }
}

hydrateCachesFromDatabase();

function delay(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function ingestMessages(phoneNumber, messages, emitMessages = true) {
  let changed = false;

  for (const message of messages || []) {
    if (!message.message || !message.key || !message.key.remoteJid) continue;

    const publicMessage = toPublicMessage(message);
    if (!shouldExposeMessage(publicMessage)) continue;

    if (publicMessage.media) {
      const mediaPath = await downloadAndStoreMedia(phoneNumber, publicMessage, message);
      if (mediaPath) {
        publicMessage.media.path = mediaPath;
        publicMessage.media.url = `/ui/media/${encodeMediaPath(mediaPath)}`;
        publicMessage.media.size = publicMessage.media.fileLength || 0;
      }
    }

    storePublicMessage(phoneNumber, publicMessage.chatId, publicMessage, message);
    changed = true;

    if (emitMessages) {
      emitGateway('new-message', {
        phoneNumber,
        chatId: publicMessage.chatId,
        message: publicMessage
      });
    }

    const accountChats = latestChats.get(phoneNumber) || new Map();
    const previousChat = accountChats.get(publicMessage.chatId) || { id: publicMessage.chatId };
    accountChats.set(publicMessage.chatId, {
      ...previousChat,
      id: publicMessage.chatId,
      name: publicMessage.pushName || previousChat.name || publicMessage.chatId,
      lastMessage: publicMessage.text || previousChat.lastMessage || '',
      timestamp: publicMessage.timestamp
    });
    storePublicChat(phoneNumber, accountChats.get(publicMessage.chatId));
    latestChats.set(phoneNumber, accountChats);
  }

  if (changed) {
    sendChatsSnapshot(phoneNumber);
  }

  return changed;
}

async function backfillChatMessages(phoneNumber, chatId, count = 30) {
  const sock = activeSockets.get(phoneNumber);
  if (!sock || typeof sock.loadMessages !== 'function') return;

  const key = `${phoneNumber}::${chatId}`;
  const cursor = chatMessageCursors.get(key);

  try {
    const messages = await sock.loadMessages(chatId, count, cursor);
    await ingestMessages(phoneNumber, messages || [], false);
  } catch (error) {
    logger.warn({ phoneNumber, chatId, error: error.message }, 'failed to backfill chat messages');
  }
}

async function fetchOlderHistory(phoneNumber, chatId, count = 50) {
  const sock = activeSockets.get(phoneNumber);
  if (!sock || typeof sock.fetchMessageHistory !== 'function') {
    return { fetched: 0, supported: false };
  }

  const oldest = statements.selectOldestMessage.get(phoneNumber, chatId);
  if (!oldest) {
    await backfillChatMessages(phoneNumber, chatId, count);
    return { fetched: getMessagesSnapshot(phoneNumber, chatId).length, supported: true };
  }

  const oldestKey = {
    remoteJid: chatId,
    id: oldest.message_id,
    fromMe: Boolean(oldest.from_me)
  };

  if (oldest.participant) {
    oldestKey.participant = oldest.participant;
  }

  try {
    const result = await sock.fetchMessageHistory(count, oldestKey, oldest.timestamp);
    const messages = Array.isArray(result) ? result : (result && result.messages ? result.messages : []);
    await ingestMessages(phoneNumber, messages, false);
    return { fetched: messages.length, supported: true };
  } catch (error) {
    logger.warn({ phoneNumber, chatId, error: error.message }, 'failed to fetch older history');
    return { fetched: 0, supported: true, error: error.message };
  }
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

async function getStoredBaileysMessage(phoneNumber, key) {
  if (!key || !key.remoteJid || !key.id) return undefined;

  try {
    const row = statements.selectRawMessageById.get(phoneNumber, key.remoteJid, key.id);
    if (!row || !row.raw_json) return undefined;
    const parsed = JSON.parse(row.raw_json);
    return parsed.message;
  } catch (error) {
    logger.warn({ phoneNumber, error: error.message }, 'failed to read stored message for retry');
    return undefined;
  }
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
  const registeredAuth = hasRegisteredAuth(normalizedPhone);
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
    browser: shouldRequestPairing && !registeredAuth ? Browsers.ubuntu('Chrome') : Browsers.macOS('Desktop'),
    syncFullHistory: SYNC_FULL_HISTORY,
    getMessage: (key) => getStoredBaileysMessage(normalizedPhone, key)
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
    ingestMessages(normalizedPhone, messages || [], true).catch((error) => {
      logger.error({ phoneNumber: normalizedPhone, error: error.message }, 'failed to ingest messages');
    });
  });

  sock.ev.on('chats.upsert', (chats) => {
    mergeAndSendChats(normalizedPhone, chats);
  });

  sock.ev.on('chats.update', (chats) => {
    mergeAndSendChats(normalizedPhone, chats);
  });

  sock.ev.on('messaging-history.set', ({ chats, contacts, messages }) => {
    mergeAndSendChats(normalizedPhone, chats || []);

    for (const contact of contacts || []) {
      storeContact(normalizedPhone, contact);
    }

    ingestMessages(normalizedPhone, messages || [], true).catch((error) => {
      logger.error({ phoneNumber: normalizedPhone, error: error.message }, 'failed to ingest history messages');
    });
  });

  sock.ev.on('contacts.upsert', (contacts) => {
    for (const contact of contacts || []) {
      storeContact(normalizedPhone, contact);
    }
    sendChatsSnapshot(normalizedPhone);
  });

  sock.ev.on('contacts.update', (contacts) => {
    for (const contact of contacts || []) {
      storeContact(normalizedPhone, contact);
    }
    sendChatsSnapshot(normalizedPhone);
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
    storePublicChat(phoneNumber, accountChats.get(publicChat.id));
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
    accounts: statements.selectAccounts.all().length,
    dbFile: DB_FILE
  });
});

app.get('/ui', (req, res) => {
  res.sendFile(path.join(__dirname, 'private-ui', 'index.html'));
});

app.get('/ui/api/state', privateUiAuth, (req, res) => {
  res.json(getStateSnapshot());
});

app.get('/ui/api/messages/:phoneNumber/:chatId', privateUiAuth, async (req, res) => {
  const phoneNumber = normalizePhoneNumber(req.params.phoneNumber);
  const chatId = String(req.params.chatId || '');

  if (!phoneNumber || !chatId) {
    return res.status(400).json({ error: 'phoneNumber and chatId are required' });
  }

  await backfillChatMessages(phoneNumber, chatId, Number(req.query.limit || 30));

  return res.json({
    phoneNumber,
    chatId,
    messages: getMessagesSnapshot(phoneNumber, chatId)
  });
});

app.post('/ui/api/history/:phoneNumber/:chatId', privateUiAuth, async (req, res) => {
  const phoneNumber = normalizePhoneNumber(req.params.phoneNumber);
  const chatId = String(req.params.chatId || '');
  const count = Math.min(Number(req.body && req.body.count ? req.body.count : 50), 100);

  if (!phoneNumber || !chatId) {
    return res.status(400).json({ error: 'phoneNumber and chatId are required' });
  }

  const result = await fetchOlderHistory(phoneNumber, chatId, count);
  return res.json({
    ...result,
    phoneNumber,
    chatId,
    messages: getMessagesSnapshot(phoneNumber, chatId)
  });
});

app.get('/ui/api/search', privateUiAuth, (req, res) => {
  const phoneNumber = normalizePhoneNumber(req.query.account || '');
  const chatId = String(req.query.chatId || '');
  const query = String(req.query.q || '').trim().toLowerCase();
  const limit = Math.min(Number(req.query.limit || 100), 500);

  if (!phoneNumber) {
    return res.status(400).json({ error: 'account is required' });
  }

  if (!query) {
    return res.json({ results: [] });
  }

  const results = statements.searchMessages
    .all({
      phoneNumber,
      chatId,
      query: `%${query}%`,
      limit
    })
    .map(rowToSearchMessage);

  return res.json({ results });
});

app.get('/ui/api/export', privateUiAuth, (req, res) => {
  const phoneNumber = normalizePhoneNumber(req.query.account || '');
  const chatId = String(req.query.chatId || '');
  const queryText = String(req.query.q || '').trim().toLowerCase();
  const format = String(req.query.format || 'json').toLowerCase();

  if (!phoneNumber) {
    return res.status(400).json({ error: 'account is required' });
  }

  const rows = statements.exportMessages.all({
    phoneNumber,
    chatId,
    query: queryText ? `%${queryText}%` : ''
  });

  const stamp = new Date().toISOString().replace(/[:.]/g, '-');
  const scope = chatId ? jidToSafeName(chatId) : 'all-chats';

  if (format === 'csv') {
    res.setHeader('Content-Type', 'text/csv; charset=utf-8');
    res.setHeader('Content-Disposition', `attachment; filename="whatsapp-${phoneNumber}-${scope}-${stamp}.csv"`);
    return res.send(exportRowsToCsv(rows));
  }

  res.setHeader('Content-Type', 'application/json; charset=utf-8');
  res.setHeader('Content-Disposition', `attachment; filename="whatsapp-${phoneNumber}-${scope}-${stamp}.json"`);
  return res.send(JSON.stringify(rows.map(rowToSearchMessage), null, 2));
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
    statements.deleteAccount.run(phoneNumber);
    statements.deleteAccountChats.run(phoneNumber);
    statements.deleteAccountMessages.run(phoneNumber);
    statements.deleteAccountContacts.run(phoneNumber);

    for (const key of latestMessages.keys()) {
      if (key.startsWith(`${phoneNumber}::`)) {
        latestMessages.delete(key);
      }
    }

    for (const key of chatMessageCursors.keys()) {
      if (key.startsWith(`${phoneNumber}::`)) {
        chatMessageCursors.delete(key);
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

    storePublicMessage(accountId, to, publicMessage, sent);
    storePublicChat(accountId, {
      id: to,
      name: to,
      lastMessage: text,
      timestamp: publicMessage.timestamp
    });
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
  db.close();
  server.close(() => process.exit(0));
});

process.on('SIGTERM', () => {
  logger.info('shutting down');
  socketClientToGateway.close();
  privateIo.close();
  db.close();
  server.close(() => process.exit(0));
});
