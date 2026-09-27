const express = require('express');
const fs = require('fs-extra');
const path = require('path');
const { sms } = require("./msg");
const os = require('os');
const { exec } = require('child_process');
const router = express.Router();
const pino = require('pino');
const moment = require('moment-timezone');
const Jimp = require('jimp');
const crypto = require('crypto');
const axios = require('axios');
const FileType = require('file-type');
const fetch = require('node-fetch');
const { MongoClient } = require('mongodb');

const {
  default: makeWASocket,
  useMultiFileAuthState,
  delay,
  getContentType,
  makeCacheableSignalKeyStore,
  Browsers,
  jidNormalizedUser,
  downloadContentFromMessage,
  proto,
  DisconnectReason
} = require('baileys');
// ---------------- CONFIG ----------------

const BOT_NAME_FANCY = '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';

const config = {
  AUTO_VIEW_STATUS: 'true',
  AUTO_LIKE_STATUS: 'true',
  AUTO_RECORDING: 'true',
  AUTO_REPLY_STATUS: 'true', 
  AUTO_REPLY_MSG: 'ʏᴏᴜ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ ʙʏ Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ🥷',
  AUTO_LIKE_EMOJI: [
    '💖', '🩷', '💘', '💝', '💗', '💕', '💞', '🌸', '🎀', '🧸',
    '🐰', '🦋', '🩵', '🍓', '🧁', '🌷', '☁️', '🌈', '🍒', '🐝',
    '💫', '⭐', '🫶', '🦄', '🐥', '💐', '🪩', '🕊️', '💟', '🩰',
    '✨', '🎈', '🧃', '🐇', '🥹', '🌼', '🪻', '🫧', '🌹', '🦢'
  ],
  PREFIX: '.',
  MAX_RETRIES: 3,
  GROUP_INVITE_LINK: 'https://chat.whatsapp.com/DbPmjbBXmfy3qwlSDxPZGb?mode=gi_c',
  RCD_IMAGE_PATH: 'https://www.movanest.xyz/BDhh39.jpg',
  NEWSLETTER_JID: '120363406261194661@newsletter',
  OTP_EXPIRY: 300000,
  WORK_TYPE: 'public',
  OWNER_NUMBER: process.env.OWNER_NUMBER || '94784534871',
  CHANNEL_LINK: 'https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00',
  BOT_NAME: '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3',
  BOT_VERSION: '3.0.0V',
  OWNER_NAME: 'Sᴀɴᴜ xᴅ│Sɪᴛʜᴜᴡᴀ xᴅ',
  IMAGE_PATH: 'https://www.movanest.xyz/BDhh39.jpg',
  BOT_FOOTER: '> *𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3*',
  BUTTON_IMAGES: { ALIVE: 'https://www.movanest.xyz/BDhh39.jpg' }
};

// ---------------- MONGO SETUP ----------------

const MONGO_URI = process.env.MONGO_URI || 'mongodb+srv://mongodbchamaminibot_db_user:ZBbUugrtQimXAUSY@chamaminibotv3.3enbza8.mongodb.net/PYEZVRQL';
const MONGO_DB = process.env.MONGO_DB || 'CHAMA-OFC';

let mongoClient, mongoDB;
let sessionsCol, numbersCol, adminsCol, newsletterCol, configsCol, newsletterReactsCol;

async function initMongo() {
  try {
    if (mongoClient && mongoClient.topology && mongoClient.topology.isConnected && mongoClient.topology.isConnected()) return;
  } catch(e){}
  mongoClient = new MongoClient(MONGO_URI, { useNewUrlParser: true, useUnifiedTopology: true });
  await mongoClient.connect();
  mongoDB = mongoClient.db(MONGO_DB);

  sessionsCol = mongoDB.collection('sessions');
  numbersCol = mongoDB.collection('numbers');
  adminsCol = mongoDB.collection('admins');
  newsletterCol = mongoDB.collection('newsletter_list');
  configsCol = mongoDB.collection('configs');
  newsletterReactsCol = mongoDB.collection('newsletter_reacts');

  await sessionsCol.createIndex({ number: 1 }, { unique: true });
  await numbersCol.createIndex({ number: 1 }, { unique: true });
  await newsletterCol.createIndex({ jid: 1 }, { unique: true });
  await newsletterReactsCol.createIndex({ jid: 1 }, { unique: true });
  await configsCol.createIndex({ number: 1 }, { unique: true });
  console.log('✅ Mongo initialized and collections ready');
}

// ---------------- Mongo helpers ----------------

async function saveCredsToMongo(number, creds, keys = null) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = { number: sanitized, creds, keys, updatedAt: new Date() };
    await sessionsCol.updateOne({ number: sanitized }, { $set: doc }, { upsert: true });
    console.log(`Saved creds to Mongo for ${sanitized}`);
  } catch (e) { console.error('saveCredsToMongo error:', e); }
}

async function loadCredsFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await sessionsCol.findOne({ number: sanitized });
    return doc || null;
  } catch (e) { console.error('loadCredsFromMongo error:', e); return null; }
}

async function removeSessionFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await sessionsCol.deleteOne({ number: sanitized });
    console.log(`Removed session from Mongo for ${sanitized}`);
  } catch (e) { console.error('removeSessionToMongo error:', e); }
}

async function addNumberToMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.updateOne({ number: sanitized }, { $set: { number: sanitized } }, { upsert: true });
    console.log(`Added number ${sanitized} to Mongo numbers`);
  } catch (e) { console.error('addNumberToMongo', e); }
}

async function removeNumberFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await numbersCol.deleteOne({ number: sanitized });
    console.log(`Removed number ${sanitized} from Mongo numbers`);
  } catch (e) { console.error('removeNumberFromMongo', e); }
}

async function getAllNumbersFromMongo() {
  try {
    await initMongo();
    const docs = await numbersCol.find({}).toArray();
    return docs.map(d => d.number);
  } catch (e) { console.error('getAllNumbersFromMongo', e); return []; }
}

async function loadAdminsFromMongo() {
  try {
    await initMongo();
    const docs = await adminsCol.find({}).toArray();
    return docs.map(d => d.jid || d.number).filter(Boolean);
  } catch (e) { console.error('loadAdminsFromMongo', e); return []; }
}

async function addAdminToMongo(jidOrNumber) {
  try {
    await initMongo();
    const doc = { jid: jidOrNumber };
    await adminsCol.updateOne({ jid: jidOrNumber }, { $set: doc }, { upsert: true });
    console.log(`Added admin ${jidOrNumber}`);
  } catch (e) { console.error('addAdminToMongo', e); }
}

async function removeAdminFromMongo(jidOrNumber) {
  try {
    await initMongo();
    await adminsCol.deleteOne({ jid: jidOrNumber });
    console.log(`Removed admin ${jidOrNumber}`);
  } catch (e) { console.error('removeAdminFromMongo', e); }
}

async function addNewsletterToMongo(jid, emojis = []) {
  try {
    await initMongo();
    const doc = { jid, emojis: Array.isArray(emojis) ? emojis : [], addedAt: new Date() };
    await newsletterCol.updateOne({ jid }, { $set: doc }, { upsert: true });
    console.log(`Added newsletter ${jid} -> emojis: ${doc.emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterToMongo', e); throw e; }
}

async function removeNewsletterFromMongo(jid) {
  try {
    await initMongo();
    await newsletterCol.deleteOne({ jid });
    console.log(`Removed newsletter ${jid}`);
  } catch (e) { console.error('removeNewsletterFromMongo', e); throw e; }
}

async function listNewslettersFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewslettersFromMongo', e); return []; }
}

async function saveNewsletterReaction(jid, messageId, emoji, sessionNumber) {
  try {
    await initMongo();
    const doc = { jid, messageId, emoji, sessionNumber, ts: new Date() };
    if (!mongoDB) await initMongo();
    const col = mongoDB.collection('newsletter_reactions_log');
    await col.insertOne(doc);
    console.log(`Saved reaction ${emoji} for ${jid}#${messageId}`);
  } catch (e) { console.error('saveNewsletterReaction', e); }
}

async function setUserConfigInMongo(number, conf) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    await configsCol.updateOne({ number: sanitized }, { $set: { number: sanitized, config: conf, updatedAt: new Date() } }, { upsert: true });
  } catch (e) { console.error('setUserConfigInMongo', e); }
}

async function loadUserConfigFromMongo(number) {
  try {
    await initMongo();
    const sanitized = number.replace(/[^0-9]/g, '');
    const doc = await configsCol.findOne({ number: sanitized });
    return doc ? doc.config : null;
  } catch (e) { console.error('loadUserConfigFromMongo', e); return null; }
}

// -------------- newsletter react-config helpers --------------

async function addNewsletterReactConfig(jid, emojis = []) {
  try {
    await initMongo();
    await newsletterReactsCol.updateOne({ jid }, { $set: { jid, emojis, addedAt: new Date() } }, { upsert: true });
    console.log(`Added react-config for ${jid} -> ${emojis.join(',')}`);
  } catch (e) { console.error('addNewsletterReactConfig', e); throw e; }
}

async function removeNewsletterReactConfig(jid) {
  try {
    await initMongo();
    await newsletterReactsCol.deleteOne({ jid });
    console.log(`Removed react-config for ${jid}`);
  } catch (e) { console.error('removeNewsletterReactConfig', e); throw e; }
}

async function listNewsletterReactsFromMongo() {
  try {
    await initMongo();
    const docs = await newsletterReactsCol.find({}).toArray();
    return docs.map(d => ({ jid: d.jid, emojis: Array.isArray(d.emojis) ? d.emojis : [] }));
  } catch (e) { console.error('listNewsletterReactsFromMongo', e); return []; }
}

async function getReactConfigForJid(jid) {
  try {
    await initMongo();
    const doc = await newsletterReactsCol.findOne({ jid });
    return doc ? (Array.isArray(doc.emojis) ? doc.emojis : []) : null;
  } catch (e) { console.error('getReactConfigForJid', e); return null; }
}

// ---------------- basic utils ----------------

function formatMessage(title, content, footer) {
  return `*${title}*\n\n${content}\n\n> *${footer}*`;
}
function generateOTP(){ return Math.floor(100000 + Math.random() * 900000).toString(); }
function getSriLankaTimestamp(){ return moment().tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss'); }

const activeSockets = new Map();

const socketCreationTime = new Map();

const otpStore = new Map();

// ---------------- helpers kept/adapted ----------------

async function joinGroup(socket) {
  let retries = config.MAX_RETRIES;
  const inviteCodeMatch = (config.GROUP_INVITE_LINK || '').match(/chat\.whatsapp\.com\/([a-zA-Z0-9]+)/);
  if (!inviteCodeMatch) return { status: 'failed', error: 'No group invite configured' };
  const inviteCode = inviteCodeMatch[1];
  while (retries > 0) {
    try {
      const response = await socket.groupAcceptInvite(inviteCode);
      if (response?.gid) return { status: 'success', gid: response.gid };
      throw new Error('No group ID in response');
    } catch (error) {
      retries--;
      let errorMessage = error.message || 'Unknown error';
      if (error.message && error.message.includes('not-authorized')) errorMessage = 'Bot not authorized';
      else if (error.message && error.message.includes('conflict')) errorMessage = 'Already a member';
      else if (error.message && error.message.includes('gone')) errorMessage = 'Invite invalid/expired';
      if (retries === 0) return { status: 'failed', error: errorMessage };
      await delay(2000 * (config.MAX_RETRIES - retries));
    }
  }
  return { status: 'failed', error: 'Max retries reached' };
}

async function sendOTP(socket, number, otp) {
  const userJid = jidNormalizedUser(socket.user.id);
  const message = formatMessage(`*🔐ᴏᴛᴘ ᴠᴇʀɪꜰɪᴄᴀᴛɪᴏɴ💗${BOT_NAME_FANCY}*`, `*ʏᴏᴜ ᴏᴛᴘ ꜰʀᴏ ᴄᴏɴꜰɪɢ ᴜᴘᴅᴀᴛᴇ ɪꜱ 🔄:* *${otp}*\nᴛʜɪꜱ ᴏᴛᴘ ᴡɪʟʟ ᴇxᴘɪʀᴇ ɪɴ 5 ᴍᴜɴᴜᴛᴇꜱ.\n\n*ɴᴜᴍʙᴇʀ📍:* ${number}`, BOT_NAME_FANCY);
  try { await socket.sendMessage(userJid, { text: message }); console.log(`OTP ${otp} sent to ${number}`); }
  catch (error) { console.error(`Failed to send OTP to ${number}:`, error); throw error; }
}

// ---------------- handlers (newsletter + reactions) ----------------

async function setupNewsletterHandlers(socket, sessionNumber) {
  const rrPointers = new Map();

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages[0];
    if (!message?.key) return;
    const jid = message.key.remoteJid;

    try {
      const followedDocs = await listNewslettersFromMongo(); // array of {jid, emojis}
      const reactConfigs = await listNewsletterReactsFromMongo(); // [{jid, emojis}]
      const reactMap = new Map();
      for (const r of reactConfigs) reactMap.set(r.jid, r.emojis || []);

      const followedJids = followedDocs.map(d => d.jid);
      if (!followedJids.includes(jid) && !reactMap.has(jid)) return;

      let emojis = reactMap.get(jid) || null;
      if ((!emojis || emojis.length === 0) && followedDocs.find(d => d.jid === jid)) {
        emojis = (followedDocs.find(d => d.jid === jid).emojis || []);
      }
      if (!emojis || emojis.length === 0) emojis = config.AUTO_LIKE_EMOJI;

      let idx = rrPointers.get(jid) || 0;
      const emoji = emojis[idx % emojis.length];
      rrPointers.set(jid, (idx + 1) % emojis.length);

      const messageId = message.newsletterServerId || message.key.id;
      if (!messageId) return;

      let retries = 3;
      while (retries-- > 0) {
        try {
          if (typeof socket.newsletterReactMessage === 'function') {
            await socket.newsletterReactMessage(jid, messageId.toString(), emoji);
          } else {
            await socket.sendMessage(jid, { react: { text: emoji, key: message.key } });
          }
          console.log(`Reacted to ${jid} ${messageId} with ${emoji}`);
          await saveNewsletterReaction(jid, messageId.toString(), emoji, sessionNumber || null);
          break;
        } catch (err) {
          console.warn(`Reaction attempt failed (${3 - retries}/3):`, err?.message || err);
          await delay(1200);
        }
      }

    } catch (error) {
      console.error('Newsletter reaction handler error:', error?.message || error);
    }
  });
}

// ---------------- status + revocation + resizing ----------------
async function setupStatusHandlers(socket, sessionNumber) {
  const isOn = (value) => value === true || value === 'true' || value === 'on';

  socket.ev.on('messages.upsert', async ({ messages }) => {
    const message = messages?.[0];

    if (
      !message?.key ||
      message.key.remoteJid !== 'status@broadcast' ||
      !message.key.participant
    ) return;

    try {
      let userEmojis = config.AUTO_LIKE_EMOJI || ['❤️'];
      let autoViewStatus = config.AUTO_VIEW_STATUS;
      let autoLikeStatus = config.AUTO_LIKE_STATUS;
      let autoRecording = config.AUTO_RECORDING;

      if (typeof global.statusSeenMode !== 'undefined') {
        autoViewStatus = global.statusSeenMode;
      }

      if (sessionNumber) {
        const userConfig = await loadUserConfigFromMongo(sessionNumber) || {};

        if (
          Array.isArray(userConfig.AUTO_LIKE_EMOJI) &&
          userConfig.AUTO_LIKE_EMOJI.length > 0
        ) {
          userEmojis = userConfig.AUTO_LIKE_EMOJI;
        }

        if (userConfig.AUTO_VIEW_STATUS !== undefined) {
          autoViewStatus = userConfig.AUTO_VIEW_STATUS;
        }

        if (userConfig.STATUS_SEEN_MODE !== undefined) {
          autoViewStatus = userConfig.STATUS_SEEN_MODE;
        }

        if (userConfig.AUTO_LIKE_STATUS !== undefined) {
          autoLikeStatus = userConfig.AUTO_LIKE_STATUS;
        }

        if (userConfig.AUTO_RECORDING !== undefined) {
          autoRecording = userConfig.AUTO_RECORDING;
        }
      }

      if (isOn(autoRecording)) {
        await socket.sendPresenceUpdate('recording', message.key.participant);
      }

      if (isOn(autoViewStatus)) {
        let retries = config.MAX_RETRIES || 3;

        while (retries > 0) {
          try {
            await socket.readMessages([message.key]);

            await socket.sendMessage(
              message.key.participant,
              {
                text: 'ʏᴏᴜ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ ʙʏ Qᴜᴜᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ',
                contextInfo: {
                  forwardingScore: 999,
                  isForwarded: true,
                  forwardedNewsletterMessageInfo: {
                    newsletterJid: '120363160031023229@newsletter',
                    newsletterName: 'QUEEN IMALSHA MD',
                    serverMessageId: -1
                  }
                }
              },
              {
                quoted: {
                  key: message.key,
                  message: message.message
                }
              }
            );

            break;
          } catch (error) {
            retries--;
            if (retries === 0) throw error;
            await delay(1000);
          }
        }
      }

      if (isOn(autoLikeStatus)) {
        const randomEmoji = userEmojis[Math.floor(Math.random() * userEmojis.length)];
        let retries = config.MAX_RETRIES || 3;

        while (retries > 0) {
          try {
            await socket.sendMessage(
              message.key.remoteJid,
              {
                react: {
                  text: randomEmoji,
                  key: message.key
                }
              },
              {
                statusJidList: [message.key.participant]
              }
            );

            break;
          } catch (error) {
            retries--;
            if (retries === 0) throw error;
            await delay(1000);
          }
        }
      }
    } catch (error) {
      console.error('Status handler error:', error);
    }
  });
}

async function handleMessageRevocation(socket, number) {
  socket.ev.on('messages.delete', async ({ keys }) => {
    if (!keys || keys.length === 0) return;
    const messageKey = keys[0];
    const userJid = jidNormalizedUser(socket.user.id);
    const deletionTime = getSriLankaTimestamp();
    const message = formatMessage('*📍ᴍᴇꜱꜱᴀɢᴇ ᴅᴇʟᴇᴛᴇᴅ*', `ᴀ ᴍᴇꜱꜱᴀɢᴇ ᴡᴀꜱ ᴅᴇʟᴇᴛᴇᴅ ꜰʀᴏᴍ ʏᴏᴜʀ ᴄʜᴀᴛ.\n*📍ꜰʀᴏᴍ:* ${messageKey.remoteJid}\n*📍ᴅʟᴇᴛᴇᴛɪᴏɴ ᴛɪᴍᴇ:* ${deletionTime}`, BOT_NAME_FANCY);
    try { await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: message }); }
    catch (error) { console.error('Failed to send deletion notification:', error); }
  });
}


async function resize(image, width, height) {
  let oyy = await Jimp.read(image);
  return await oyy.resize(width, height).getBufferAsync(Jimp.MIME_JPEG);
}
// ---------------- command handlers ---------------
function setupCommandHandlers(socket, number) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    const type = getContentType(msg.message);
    if (!msg.message) return;
    msg.message = (getContentType(msg.message) === 'ephemeralMessage') ? msg.message.ephemeralMessage.message : msg.message;

    const from = msg.key.remoteJid;
    const sender = from;
    const nowsender = msg.key.fromMe ? (socket.user.id.split(':')[0] + '@s.whatsapp.net' || socket.user.id) : (msg.key.participant || msg.key.remoteJid);
    const senderNumber = (nowsender || '').split('@')[0];
    const developers = `${config.OWNER_NUMBER}`;
    const botNumber = socket.user.id.split(':')[0];
    const isbot = botNumber.includes(senderNumber);
    const isOwner = isbot ? isbot : developers.includes(senderNumber);
    const isGroup = from.endsWith("@g.us");


 const m = sms(socket, msg);                                               
const quoted =
            type == "extendedTextMessage" &&
            msg.message.extendedTextMessage.contextInfo != null
              ? msg.message.extendedTextMessage.contextInfo.quotedMessage || []
              : [];
        const body = (type === 'conversation') ? msg.message.conversation 
            : msg.message?.extendedTextMessage?.contextInfo?.hasOwnProperty('quotedMessage') 
                ? msg.message.extendedTextMessage.text 
            : (type == 'interactiveResponseMessage') 
                ? msg.message.interactiveResponseMessage?.nativeFlowResponseMessage 
                    && JSON.parse(msg.message.interactiveResponseMessage.nativeFlowResponseMessage.paramsJson)?.id 
            : (type == 'templateButtonReplyMessage') 
                ? msg.message.templateButtonReplyMessage?.selectedId 
            : (type === 'extendedTextMessage') 
                ? msg.message.extendedTextMessage.text 
            : (type == 'imageMessage') && msg.message.imageMessage.caption 
                ? msg.message.imageMessage.caption 
            : (type == 'videoMessage') && msg.message.videoMessage.caption 
                ? msg.message.videoMessage.caption 
            : (type == 'buttonsResponseMessage') 
                ? msg.message.buttonsResponseMessage?.selectedButtonId 
            : (type == 'listResponseMessage') 
                ? msg.message.listResponseMessage?.singleSelectReply?.selectedRowId 
            : (type == 'messageContextInfo') 
                ? (msg.message.buttonsResponseMessage?.selectedButtonId 
                    || msg.message.listResponseMessage?.singleSelectReply?.selectedRowId 
                    || msg.text) 
            : (type === 'viewOnceMessage') 
                ? msg.message[type]?.message[getContentType(msg.message[type].message)] 
            : (type === "viewOnceMessageV2") 
                ? (msg.message[type]?.message?.imageMessage?.caption || msg.message[type]?.message?.videoMessage?.caption || "") 
            : '';

    if (!body || typeof body !== 'string') return;

    const prefix = config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);
    const command = isCmd ? body.slice(prefix.length).trim().split(' ').shift().toLowerCase() : null;
    const args = body.trim().split(/ +/).slice(1);

    // helper: download quoted media into buffer
    async function downloadQuotedMedia(quoted) {
      if (!quoted) return null;
      const qTypes = ['imageMessage','videoMessage','audioMessage','documentMessage','stickerMessage'];
      const qType = qTypes.find(t => quoted[t]);
      if (!qType) return null;
      const messageType = qType.replace(/Message$/i, '').toLowerCase();
      const stream = await downloadContentFromMessage(quoted[qType], messageType);
      let buffer = Buffer.from([]);
      for await (const chunk of stream) buffer = Buffer.concat([buffer, chunk]);
      return {
        buffer,
        mime: quoted[qType].mimetype || '',
        caption: quoted[qType].caption || quoted[qType].fileName || '',
        ptt: quoted[qType].ptt || false,
        fileName: quoted[qType].fileName || ''
      };
    }
      const sanitizedSession = (number || '').replace(/[^0-9]/g, ''); // Session Number
    let groupAdmins = [];
    if (isGroup) {
        try {
            const groupMetadata = await socket.groupMetadata(from);
            groupAdmins = groupMetadata.participants
                .filter(p => p.admin === 'admin' || p.admin === 'superadmin')
                .map(p => p.id);
        } catch (e) {}
    }

    // Anti-Badword Function Call
    await handleBadWords(socket, msg, body, sender, isGroup, groupAdmins, isOwner, sanitizedSession);

    if (!command) return;

    try {

      // Load user config for work type restrictions
      const sanitized = (number || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      
// ========== ADD WORK TYPE RESTRICTIONS HERE ==========
// Apply work type restrictions for non-owner users
if (!isOwner) {
  // Get work type from user config or fallback to global config
  const workType = userConfig.WORK_TYPE || 'public'; // Default to public if not set
  
  // If work type is "private", only owner can use commands
  if (workType === "private") {
    console.log(`Command blocked: WORK_TYPE is private for ${sanitized}`);
    return;
  }
  
  // If work type is "inbox", block commands in groups
  if (isGroup && workType === "inbox") {
    console.log(`Command blocked: WORK_TYPE is inbox but message is from group for ${sanitized}`);
    return;
  }
  
  // If work type is "groups", block commands in private chats
  if (!isGroup && workType === "groups") {
    console.log(`Command blocked: WORK_TYPE is groups but message is from private chat for ${sanitized}`);
    return;
  }
  
  // If work type is "public", allow all (no restrictions needed)
}
// ========== END WORK TYPE RESTRICTIONS ==========

      switch (command) {
        // --- existing commands (deletemenumber, unfollow, newslist, admin commands etc.) ---
        // ... (keep existing other case handlers unchanged) ...
          case 'ts': {
    const axios = require('axios');

    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';

    let query = q.replace(/^[.\/!]ts\s*/i, '').trim();

    if (!query) {
        return await socket.sendMessage(sender, {
            text: '*[❗] TikTok එකේ මොකද්ද බලන්න ඕනෙ කියපං! 🔍*'
        }, { quoted: msg });
    }

    // 🔹 Load bot name dynamically
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ ᴠ3.0.0💗';

    // 🔹 Fake contact for quoting
    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_TS"
        },
        message: {
            contactMessage: {
                displayName: botName,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    try {
        await socket.sendMessage(sender, { text: `🔎 Searching TikTok for: ${query}...` }, { quoted: shonux });

        const searchParams = new URLSearchParams({ keywords: query, count: '10', cursor: '0', HD: '1' });
        const response = await axios.post("https://tikwm.com/api/feed/search", searchParams, {
            headers: { 'Content-Type': "application/x-www-form-urlencoded; charset=UTF-8", 'Cookie': "current_language=en", 'User-Agent': "Mozilla/5.0" }
        });

        const videos = response.data?.data?.videos;
        if (!videos || videos.length === 0) {
            return await socket.sendMessage(sender, { text: '⚠️ No videos found.' }, { quoted: shonux });
        }

        // Limit number of videos to send
        const limit = 3; 
        const results = videos.slice(0, limit);

        // 🔹 Send videos one by one
        for (let i = 0; i < results.length; i++) {
            const v = results[i];
            const videoUrl = v.play || v.download || null;
            if (!videoUrl) continue;

            await socket.sendMessage(sender, { text: `*⏳ Downloading:* ${v.title || 'No Title'}` }, { quoted: shonux });

            await socket.sendMessage(sender, {
                video: { url: videoUrl },
                caption: `*🎵 ${botName} ᴛɪᴋᴛᴏᴋ ᴅᴏᴡɴʟᴏᴀᴅᴇʀ*\n📍ᴛɪᴛʟᴇ: ${v.title || '❌ɴᴏ ᴛɪᴛʟᴇ'}\n*📍ᴀᴜᴛʜᴏʀ:* ${v.author?.nickname || 'Unknown'}`
            }, { quoted: shonux });
        }

    } catch (err) {
        console.error('TikTok Search Error:', err);
        await socket.sendMessage(sender, { text: `❌ Error: ${err.message}` }, { quoted: shonux });
    }

    break;
}

case 'setting': {
    await socket.sendMessage(sender, { react: { text: '⚙️', key: msg.key } });
    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const senderNum = (nowsender || '').split('@')[0];
        const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');

        // Permission check
        if (senderNum !== sanitized && senderNum !== ownerNum) {
            const shonux = {
                key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETTING1" },
                message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
            };
            return await socket.sendMessage(sender, { text: '❌ Permission denied.' }, { quoted: shonux });
        }

        const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
        const botName = currentConfig.botName || BOT_NAME_FANCY;
        const prefix = currentConfig.PREFIX || config.PREFIX;

        // --- Audio Conversion and Sending ---
        const fs = require('fs');
        const axios = require('axios');
        const path = require('path');
        const ffmpeg = require('fluent-ffmpeg');
        const ffmpegPath = require('ffmpeg-static');
        if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);

        const tempMp3 = path.join(__dirname, 'temp.mp3');
        const tempOpus = path.join(__dirname, 'temp.opus');

        // 1. Audio එක download කරගැනීම
        const resp = await axios.get('https://files.catbox.moe/wyreig.mp4', { responseType: 'arraybuffer' });
        fs.writeFileSync(tempMp3, Buffer.from(resp.data));

        // 2. Opus වලට convert කිරීම
        await new Promise((resolve, reject) => {
            ffmpeg(tempMp3)
                .noVideo()
                .audioCodec('libopus')
                .format('opus')
                .on('end', () => resolve())
                .on('error', (err) => reject(err))
                .save(tempOpus);
        });

        // 3. Audio එක Voice Note එකක් ලෙස යැවීම
        if (fs.existsSync(tempOpus)) {
            const opusBuffer = fs.readFileSync(tempOpus);
            await socket.sendMessage(sender, { 
                audio: opusBuffer, 
                mimetype: 'audio/ogg; codecs=opus', 
                ptt: true 
            }, { quoted: msg });
        }

        // 4. තාවකාලික ගොනු මකා දැමීම
        if (fs.existsSync(tempMp3)) fs.unlinkSync(tempMp3);
        if (fs.existsSync(tempOpus)) fs.unlinkSync(tempOpus);

        // ------------------------------------

        // මෙතැන් සිට මෙනුව පෙන්වන කොටස
        
const settingOptions = {
      name: 'single_select',
      paramsJson: JSON.stringify({
        title: `💗 ${botName}`,
        sections: [
          {
            title: '◉ᴠ3.0.0💗ᴛʏᴘᴇ ᴏꜰ ᴡᴏʀᴋ',
            rows: [
              { title: 'ᴘᴜʙʟɪᴄ🌐', description: '', id: `${prefix}wtype public` },
              { title: 'ɢʀᴏᴜᴘ👥', description: '', id: `${prefix}wtype groups` },
              { title: 'ɪɴʙᴏx👤', description: '', id: `${prefix}wtype inbox` },
              { title: 'ᴘʀɪᴠᴀᴛᴇ🔒', description: '', id: `${prefix}wtype private` },
            ],
          },
          {
            title: '◉ᴠ3.0.0💗ꜰᴀᴋᴇ ᴛʏᴘɪɴɢ',
            rows: [
              { title: 'ᴛʏᴘɪɴɢ ᴏɴ✅', description: '', id: `${prefix}autotyping on` },
              { title: 'ᴛʏᴘɪɴɢ ᴏꜰꜰ❌', description: '', id: `${prefix}autotyping off` },
            ],
          },
          {
            title: '◉ᴠ3.0.0💗ꜰᴀᴋᴇ ʀᴇᴄᴏʀᴅɪɴɢ',
            rows: [
              { title: 'ʀᴇᴄᴏʀᴅɪɴɢ ᴏɴ✅', description: '', id: `${prefix}autorecording on` },
              { title: 'ʀᴇᴄᴏʀᴅɪɴɢ ᴏꜰꜰ❌', description: '', id: `${prefix}autorecording off` },
            ],
          },
          {
            title: '◉ᴠ3.0.0💗ᴀʟʟᴡᴀʏꜱ ᴏɪɴʟɪɴᴇ',
            rows: [
              { title: 'ᴏɴʟɪɴᴇ ᴏɴ✅', description: '', id: `${prefix}botpresence online` },
              { title: 'ᴏɴʟɪɴᴇ ᴏꜰꜰ❌', description: '', id: `${prefix}botpresence offline` },
            ],
          },
          {
            title: '◉ᴠ3.0.0💗ᴀᴜᴛᴏ ꜱᴇᴇɴ ꜱᴛᴀᴛᴜꜱ',
            rows: [
              { title: 'ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ ᴏɴ✅', description: '', id: `${prefix}rstatus on` },
              { title: 'ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ ᴏꜰꜰ❌', description: '', id: `${prefix}rstatus off` },
            ],
          },
          {
            title: '◉ᴠ3.0.0💗ᴀᴜᴛᴏ ʀᴇᴀᴄᴛ ꜱᴛᴀᴛᴜꜱ',
            rows: [
              { title: 'ꜱᴛᴀᴛᴜꜱ ʟɪᴋᴇ ᴏɴ✅', description: '', id: `${prefix}arm on` },
              { title: 'ꜱᴛᴀᴛᴜꜱ ʟɪᴋᴇ ᴏꜰꜰ❌', description: '', id: `${prefix}arm off` },
            ],
          }, 
          {
            title: '◉ᴠ3.0.0💗ᴀᴜᴛᴏ ʀᴇᴊᴇᴄᴛ ᴄᴀʟʟꜱ',
            rows: [
              { title: 'ʀᴇᴊᴇᴄᴛ ᴄᴀʟʟ ᴏɴ✅', description: '', id: `${prefix}creject on` },
              { title: 'ʀᴇᴊᴇᴄᴛ ᴄᴀʟʟ ᴏꜰꜰ❌', description: '', id: `${prefix}creject off` },
            ],
          },
          {
            title: '◉ᴠ3.0.0💗ᴀᴜᴛᴏ ʀᴇᴀᴅ ᴍᴇꜱꜱᴀɢᴇꜱ',
            rows: [
              { title: 'ʀᴇᴀᴅ ᴍᴇꜱꜱᴀɢᴇꜱ✅', description: '', id: `${prefix}mread all` },
              { title: 'ᴀʟʟ ᴍᴀꜱꜱᴀɢᴇꜱ ᴄᴏᴍᴍᴀɴᴅꜱ🔒', description: '', id: `${prefix}mread cmd` },
              { title: 'ᴏɴᴛ ᴀɴʏ ᴍᴀꜱꜱᴀɢᴇ ᴏꜰꜰ❌', description: '', id: `${prefix}mread off` },
            ],
          },
        ],
      }),
    };
    
        await socket.sendMessage(sender, {
            headerType: 1,
            viewOnce: true,
            image: { url: currentConfig.logo || config.RCD_IMAGE_PATH },
            caption: `
*_Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ ᴠ3.0.0💗Sᴇᴛᴛɪɴɢ Pᴀɴᴀʟ_*
 
*╭─╮*
*◎╭ᴡᴏʀᴋ ᴛʏᴘᴇ* ${currentConfig.WORK_TYPE || 'public'}
*│❑ʙᴏᴛ ᴘʀᴇꜱᴇɴᴄᴇ* ${currentConfig.PRESENCE || 'available'}
*│❑ᴀᴜᴛɪ ᴠɪᴇᴡ ꜱᴛᴀᴛᴜꜱ* ${currentConfig.AUTO_VIEW_STATUS || 'true'}
*│❑ᴀᴜᴛᴏ ʟɪᴋᴇ ꜱᴛᴀᴛᴜꜱ* ${currentConfig.AUTO_LIKE_STATUS || 'true'}
*│❑ᴀᴜᴛᴏ ᴀɴᴛɪ ᴄᴀʟʟ* ${currentConfig.ANTI_CALL || 'off'}
*│❑ᴀᴜᴛᴏ ʀᴇᴀᴅ ᴍᴀꜱꜱᴀɢᴇ* ${currentConfig.AUTO_READ_MESSAGE || 'off'}
*│❑ᴀᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ* ${currentConfig.AUTO_RECORDING || 'false'}
*◎╰ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ* ${currentConfig.AUTO_TYPING || 'false'}
*╰─╯*
`,
            buttons: [
                {
                    buttonId: 'settings_action',
                    buttonText: { displayText: '💗 ᴄᴏɴꜰɪɢᴜʀᴇ ꜱᴇᴛᴛɪɴɢꜱ ᴠ3.0.0' },
                    type: 4,
                    nativeFlowInfo: settingOptions,
                },
            ],
            footer: botName,
        }, { quoted: msg });

    } catch (e) {
        console.error('Setting command error:', e);
    }
    break;
}
case 'wtype': {
  await socket.sendMessage(sender, { react: { text: '🛠️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change work type.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = {
      groups: "groups",
      inbox: "inbox", 
      private: "private",
      public: "public"
    };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.WORK_TYPE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `📍 *ʏᴏᴜ ᴡᴏʀᴋ ᴛʏᴘᴇ ᴜᴘᴅᴀᴛᴇ ᴛᴏ: ${settings[q]}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- public\n- groups\n- inbox\n- private" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Wtype command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_WTYPE4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your work type!*" }, { quoted: shonux });
  }
  break;
}

case 'botpresence': {
  await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change bot presence.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = {
      online: "available",
      offline: "unavailable"
    };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.PRESENCE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      // Apply presence immediately
      await socket.sendPresenceUpdate(settings[q]);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `📍 *ʏᴏᴜ ʙᴏᴛ ᴘʀᴇꜱᴇɴᴄᴇ ᴜᴘᴅᴀᴛᴇ ᴛᴏ: ${q}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- online\n- offline" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Botpresence command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PRESENCE4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your bot presence!*" }, { quoted: shonux });
  }
  break;
}

case 'autotyping': {
  await socket.sendMessage(sender, { react: { text: '⌨️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change auto typing.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "true", off: "false" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_TYPING = settings[q];
      
      // If turning on auto typing, turn off auto recording to avoid conflict
      if (q === 'on') {
        userConfig.AUTO_RECORDING = "false";
      }
      
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `📍 *ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ ${q === 'on' ? 'ᴇɴᴀʙʟᴇᴅ' : 'ᴅɪꜱᴀʙʟᴇᴅ'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Options:* on / off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Autotyping error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TYPING4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating auto typing!*" }, { quoted: shonux });
  }
  break;
}

case 'rstatus': {
  await socket.sendMessage(sender, { react: { text: '👁️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change status seen setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "true", off: "false" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_VIEW_STATUS = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `📍 *ʏᴏᴜʀ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇᴍ ${q === 'on' ? 'ᴇɴᴀʙʟᴇᴅ' : 'ᴅɪꜱᴀʙʟᴇᴅ'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- on\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Rstatus command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RSTATUS4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your status seen setting!*" }, { quoted: shonux });
  }
  break;
}

case 'creject': {
  await socket.sendMessage(sender, { react: { text: '📞', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change call reject setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "on", off: "off" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.ANTI_CALL = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `📍 *ʏᴏᴜʀ ᴀᴜᴛᴏ ᴄᴀʟʟ ʀᴇᴊᴇᴄᴛ ${q === 'on' ? 'ᴇɴᴀʙʟᴇᴅ' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- on\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Creject command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CREJECT4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your call reject setting!*" }, { quoted: shonux });
  }
  break;
}

case 'arm': {
  await socket.sendMessage(sender, { react: { text: '❤️', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change status react setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { on: "true", off: "false" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_LIKE_STATUS = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Auto Status React ${q === 'on' ? 'ENABLED' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- on\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Arm command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ARM4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your status react setting!*" }, { quoted: shonux });
  }
  break;
}

case 'mread': {
  await socket.sendMessage(sender, { react: { text: '📖', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change message read setting.' }, { quoted: shonux });
    }
    
    let q = args[0];
    const settings = { all: "all", cmd: "cmd", off: "off" };
    
    if (settings[q]) {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_READ_MESSAGE = settings[q];
      await setUserConfigInMongo(sanitized, userConfig);
      
      let statusText = "";
      switch (q) {
        case "all":
          statusText = "READ ALL MESSAGES";
          break;
        case "cmd":
          statusText = "READ ONLY COMMAND MESSAGES"; 
          break;
        case "off":
          statusText = "DONT READ ANY MESSAGES";
          break;
      }
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Your Auto Message Read: ${statusText}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid option!*\n\nAvailable options:\n- all\n- cmd\n- off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Mread command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_MREAD4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your message read setting!*" }, { quoted: shonux });
  }
  break;
}

case 'autorecording': {
  await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change auto recording.' }, { quoted: shonux });
    }
    
    let q = args[0];
    
    if (q === 'on' || q === 'off') {
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      userConfig.AUTO_RECORDING = (q === 'on') ? "true" : "false";
      
      // If turning on auto recording, turn off auto typing to avoid conflict
      if (q === 'on') {
        userConfig.AUTO_TYPING = "false";
      }
      
      await setUserConfigInMongo(sanitized, userConfig);
      
      // Immediately stop any current recording if turning off
      if (q === 'off') {
        await socket.sendPresenceUpdate('available', sender);
      }
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: `✅ *Auto Recording ${q === 'on' ? 'ENABLED' : 'DISABLED'}*` }, { quoted: shonux });
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: "❌ *Invalid! Use:* .autorecording on/off" }, { quoted: shonux });
    }
  } catch (e) {
    console.error('Autorecording error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RECORDING4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating auto recording!*" }, { quoted: shonux });
  }
  break;
}

case 'prefix': {
  await socket.sendMessage(sender, { react: { text: '🔣', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change prefix.' }, { quoted: shonux });
    }
    
    let newPrefix = args[0];
    if (!newPrefix || newPrefix.length > 2) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: "❌ *Invalid prefix!*\nPrefix must be 1-2 characters long." }, { quoted: shonux });
    }
    
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    userConfig.PREFIX = newPrefix;
    await setUserConfigInMongo(sanitized, userConfig);
    
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `✅ *Your Prefix updated to: ${newPrefix}*` }, { quoted: shonux });
  } catch (e) {
    console.error('Prefix command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_PREFIX4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your prefix!*" }, { quoted: shonux });
  }
  break;
}

case 'settings': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETTINGS1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can view settings.' }, { quoted: shonux });
    }

    const currentConfig = await loadUserConfigFromMongo(sanitized) || {};
    const botName = currentConfig.botName || BOT_NAME_FANCY;
    
    const settingsText = `
*╭─「 ᴄᴜʀʀᴇɴᴛ ꜱᴇᴛᴛɪɴɢꜱ 」─●●➤*  
*│ 🔧 ᴡᴏʀᴋ ᴛʏᴘᴇ:* ${currentConfig.WORK_TYPE || 'public'}
*│ 🎭 ᴘʀᴇꜱᴇɴꜱᴇ:* ${currentConfig.PRESENCE || 'available'}
*│ 👁️ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ꜱᴇᴇɴ:* ${currentConfig.AUTO_VIEW_STATUS || 'true'}
*│ ❤️ ᴀᴜᴛᴏ ꜱᴛᴀᴛᴜꜱ ʟɪᴋᴇ:* ${currentConfig.AUTO_LIKE_STATUS || 'true'}
*│ 📞 ᴀᴜᴛᴏ ʀᴇᴊᴇᴄᴛ ᴄᴀʟʟ:* ${currentConfig.ANTI_CALL || 'off'}
*│ 📖 ᴀᴜᴛᴏ ʀᴇᴀᴅ ᴍᴇꜱꜱᴀɢᴇ:* ${currentConfig.AUTO_READ_MESSAGE || 'off'}
*│ 🎥 ᴀᴜᴛᴏ ʀᴇᴄᴏʀᴅɪɴɢ:* ${currentConfig.AUTO_RECORDING || 'false'}
*│ ⌨️ ᴀᴜᴛᴏ ᴛʏᴘɪɴɢ:* ${currentConfig.AUTO_TYPING || 'false'}
*│ 🔣 ᴘʀᴇꜰɪx:* ${currentConfig.PREFIX || '.'}
*│ 🎭 ꜱᴛᴀᴛᴜꜱ ᴇᴍᴏᴊɪꜱ:* ${(currentConfig.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI).join(' ')}
*╰──────────────●●➤*

*𝐔se ${currentConfig.PREFIX || '.'}𝐒etting 𝐓o 𝐂hange 𝐒ettings 𝐕ia 𝐌enu*
    `;

    await socket.sendMessage(sender, {
      image: { url: currentConfig.logo || config.RCD_IMAGE_PATH },
      caption: settingsText
    }, { quoted: msg });
    
  } catch (e) {
    console.error('Settings command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETTINGS2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error loading settings!*" }, { quoted: shonux });
  }
  break;
}

case 'checkjid': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CHECKJID1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can use this command.' }, { quoted: shonux });
    }

    const target = args[0] || sender;
    let targetJid = target;

    if (!target.includes('@')) {
      if (target.includes('-')) {
        targetJid = target.endsWith('@g.us') ? target : `${target}@g.us`;
      } else if (target.length > 15) {
        targetJid = target.endsWith('@newsletter') ? target : `${target}@newsletter`;
      } else {
        targetJid = target.endsWith('@s.whatsapp.net') ? target : `${target}@s.whatsapp.net`;
      }
    }

    let type = 'Unknown';
    if (targetJid.endsWith('@g.us')) {
      type = 'Group';
    } else if (targetJid.endsWith('@newsletter')) {
      type = 'Newsletter';
    } else if (targetJid.endsWith('@s.whatsapp.net')) {
      type = 'User';
    } else if (targetJid.endsWith('@broadcast')) {
      type = 'Broadcast List';
    } else {
      type = 'Unknown';
    }

    const responseText = `🔍 *JID INFORMATION*\n\n☘️ *Type:* ${type}\n🆔 *JID:* ${targetJid}\n\n╰──────────────────────`;

    await socket.sendMessage(sender, {
      image: { url: config.RCD_IMAGE_PATH },
      caption: responseText
    }, { quoted: msg });

  } catch (error) {
    console.error('Checkjid command error:', error);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CHECKJID2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error checking JID information!*" }, { quoted: shonux });
  }
  break;
}

case 'emojis': {
  await socket.sendMessage(sender, { react: { text: '🎭', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    // Permission check - only session owner or bot owner can change emojis
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS1" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change status reaction emojis.' }, { quoted: shonux });
    }
    
    let newEmojis = args;
    
    if (!newEmojis || newEmojis.length === 0) {
      // Show current emojis if no args provided
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};
      const currentEmojis = userConfig.AUTO_LIKE_EMOJI || config.AUTO_LIKE_EMOJI;
      
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      
      return await socket.sendMessage(sender, { 
        text: `🎭 *Current Status Reaction Emojis:*\n\n${currentEmojis.join(' ')}\n\nUsage: \`.emojis 😀 😄 😊 🎉 ❤️\`` 
      }, { quoted: shonux });
    }
    
    // Validate emojis (basic check)
    const invalidEmojis = newEmojis.filter(emoji => !/\p{Emoji}/u.test(emoji));
    if (invalidEmojis.length > 0) {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS3" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { 
        text: `❌ *Invalid emojis detected:* ${invalidEmojis.join(' ')}\n\nPlease use valid emoji characters only.` 
      }, { quoted: shonux });
    }
    
    // Get user-specific config from MongoDB
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    
    // Update ONLY this user's emojis
    userConfig.AUTO_LIKE_EMOJI = newEmojis;
    
    // Save to MongoDB
    await setUserConfigInMongo(sanitized, userConfig);
    
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    
    await socket.sendMessage(sender, { 
      text: `✅ *Your Status Reaction Emojis Updated!*\n\nNew emojis: ${newEmojis.join(' ')}\n\nThese emojis will be used for your automatic status reactions.` 
    }, { quoted: shonux });
    
  } catch (e) {
    console.error('Emojis command error:', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_EMOJIS5" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: "*❌ Error updating your status reaction emojis!*" }, { quoted: shonux });
  }
  break;
}

case 'ai':
case 'chat':
case 'gpt': {
  try {
    const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
    const q = text.split(" ").slice(1).join(" ").trim();

    if (!q) {
      await socket.sendMessage(sender, { 
        text: '*🚫 Please provide a message for AI.*',
        buttons: [
          { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📒Mᴇɴᴜ' }, type: 1 }
        ]
      });
      return;
    }

    // Session number
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    // Load bot name from DB or default
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    // Meta AI mention for quote
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: `META_AI_${Date.now()}` },
      message: { 
        contactMessage: { 
          displayName: botName, 
          vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` 
        } 
      }
    };

    await socket.sendMessage(sender, { react: { text: '🤖', key: msg.key } });
    await socket.sendMessage(sender, { text: '*⏳ AI thinking...*', quoted: metaQuote });

    const prompt = `
*ඔබ සැබෑ ගැහැනු ලමයෙකු මෙන් හැසිරිය යුතුය. ඉමෝජි පමනක් තිබේනම් ඔබද ඉමෝජි පමනක් යවන්න... 
ඔයාගේ නම ${botName} , ඔයාගෙන් කවුරුහරි "ඔයාව හැදුවේ කවුද?" කියලා ඇහුවොත් විතරක් ඔයා කියන්නේ "මාව හැදුවේ හිරුන් විකසිත" කියලා.* User Message: ${q}
    `;

    const payload = { contents: [{ parts: [{ text: prompt }] }] };

    const { data } = await axios.post(
      `https://generativelanguage.googleapis.com/v1beta/models/gemini-2.0-flash:generateContent?key=AIzaSyDD79CzhemWoS4WXoMTpZcs8g0fWNytNug`,
      payload,
      { headers: { "Content-Type": "application/json" } }
    );

    if (!data?.candidates?.[0]?.content?.parts?.[0]?.text) {
      await socket.sendMessage(sender, { 
        text: '*🚩 AI reply not found.*',
        buttons: [
          { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: 'Mᴇɴᴜ📒' }, type: 1 }
        ],
        quoted: metaQuote
      });
      return;
    }

    const aiReply = data.candidates[0].content.parts[0].text;

    await socket.sendMessage(sender, {
      text: aiReply,
      footer: `🤖 ${botName}`,
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📒Mᴇɴᴜ' }, type: 1 },
        { buttonId: `${config.PREFIX}alive`, buttonText: { displayText: '📡Bᴏᴛ ɪɴғᴏ' }, type: 1 }
      ],
      headerType: 1,
      quoted: metaQuote
    });

  } catch (err) {
    console.error("Error in AI chat:", err);
    await socket.sendMessage(sender, { 
      text: '*❌ Internal AI Error. Please try again later.*',
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📒Mᴇɴᴜ' }, type: 1 }
      ]
    });
  }
  break;
}
 case 'weather':
    try {
        // Messages in English
        const messages = {
            noCity: "❗ *Please provide a city name!* \n📋 *Usage*: .weather [city name]",
            weather: (data) => `
*🌤️ Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ Wᴇᴛʜᴀʀ⛈️*

*◈  ${data.name}, ${data.sys.country}  ◈*

*╭──🌤️───────●⛈️➤*
*┣⛈️𝑇𝑒𝑚𝑙𝑒𝑟𝑎𝑡𝑢𝑟𝑒:* ${data.main.temp}°C
*┣⛈️𝐹𝑒𝑒𝑙𝑠 𝐿𝑖𝑘𝑒:* ${data.main.feels_like}°C
*┣⛈️𝑀𝑖𝑛 𝑇𝑒𝑚𝑝:* ${data.main.temp_min}°C
*┣⛈️𝑀𝑎𝑥 𝑇𝑒𝑚𝑝:* ${data.main.temp_max}°C
*┣⛈️𝐻𝑢𝑚𝑖𝑑𝑖𝑡𝑦:* ${data.main.humidity}%
*┣⛈️𝑊𝑒𝑎𝑡ℎ𝑒𝑟:* ${data.weather[0].main}
*┣⛈️𝐷𝑒𝑠𝑐𝑟𝑖𝑝𝑡𝑖𝑜𝑛:* ${data.weather[0].description}
*┣⛈️𝑊𝑖𝑛𝑑 𝑆𝑝𝑒𝑒𝑑:* ${data.wind.speed} m/s
*┣⛈️𝑃𝑟𝑒𝑠𝑠𝑢𝑟𝑒:* ${data.main.pressure} hPa
*╰──🌤️───────●⛈️➤*

*Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2*
`,
            cityNotFound: "🚫 *City not found!* \n🔍 Please check the spelling and try again.",
            error: "⚠️ *An error occurred!* \n🔄 Please try again later."
        };

        // Check if a city name was provided
        if (!args || args.length === 0) {
            await socket.sendMessage(sender, { text: messages.noCity });
            break;
        }

        const apiKey = '2d61a72574c11c4f36173b627f8cb177';
        const city = args.join(" ");
        const url = `http://api.openweathermap.org/data/2.5/weather?q=${city}&appid=${apiKey}&units=metric`;

        const response = await axios.get(url);
        const data = response.data;

        // Get weather icon
        const weatherIcon = `https://openweathermap.org/img/wn/${data.weather[0].icon}@2x.png`;
        
        await socket.sendMessage(sender, {
            image: { url: weatherIcon },
            caption: messages.weather(data)
        });

    } catch (e) {
        console.log(e);
        if (e.response && e.response.status === 404) {
            await socket.sendMessage(sender, { text: messages.cityNotFound });
        } else {
            await socket.sendMessage(sender, { text: messages.error });
        }
    }
    break;
case 'statusautoseen': {
    if (!isCreator) return m.reply("*This command is only for the Bot Owner!*");
    if (!text) return m.reply(`*Usage:* ${prefix}autoseen on or off`);

    if (text.toLowerCase() === 'on') {
        config.AUTO_REPLY_STATUS = 'true';
        m.reply("✅ *Sᴛᴀᴛᴜꜱ Aᴜᴛᴏ Rᴇᴘʟʏ Oɴ Dᴏɴᴇ*");
    } else if (text.toLowerCase() === 'off') {
        config.AUTO_REPLY_STATUS = 'false';
        m.reply("❌ *Sᴛᴀᴛᴜꜱ Aᴜᴛᴏ Rᴇᴘʟʏ Oꜰꜰ Dᴏɴᴇ*");
    } else {
        m.reply(`*Invalid Usage!* \n\n*Please use:* \n${prefix}autoseen on \n${prefix}autoseen off`);
    }
    break;
}

case 'aiimg': 
case 'aiimg2': {
    const axios = require('axios');

    const q =
        msg.message?.conversation ||
        msg.message?.extendedTextMessage?.text ||
        msg.message?.imageMessage?.caption ||
        msg.message?.videoMessage?.caption || '';

    const prompt = q.trim();

    if (!prompt) {
        return await socket.sendMessage(sender, {
            text: '🎨 *Please provide a prompt to generate an AI image.*'
        }, { quoted: msg });
    }

    try {
        // 🔹 Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        // 🔹 Fake contact with dynamic bot name
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_AIIMG"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        // Notify user
        await socket.sendMessage(sender, { text: '🧠 *Creating your AI image...*' });

        // Determine API URL based on command
        let apiUrl = '';
        if (command === 'aiimg') {
            apiUrl = `https://api.siputzx.my.id/api/ai/flux?prompt=${encodeURIComponent(prompt)}`;
        } else if (command === 'aiimg2') {
            apiUrl = `https://api.siputzx.my.id/api/ai/magicstudio?prompt=${encodeURIComponent(prompt)}`;
        }

        // Call AI API
        const response = await axios.get(apiUrl, { responseType: 'arraybuffer' });

        if (!response || !response.data) {
            return await socket.sendMessage(sender, {
                text: '❌ *API did not return a valid image. Please try again later.*'
            }, { quoted: shonux });
        }

        const imageBuffer = Buffer.from(response.data, 'binary');

        // Send AI Image with bot name in caption
        await socket.sendMessage(sender, {
            image: imageBuffer,
            caption: `🧠 *${botName} AI IMAGE*\n\n📌 Prompt: ${prompt}`
        }, { quoted: shonux });

    } catch (err) {
        console.error('AI Image Error:', err);

        await socket.sendMessage(sender, {
            text: `❗ *An error occurred:* ${err.response?.data?.message || err.message || 'Unknown error'}`
        }, { quoted: msg });
    }
    break;
}
// ================= WELCOME ON/OFF COMMAND =================
case 'welcome': {
  await socket.sendMessage(sender, { react: { text: '👋', key: msg.key } });
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const senderNum = (nowsender || '').split('@')[0];
    const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    
    // Permission Check (Owner Only)
    if (senderNum !== sanitized && senderNum !== ownerNum) {
      return await socket.sendMessage(sender, { 
          text: '❌ Permission denied. Only the owner can change welcome settings.' 
      }, { quoted: msg });
    }
    
    // Load Config
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    let q = args[0];
    
    if (q === 'on' || q === 'off') {
      // Save Setting to Database
      userConfig.WELCOME_MSG = (q === 'on') ? "true" : "false";
      await setUserConfigInMongo(sanitized, userConfig);
      
      const statusMsg = q === 'on' ? '✅ *Group Welcome & Bye ENABLED*' : '❌ *Group Welcome & Bye DISABLED*';
      
      // Fake Header for Reply
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_WELCOME_CMD" },
        message: { contactMessage: { displayName: userConfig.botName || "Queen Seya", vcard: `BEGIN:VCARD\nVERSION:3.0\nFN:Bot\nEND:VCARD` } }
      };

      await socket.sendMessage(sender, { text: statusMsg }, { quoted: shonux });
      
    } else {
      await socket.sendMessage(sender, { 
          text: "❌ *Invalid option!*\nUsage: .welcome on / off" 
      }, { quoted: msg });
    }

  } catch (e) {
    console.error('Welcome command error:', e);
    await socket.sendMessage(sender, { text: "*❌ Error updating settings!*" }, { quoted: msg });
  }
  break;
}
case 'antibadword':
case 'antiword':
case 'badword': {
    // 1. React First (Shield Emoji)
    await socket.sendMessage(sender, { react: { text: '🛡️', key: msg.key } });

    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const senderNum = (nowsender || '').split('@')[0];
        const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');

        // 2. Permission Check
        if (senderNum !== sanitized && senderNum !== ownerNum) {
            return await socket.sendMessage(sender, { text: '❌ Permission denied. Only the Owner can change Anti-Badword settings.' }, { quoted: msg });
        }

        // 3. Load Config (For Bot Name & Logo)
        const userConfig = await loadUserConfigFromMongo(sanitized) || {};
        const botName = userConfig.botName || 'ᴠ3.0.0💗ᴀɴᴛɪ ʙᴀᴅ ᴡᴏʀᴅ';
        const botLogo = userConfig.logo || config.RCD_IMAGE_PATH || " වෙන image එකක් දන්න ඔනේ "; 

        // 4. Define Fake Header (AdReply)
        const fakeHeader = {
            title: "🛡️ ᴀɴᴛɪ ʙᴀᴅᴇᴏʀᴅ ꜱʏꜱᴛᴇᴍ ᴠ3.0.0💗",
            body: botName,
            thumbnailUrl: botLogo,
            sourceUrl: config.CHANNEL_LINK || "https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00",
            mediaType: 1,
            renderLargerThumbnail: true
        };

        // 5. Get Input (on / off)
        let q = args[0];
        if (q === 'on' || q === 'off') {
            userConfig.ANTI_BADWORD = (q === 'on') ? "true" : "false";
            await setUserConfigInMongo(sanitized, userConfig); //

            const statusMsg = q === 'on' 
                ? '✅ *ᴀɴᴛɪ ᴅᴀʙᴡᴏʀᴅ🛡️ꜱʏꜱᴛᴇᴍ ᴇɴᴀʙʟᴇᴅ*\n\n🛡️ Your group is now protected from bad words.' 
                : '❌ *ᴀɴᴛɪ ʙᴀᴅᴡᴏʀᴅ ꜱʏꜱᴛᴇᴍ ᴅɪꜱᴀʙʟᴇᴅ*\n\n⚠️ Bad word protection is turned off.';

            // Send Message with Fake Header
            await socket.sendMessage(sender, { 
                text: statusMsg,
                contextInfo: {
                    externalAdReply: fakeHeader
                }
            }, { quoted: msg });

        } else {
            // Invalid Option Message
            await socket.sendMessage(sender, { 
                text: "❌ *Invalid option!*\nUsage: .antibadword on / off",
                contextInfo: {
                    externalAdReply: fakeHeader
                }
            }, { quoted: msg });
        }

    } catch (e) {
        console.error('Anti-badword command error:', e);
        await socket.sendMessage(sender, { text: "*❌ Error updating settings!*" }, { quoted: msg });
    }
    break;
}
case 'statusmg':
case 'statusseen': {
  const action = (args && args[0] ? args[0] : '').toLowerCase();

  if (!global.statusSeenMode) global.statusSeenMode = 'off';

  if (action === 'on') {
    global.statusSeenMode = 'on';

    await socket.sendMessage(m.chat, {
      image: { url: './media/logo.jpg' },
      caption: '✅ Status seen ON කරා\n\n*Queen Imalsha MD Mini*'
    }, { quoted: m });

    await socket.sendMessage(m.chat, {
      audio: { url: 'https://www.movanest.xyz/YsekEa.mp3' },
      mimetype: 'audio/mpeg',
      ptt: true
    }, { quoted: m });

    await socket.sendMessage(m.chat, {
      video: { url: 'https://www.movanest.xyz/unJuHD.mp4' },
      mimetype: 'video/mp4',
      ptv: true
    }, { quoted: m });

    return;
  }

  if (action === 'off') {
    global.statusSeenMode = 'off';

    await socket.sendMessage(m.chat, {
      image: { url: './media/logo.jpg' },
      caption: '❌ Status seen OFF කරා\n\n*Queen Imalsha MD Mini*'
    }, { quoted: m });

    return;
  }

  return m.reply(
    `*Status Seen Control*\n\nCurrent: *${global.statusSeenMode.toUpperCase()}*\n\nUse:\n.statusmg on\n.statusmg off`
  );
}
break;

// --- Auto Text Reply Control ---
case 'reply':
case 'autoreply': {
    await socket.sendMessage(sender, { react: { text: '📍', key: msg.key } });
    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userConfig = await loadUserConfigFromMongo(sanitized) || {};
        const logoUrl = 'https://www.movanest.xyz/BDhh39.jpg'; 

        // Catalog Style Context Object
        const dtzminibot = {
            key: { fromMe: false, participant: '0@s.whatsapp.net', remoteJid: "status@broadcast" },
            message: {
                orderMessage: {
                    orderId: "9999",
                    thumbnail: null,
                    itemCount: 999,
                    status: "ʙᴏᴛ ᴏɴʟɪɴᴇ",
                    surface: "CATALOG",
                    message: `QUEEN IMALSHA MD`,
                    token: "AR6xBKbXZn0Xwmu76Ksyd7rnxI+Rx87HfinVlW4lwXa6JA=="
                }
            }
        };

        let q = args[0]?.toLowerCase();
        if (q === 'on' || q === 'off') {
            userConfig.AUTO_REPLY = (q === 'on') ? "true" : "false";
            await setUserConfigInMongo(sanitized, userConfig);

            const statusMsg = q === 'on' 
                ? '✅ *Aᴜᴛᴏ Tᴇxᴛ Rᴇᴘʟʏ Eɴᴀʙʟᴇᴅ ⬡⊷*' 
                : '❌ *Aᴜᴛᴏ Tᴇxᴛ Rᴇᴘʟʏ Dɪꜱᴀʙʟᴇᴅ ⬡⊷*';

            return await socket.sendMessage(sender, { 
                image: { url: logoUrl }, 
                caption: statusMsg 
            }, { quoted: dtzminibot });

        } else {
            const currentStatus = userConfig.AUTO_REPLY === 'true' ? 'Rᴇᴘʟʏ Oɴ ✅' : 'Rᴇᴘʟʏ Oꜰꜰ ❌';
            const usageMsg = `*𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3*\n\n` +
                             `⬡Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜꜱ⬡: ${currentStatus}\n\n` +
                             `*┋Ex┋:* .reply on\n` +
                             `*┋Ex┋:* .reply off\n\n` +
                             `┋Uꜱᴀɢᴇ┋: .reply on / off`;

            return await socket.sendMessage(sender, { 
                image: { url: logoUrl }, 
                caption: usageMsg 
            }, { quoted: dtzminibot });
        }
    } catch (e) {
        console.error(e);
    }
    break;
}

// --- Auto Voice Control ---
case 'voice':
case 'autovoice': {
    await socket.sendMessage(sender, { react: { text: '📍', key: msg.key } });
    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userConfig = await loadUserConfigFromMongo(sanitized) || {};
        const logoUrl = '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3'; 

        const dtzminibot = {
            key: { fromMe: false, participant: '0@s.whatsapp.net', remoteJid: "status@broadcast" },
            message: {
                orderMessage: {
                    orderId: "9999",
                    thumbnail: null,
                    itemCount: 999,
                    status: "ʙᴏᴛ ᴏɴʟɪɴᴇ",
                    surface: "CATALOG",
                    message: `QUEEN IMALSHA MD`,
                    token: "AR6xBKbXZn0Xwmu76Ksyd7rnxI+Rx87HfinVlW4lwXa6JA=="
                }
            }
        };

        let q = args[0]?.toLowerCase();
        if (q === 'on' || q === 'off') {
            userConfig.AUTO_VOICE = (q === 'on') ? "true" : "false";
            await setUserConfigInMongo(sanitized, userConfig);

            const statusMsg = q === 'on' 
                ? '✅ *Aᴜᴛᴏ Vᴏɪᴄᴇ Rᴇᴘʟʏ Eɴᴀʙʟᴇᴅ ⬡⊷*' 
                : '❌ *Aᴜᴛᴏ Vᴏɪᴄᴇ Rᴇᴘʟʏ Dɪꜱᴀʙʟᴇᴅ ⬡⊷*';

            return await socket.sendMessage(sender, { 
                image: { url: logoUrl }, 
                caption: statusMsg 
            }, { quoted: dtzminibot });

        } else {
            const currentStatus = userConfig.AUTO_VOICE === 'true' ? 'ᴀᴏᴛᴜ ᴠᴏɪᴄᴇ Oɴ ✅' : 'ᴀᴏᴛᴜ ᴠᴏɪᴄᴇ Oꜰꜰ ❌';
            const usageMsg = `*𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3*\n\n` +
                             `⬡Cᴜʀʀᴇɴᴛ Sᴛᴀᴛᴜs⬡: ${currentStatus}\n\n` +
                             `*┋Ex┋:* .voice on\n` +
                             `*┋Ex┋:* .voice off\n\n` +
                             `┋Uꜱᴀɢᴇ┋: .voice on / off`;

            return await socket.sendMessage(sender, { 
                image: { url: logoUrl }, 
                caption: usageMsg 
            }, { quoted: dtzminibot });
        }
    } catch (e) {
        console.error(e);
    }
    break;
}

case 'pair':
case 'freebot': {
    // ✅ Fix for node-fetch v3.x (ESM-only module)
    const fetch = (...args) => import('node-fetch').then(({ default: fetch }) => fetch(...args));
    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';

    const number = q.replace(/^[.\/!]pair\s*/i, '').trim();

    if (!number) {
        return await socket.sendMessage(sender, {
            text: '*📃 Usage:* .freebot +9476XXX'
        }, { quoted: msg });
    }

    try {
        const url = `ලින්ක් එක දන්න ඔනෙ හුට්ටො/code?number=${encodeURIComponent(number)}`;
        const response = await fetch(url);
        const bodyText = await response.text();

        let result;
        try {
            result = JSON.parse(bodyText);
        } catch (e) {
            return await socket.sendMessage(sender, {
                text: '❌ Invalid response from server.'
            }, { quoted: msg });
        }

        if (!result || !result.code) {
            return await socket.sendMessage(sender, {
                text: '❌ Failed to retrieve pairing code.'
            }, { quoted: msg });
        }

        // --- මෙතැන් සිට වෙනස් කළ කොටස ---

        // 1. ප්‍රධාන විස්තරය සහිත පණිවිඩය
        const mainMsg = `*Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2 ᴘᴀɪʀ ᴄᴏɴɴᴇᴄᴛᴇᴅ* ✅\n\n*🔑 ʏᴏᴜʀ ᴘᴀɪʀ ᴄᴏᴅᴇ :* ${result.code}\n\n> *ᴘᴏᴡᴇʀᴅ ʙʏ Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2*`;

        await socket.sendMessage(sender, { text: mainMsg }, { quoted: msg });

        await sleep(1500);

        // 2. කොපි කිරීමට පහසු වන ලෙස Monospace font එකෙන් code එක පමණක් යැවීම
        // මෙය WhatsApp වල එක පාරක් ටැප් කළ විට පහසුවෙන් කොපි කරගත හැක.
        await socket.sendMessage(sender, {
            text: '```' + result.code + '```'
        }, { quoted: msg });

        // --- අවසානය ---

    } catch (err) {
        console.error("❌ Pair Command Error:", err);
        await socket.sendMessage(sender, {
            text: '❌ An error occurred. Please try again.'
        }, { quoted: msg });
    }

    break;
}

  case 'cricket':
    try {
        console.log('Fetching cricket news from API...');
        
        const response = await fetch('https://suhas-bro-api.vercel.app/news/cricbuzz');
        console.log(`API Response Status: ${response.status}`);

        if (!response.ok) {
            throw new Error(`API request failed with status ${response.status}`);
        }

        const data = await response.json();
        console.log('API Response Data:', JSON.stringify(data, null, 2));

       
        if (!data.status || !data.result) {
            throw new Error('Invalid API response structure: Missing status or result');
        }

        const { title, score, to_win, crr, link } = data.result;
        if (!title || !score || !to_win || !crr || !link) {
            throw new Error('Missing required fields in API response: ' + JSON.stringify(data.result));
        }

       
        console.log('Sending message to user...');
        await socket.sendMessage(sender, {
            text: formatMessage(
                '*🏏 QUEEN IMALSHA CRICKET NEWS🏏*',
                `📢 *${title}*\n\n` +
                `🏆 *mark*: ${score}\n` +
                `🎯 *to win*: ${to_win}\n` +
                `📈 *now speed*: ${crr}\n\n` +
                `🌐 *link*: ${link}`,
                
                '> *Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2*'
            )
        });
        console.log('Message sent successfully.');
    } catch (error) {
        console.error(`Error in 'news' case: ${error.message}`);
        await socket.sendMessage(sender, {
            text: '⚠️ දැන්නම් හරි යන්නම ඕන 🙌.'
        });
    }
                    break;
                case 'gossip':
    try {
        
        const response = await fetch('https://suhas-bro-api.vercel.app/news/gossiplankanews');
        if (!response.ok) {
            throw new Error('API එකෙන් news ගන්න බැරි වුණා.බන් 😩');
        }
        const data = await response.json();


        if (!data.status || !data.result || !data.result.title || !data.result.desc || !data.result.link) {
            throw new Error('API එකෙන් ලැබුණු news data වල ගැටලුවක්');
        }


        const { title, desc, date, link } = data.result;


        let thumbnailUrl = 'https://via.placeholder.com/150';
        try {
            
            const pageResponse = await fetch(link);
            if (pageResponse.ok) {
                const pageHtml = await pageResponse.text();
                const $ = cheerio.load(pageHtml);
                const ogImage = $('meta[property="og:image"]').attr('content');
                if (ogImage) {
                    thumbnailUrl = ogImage; 
                } else {
                    console.warn(`No og:image found for ${link}`);
                }
            } else {
                console.warn(`Failed to fetch page ${link}: ${pageResponse.status}`);
            }
        } catch (err) {
            console.warn(`Thumbnail scrape කරන්න බැරි වුණා from ${link}: ${err.message}`);
        }


        await socket.sendMessage(sender, {
            image: { url: thumbnailUrl },
            caption: formatMessage(
                '📰QUEEN IMALSHA MD NEWS 📰',
                `📢 *${title}*\n\n${desc}\n\n🕒 *𝐃ate*: ${date || 'තවම ලබාදීලා නැත'}\n🌐 *Link*: ${link}`,
                '> *Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2*'
            )
        });
    } catch (error) {
        console.error(`Error in 'news' case: ${error.message}`);
        await socket.sendMessage(sender, {
            text: '⚠️ නිව්ස් ගන්න බැරි වුණා සුද්දෝ! 😩 යමක් වැරදුණා වගේ.'
        });
    }
                    break;
case 'deleteme': {
  // 'number' is the session number passed to setupCommandHandlers (sanitized in caller)
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  // determine who sent the command
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');

  // Permission: only the session owner or the bot OWNER can delete this session
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or the bot owner can delete this session.' }, { quoted: msg });
    break;
  }

  try {
    // 1) Remove from Mongo
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);

    // 2) Remove temp session dir
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try {
      if (fs.existsSync(sessionPath)) {
        fs.removeSync(sessionPath);
        console.log(`Removed session folder: ${sessionPath}`);
      }
    } catch (e) {
      console.warn('Failed removing session folder:', e);
    }

    // 3) Try to logout & close socket
    try {
      if (typeof socket.logout === 'function') {
        await socket.logout().catch(err => console.warn('logout error (ignored):', err?.message || err));
      }
    } catch (e) { console.warn('socket.logout failed:', e?.message || e); }
    try { socket.ws?.close(); } catch (e) { console.warn('ws close failed:', e?.message || e); }

    // 4) Remove from runtime maps
    activeSockets.delete(sanitized);
    socketCreationTime.delete(sanitized);

    // 5) notify user
    await socket.sendMessage(sender, {
      image: { url: config.RCD_IMAGE_PATH },
      caption: formatMessage('🗑️ SESSION DELETED', '✅ Your session has been successfully deleted from MongoDB and local storage.', BOT_NAME_FANCY)
    }, { quoted: msg });

    console.log(`Session ${sanitized} deleted by ${senderNum}`);
  } catch (err) {
    console.error('deleteme command error:', err);
    await socket.sendMessage(sender, { text: `❌ Failed to delete session: ${err.message || err}` }, { quoted: msg });
  }
  break;
  
}
case 'facebook':
case 'fbdl':
case 'fb': {
    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        const fbUrl = args.join(' ');
        if (!fbUrl || !fbUrl.startsWith('https://')) {
            return await socket.sendMessage(sender, {
                image: { url: config.RCD_IMAGE_PATH },
                caption: "❌ *Please provide a valid Facebook URL!*"
            });
        }

        await socket.sendMessage(sender, { react: { text: '⏳', key: msg.key } });

        const apiRes = await axios.get(`https://www.movanest.xyz/v2/fbdown?url=${encodeURIComponent(fbUrl)}`);
        
        if (!apiRes.data.status || !apiRes.data.results) {
            return await socket.sendMessage(sender, { text: "❌ *Failed to fetch Facebook data!*" }, { quoted: msg });
        }

        const result = apiRes.data.results[0]; 
        const menuImage = result.thumbnail || result.image || config.RCD_IMAGE_PATH;

        // මෙතනදී text message එක අයින් කර බොත්තම් පමණක් යැවීම
        const sentMsg = await socket.sendMessage(
            sender,
            {
                image: { url: menuImage },
                caption: `*ꜰᴀᴄᴋʙᴏᴏᴋ ᴅᴏᴡɴʟᴏᴀᴅ* 📥\n\n> *© ${botName}*`, 
                footer: `ᴄʜᴏᴏꜱᴇ ᴅᴏᴡɴʟᴏᴀᴅ ᴛʏᴘᴇ ʙᴇʟᴏᴡ👇`,
                buttons: [
                    { buttonId: 'fb_hd',     buttonText: { displayText: '🎬ʜᴅ ᴠɪᴅᴇᴏ' }, type: 1 },
                    { buttonId: 'fb_sd',     buttonText: { displayText: '🎥ꜱᴅ ᴠɪᴅᴇᴏ' }, type: 1 },
                    { buttonId: 'fb_audio',  buttonText: { displayText: '🎧ᴀᴜᴅɪᴏ' },    type: 1 },
                    { buttonId: 'fb_vnote',  buttonText: { displayText: '📝ᴠɪᴅᴇᴏ ɴᴏᴛᴇ' }, type: 1 },
                ],
                headerType: 4
            },
            { quoted: msg }
        );

        const messageID = sentMsg.key.id;

        const handleFBButtons = async ({ messages }) => {
            const up = messages[0];
            if (!up.message) return;

            const btn = up.message.buttonsResponseMessage || up.message.templateButtonReplyMessage;
            if (!btn) return;

            const isReplyToThis = btn.contextInfo?.stanzaId === messageID;
            if (!isReplyToThis || up.key.remoteJid !== sender) return;

            const buttonId = btn.selectedButtonId || btn.selectedId;
            await socket.sendMessage(sender, { react: { text: '⬇️', key: up.key } });

            let mediaPayload;

            if (buttonId === 'fb_hd') {
                mediaPayload = {
                    video: { url: result.hdQualityLink || result.normalQualityLink },
                    mimetype: 'video/mp4',
                    caption: `*✅ FB HD Video*\n\n*© ${botName}*`
                };
            } else if (buttonId === 'fb_sd') {
                mediaPayload = {
                    video: { url: result.normalQualityLink },
                    mimetype: 'video/mp4',
                    caption: `*✅ FB SD Video*\n\n*© ${botName}*`
                };
            } else if (buttonId === 'fb_audio') {
                mediaPayload = {
                    audio: { url: result.normalQualityLink },
                    mimetype: 'audio/mpeg',
                    fileName: `fb_audio.mp3`
                };
            } else if (buttonId === 'fb_vnote') {
                mediaPayload = {
                    video: { url: result.normalQualityLink },
                    mimetype: 'video/mp4',
                    ptv: true
                };
            }

            if (mediaPayload) {
                await socket.sendMessage(sender, mediaPayload, { quoted: up });
                await socket.sendMessage(sender, { react: { text: '✅', key: up.key } });
            }
        };

        socket.ev.on('messages.upsert', handleFBButtons);
        setTimeout(() => {
            socket.ev.removeListener('messages.upsert', handleFBButtons);
        }, 120000);

    } catch (err) {
        console.error("Error:", err);
        await socket.sendMessage(sender, { text: '*❌ Error! Try again later.*' });
    }
    break;
}
case 'cfn': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = await loadUserConfigFromMongo(sanitized) || {};
  const botName = cfg.botName || BOT_NAME_FANCY;
  const logo = cfg.logo || config.RCD_IMAGE_PATH;

  const full = body.slice(config.PREFIX.length + command.length).trim();
  if (!full) {
    await socket.sendMessage(sender, { text: `❗ Provide input: .cfn <jid@newsletter> | emoji1,emoji2\nExample: .cfn 120363402094635383@newsletter | 🔥,❤️` }, { quoted: msg });
    break;
  }

  const admins = await loadAdminsFromMongo();
  const normalizedAdmins = (admins || []).map(a => (a || '').toString());
  const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');
  const isAdmin = normalizedAdmins.includes(nowsender) || normalizedAdmins.includes(senderNumber) || normalizedAdmins.includes(senderIdSimple);
  if (!(isOwner || isAdmin)) {
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only owner or configured admins can add follow channels.' }, { quoted: msg });
    break;
  }
  let jidPart = full;
  let emojisPart = '';
  if (full.includes('|')) {
    const split = full.split('|');
    jidPart = split[0].trim();
    emojisPart = split.slice(1).join('|').trim();
  } else {
    const parts = full.split(/\s+/);
    if (parts.length > 1 && parts[0].includes('@newsletter')) {
      jidPart = parts.shift().trim();
      emojisPart = parts.join(' ').trim();
    } else {
      jidPart = full.trim();
      emojisPart = '';
    }
  }

  const jid = jidPart;
  if (!jid || !jid.endsWith('@newsletter')) {
    await socket.sendMessage(sender, { text: '❗ Invalid JID. Example: 120363406261194661@newsletter' }, { quoted: msg });
    break;
  }

  let emojis = [];
  if (emojisPart) {
    emojis = emojisPart.includes(',') ? emojisPart.split(',').map(e => e.trim()) : emojisPart.split(/\s+/).map(e => e.trim());
    if (emojis.length > 20) emojis = emojis.slice(0, 20);
  }

  try {
    if (typeof socket.newsletterFollow === 'function') {
      await socket.newsletterFollow(jid);
    }

    await addNewsletterToMongo(jid, emojis);

    const emojiText = emojis.length ? emojis.join(' ') : '(default set)';

    // Meta mention for botName
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CFN" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

    await socket.sendMessage(sender, {
      image: imagePayload,
      caption: `✅ Channel followed and saved!\n\nJID: ${jid}\nEmojis: ${emojiText}\nSaved by: @${senderIdSimple}`,
      footer: `🍁 ${botName} FOLLOW CHANNEL`,
      mentions: [nowsender], // user mention
      buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📒Mᴇɴᴜ" }, type: 1 }],
      headerType: 4
    }, { quoted: metaQuote }); // <-- botName meta mention

  } catch (e) {
    console.error('cfn error', e);
    await socket.sendMessage(sender, { text: `❌ Failed to save/follow channel: ${e.message || e}` }, { quoted: msg });
  }
  break;
}

case 'chr': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const cfg = await loadUserConfigFromMongo(sanitized) || {};
  const botName = cfg.botName || BOT_NAME_FANCY;
  const logo = cfg.logo || config.RCD_IMAGE_PATH;

  const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');

  const q = body.split(' ').slice(1).join(' ').trim();
  if (!q.includes(',')) return await socket.sendMessage(sender, { text: "❌ Usage: chr <channelJid/messageId>,<emoji>" }, { quoted: msg });

  const parts = q.split(',');
  let channelRef = parts[0].trim();
  const reactEmoji = parts[1].trim();

  let channelJid = channelRef;
  let messageId = null;
  const maybeParts = channelRef.split('/');
  if (maybeParts.length >= 2) {
    messageId = maybeParts[maybeParts.length - 1];
    channelJid = maybeParts[maybeParts.length - 2].includes('@newsletter') ? maybeParts[maybeParts.length - 2] : channelJid;
  }

  if (!channelJid.endsWith('@newsletter')) {
    if (/^\d+$/.test(channelJid)) channelJid = `${channelJid}@newsletter`;
  }

  if (!channelJid.endsWith('@newsletter') || !messageId) {
    return await socket.sendMessage(sender, { text: '❌ Provide channelJid/messageId format.' }, { quoted: msg });
  }

  try {
    await socket.newsletterReactMessage(channelJid, messageId.toString(), reactEmoji);
    await saveNewsletterReaction(channelJid, messageId.toString(), reactEmoji, sanitized);

    // BotName meta mention
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_CHR" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

    await socket.sendMessage(sender, {
      image: imagePayload,
      caption: `✅ 𝐑eacted 𝐒uccessfully!\n\n𝐂hannel: ${channelJid}\n*𝐌essage:* ${messageId}\n*𝐄moji:* ${reactEmoji}\nBy: @${senderIdSimple}`,
      footer: `🍁 ${botName} REACTION`,
      mentions: [nowsender], // user mention
      buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📒Mᴇɴᴜ" }, type: 1 }],
      headerType: 4
    }, { quoted: metaQuote }); // <-- botName meta mention

  } catch (e) {
    console.error('chr command error', e);
    await socket.sendMessage(sender, { text: `❌ Failed to react: ${e.message || e}` }, { quoted: msg });
  }
  break;
}
case 'cvideo': 
case 'queenv3': {
    const axios = require('axios');
    const botLogoUrl = 'https://files.catbox.moe/q0s5d7.jpeg'; 

    await socket.sendMessage(sender, { react: { text: '🛡️', key: msg.key } });
    
    const fullText = body.slice(config.PREFIX.length + command.length).trim();
    
    // Command එක split කරගන්නවා: JID | Caption | URL
    const parts = fullText.split('|').map(p => p.trim());
    const targetJid = parts[0];
    const userCustomCaption = parts[1]; // ඔයා ලියන caption එක
    const url = parts[2];

    if (!targetJid || !userCustomCaption || !url) {
        return await socket.sendMessage(sender, {
            text: '*📌 Usage:* .cvideo <JID> | <Your Caption> | <TikTok_URL>\n\n*Example:* .cvideo 123456@newsletter | මරු වීඩියෝ එක | https://vt.tiktok.com/ZSxxx/'
        }, { quoted: msg });
    }

    try {
        await socket.sendMessage(sender, { text: `*⏳ Processing & Uploading to Channel...*` }, { quoted: msg });
        
        const downloadUrl = `https://tikwm.com/api/?url=${encodeURIComponent(url)}&hd=1`;
        const response = await axios.get(downloadUrl);
        const videoData = response.data.data;
        
        if (!videoData || !videoData.play) throw new Error('Video fetch failed');

        const videoUrl = videoData.hdplay || videoData.play;
        const videoTitle = videoData.title || 'TikTok Video';

        const sanitized = (sender || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || '𝑸𝑼𝑬𝑬𝑵 𝑰𝑴𝑨𝑳𝑺𝑯𝑨 𝑴𝑫 𝑪𝑽𝑰𝑫𝑬𝑶 𝑼𝑷𝑳𝑶𝑨𝑫𝑰𝑵𝑮';

        const senderName = msg.pushName || 'User'; 
        let channelName = 'Channel';
        
        try {
            const metadata = await socket.newsletterMetadata("jid", targetJid);
            channelName = metadata.name || 'Our Channel';
        } catch (e) {
            channelName = 'Channel';
        }

        // --- 1. Channel එකට යන Caption එක (ඔයා ලියපු එක + Auto තොරතුරු) ---

const finalCaption = `💗🫣 \`*${channelName}*\` 🙈
*┌⬡⊷*
*┋▢💗Cᴀᴘɪᴛɪᴏɴ:* \`${userCustomCaption}\`
*┋▢👤Aᴜᴛʜᴏʀ:* \`${videoData.author?.nickname || 'Unknown'}\`
*┋▢📤Pᴏꜱᴛᴇᴅ Bʏ:* \`${senderName}\`
*└⬡⊷*

\`💗🙈 ලස්සන ලස්සන tiktok විඩියො බලන්න අපිව ෆලොව් කරන්න අනේ... අනේ අනේ බබෝ...\`

> *${botName}*`;

        // --- 2. Channel එකට වීඩියෝ එක යැවීම ---
        await socket.sendMessage(targetJid, {
            video: { url: videoUrl },
            caption: finalCaption,
            contextInfo: {
                externalAdReply: {
                    title: videoTitle,
                    body: `Uᴘʟᴏᴀᴅɪɴ ʙʏ ${botName}`,
                    thumbnailUrl: videoData.cover, 
                    sourceUrl: url,
                    mediaType: 1,
                    renderLargerThumbnail: false 
                }
            }
        });

        // --- 3. User හට ලැබෙන Success Message එක ---
        const successText = `✅ *Video Successfully Sent!*\n\n` +
                            `*📍 Target:* ${channelName}\n` +
                            `*🎬 Title:* ${videoTitle}\n\n` +
                            `> *${botName}*`;
        
        await socket.sendMessage(sender, { 
            image: { url: botLogoUrl }, 
            caption: successText 
        }, { quoted: msg });
        
    } catch (error) {
        console.error('Channel Upload Error:', error);
        await socket.sendMessage(sender, { 
            text: `❌ *Error Occurred!*\n\nJID එක නිවැරදිද සහ Bot එම චැනල් එකේ Admin ද බලන්න.` 
        }, { quoted: msg });
    }
    break;
}
case 'csong':
case 'csend': {
    try {
        const axios = require('axios');
        const yts = require('yt-search');
        const os = require('os');
        const path = require('path');
        const fs = require('fs');
        const crypto = require('crypto');
        const ffmpeg = require('fluent-ffmpeg');
        const ffmpegPath = require('ffmpeg-static');

        // Config - අලුත් API විස්තර
        const API_KEY = 'chama_f7b61e81701eeb7e860025e9f5af4f77';
        const BASE_URL = 'https://chama-api-hub.vercel.app/api/mp3_v3';

        if (args.length < 2) return await socket.sendMessage(sender, { text: "❌ *Ex:* .csong [Channel_JID] [Song Name]" }, { quoted: msg });
        
        const targetJid = args[0]; 
        const songQuery = args.slice(1).join(' '); 
        await socket.sendMessage(sender, { react: { text: '🎧', key: msg.key } });

        const senderName = msg.pushName || "Admin";

        // Channel නම ලබා ගැනීම
        let channelName = "Our Channel";
        try {
            const metadata = await socket.newsletterMetadata("jid", targetJid).catch(() => null);
            if (metadata) channelName = metadata.name;
        } catch (e) { channelName = "Community"; }

        // --- අකුරු Small Caps කරන Logic එක ---
        const toSmallCaps = (text) => {
            const fonts = {
                'a': 'ᴀ', 'b': 'ʙ', 'c': 'ᴄ', 'd': 'ᴅ', 'e': 'ᴇ', 'f': 'ғ', 'g': 'ɢ', 'h': 'ʜ', 'i': 'ɪ', 'j': 'ᴊ', 'k': 'ᴋ', 'l': 'ʟ', 'm': 'ᴍ', 'n': 'ɴ', 'o': 'ᴏ', 'p': 'ᴘ', 'q': 'ǫ', 'r': 'ʀ', 's': 's', 't': 'ᴛ', 'u': 'ᴜ', 'v': 'ᴠ', 'w': 'ᴡ', 'x': 'x', 'y': 'ʏ', 'z': 'ᴢ'
            };
            return text.toLowerCase().split('').map(char => fonts[char] || char).join('');
        };

        const upperChannelName = toSmallCaps(channelName);

        // Search Song
        const searchRes = await yts(songQuery);
        const videoInfo = searchRes.videos[0];
        if (!videoInfo) return await socket.sendMessage(sender, { text: "❌ මට ඒ සින්දුව හොයාගන්න බැරි වුණා." }, { quoted: msg });

        // --- Download Link ලබා ගැනීම (Updated API) ---
        let downloadUrl;
        try {
            const finalApiUrl = `${BASE_URL}?apikey=${API_KEY}&url=${encodeURIComponent(videoInfo.url)}`;
            const apiRes = await axios.get(finalApiUrl);
            
            // API response එක අනුව ලින්ක් එක ලබා ගැනීම
            downloadUrl = apiRes.data.result?.download_url || apiRes.data.result?.dl_url || apiRes.data.url || apiRes.data.link;
        } catch (apiErr) {
            throw new Error("Song searching (Error 500)");
        }

        if (!downloadUrl) throw new Error("Song download කරගැනීමේ ලින්ක් එක සොයාගත නොහැකි විය.");

        // File Conversion (MP3 to Opus/PTT)
        const tmpId = crypto.randomBytes(8).toString('hex');
        const tempMp3 = path.join(os.tmpdir(), `in_${tmpId}.mp3`);
        const tempOpus = path.join(os.tmpdir(), `out_${tmpId}.opus`);

        const resp = await axios.get(downloadUrl, { responseType: 'arraybuffer' });
        fs.writeFileSync(tempMp3, Buffer.from(resp.data));

        if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);
        await new Promise((resolve, reject) => {
            ffmpeg(tempMp3).noVideo().audioCodec('libopus').format('opus')
                .on('end', () => resolve())
                .on('error', (err) => reject(err))
                .save(tempOpus);
        });

        const opusBuffer = fs.readFileSync(tempOpus);

        // --- පණිවිඩය සැකසීම ---
        const personalCaption = 
                                `┌───❑ ${upperChannelName} ❑───\n` +
                                `├ ▢ ᴛɪᴛʟᴇ : ${videoInfo.title}\n` +
                                `├ ▢ ᴀʀᴛɪsᴛ : ${videoInfo.author.name}\n` +
                                `├ ▢ ᴠɪᴇᴡs : ${videoInfo.views.toLocaleString()}\n` +
                                `└ ▢ ᴅᴀᴛᴇ : ${videoInfo.ago}\n\n` +
                                `❑🫣අනේ අනේ ඉතිම් සිංදූ අහලා ලස්සන රැයැක්ට් ගොඩක් දගෙන යන්න අලේ ඉතිම්🥺💗\n` +
                                `❑ ʟɪɴᴋ : ${videoInfo.url}\n\n` +
                                `📍 ᴘᴏsᴛᴇᴅ ɪɴ : ${channelName}\n` +
                                `> sʜᴀʀᴇᴅ ʙʏ ${senderName} 🎀`;

        // Channel එකට Thumbnail එක යැවීම
        await socket.sendMessage(targetJid, { 
            image: { url: videoInfo.thumbnail }, 
            caption: personalCaption 
        });

        // Channel එකට Voice (PTT) එක යැවීම
        await socket.sendMessage(targetJid, { 
            audio: opusBuffer, 
            mimetype: "audio/ogg; codecs=opus", 
            ptt: true 
        });

        // සාර්ථක පණිවිඩය
        await socket.sendMessage(sender, { 
            text: `✅ *${videoInfo.title}* Sᴇɴᴅ Yᴏᴜ Sᴏɴɢ Cʜᴀɴɴᴇʟ Dᴏɴᴇ💗🥺` 
        }, { quoted: msg });

        // තාවකාලික ෆයිල් ඉවත් කිරීම
        try { fs.unlinkSync(tempMp3); fs.unlinkSync(tempOpus); } catch (e) {}

    } catch (e) { 
        console.error(e);
        await socket.sendMessage(sender, { text: '❌ ' + e.message }, { quoted: msg });
    }
    break;
}

case 'gsong': {
    try {
        const axios = require('axios');
        const yts = require('yt-search');

        // Config - ඔබ ලබාදුන් API තොරතුරු
        const API_KEY = 'chama_f7b61e81701eeb7e860025e9f5af4f77'; 
        const BASE_URL = 'https://chama-api-hub.vercel.app/api/mp3_v3';

        // Input Check: .gsong [JID] [Song Name]
        if (args.length < 2) {
            return await socket.sendMessage(sender, { 
                text: "❌ *භාවිතා කරන ආකාරය:* .gsong [Group_JID] [Song Name]\n\n*උදා:* .gsong 12036304@g.us Alone" 
            }, { quoted: msg });
        }

        const targetJid = args[0]; // පළමු argument එක JID එක ලෙස ගනී
        const songQuery = args.slice(1).join(' '); // ඉතිරි සියල්ල සින්දුවේ නම ලෙස ගනී

        // Reaction එකක් දැමීම (Sender වෙත)
        await socket.sendMessage(sender, { react: { text: '⏳', key: msg.key } });

        // YouTube Search
        const search = await yts(songQuery);
        const data = search.videos[0];
        if (!data) return await socket.sendMessage(sender, { text: "❌ *සොයාගත නොහැකි විය!*" }, { quoted: msg });

        const videoUrl = data.url;

        // Group එකට Thumbnail සහ විස්තර යැවීම
        const desc = `🎧 *𝑺𝑨𝑵𝑼 𝑿 𝑴𝑰𝑵𝑰 𝑩𝑶𝑻 𝑮𝑺𝑶𝑵𝑮*\n\n📝 *𝑻𝒊𝒕𝒍𝒆:* ${data.title}\n⏱️ *𝑫𝒖𝒓𝒂𝒕𝒊𝒐𝒏:* ${data.timestamp}\n🔗 *Link:* ${videoUrl}\n\n> *𝑺𝒆𝒏𝒅𝒊𝒏𝒈 𝑨𝒖𝒅𝒊𝒐 𝑻𝒐 𝑮𝒓𝒐𝒖𝒑.....* 📥`;
        
        await socket.sendMessage(targetJid, { 
            image: { url: data.thumbnail }, 
            caption: desc,
            contextInfo: {
                externalAdReply: {
                    title: data.title,
                    body: "𝐒ᴀɴᴜ x 𝐌ɪɴɪ 𝐁ᴏᴛ📡ᴠ3",
                    thumbnailUrl: data.thumbnail,
                    mediaType: 1,
                    showAdAttribution: true,
                    renderLargerThumbnail: true,
                    sourceUrl: videoUrl
                }
            }
        });

        // API URL එක සෑදීම
        const finalApiUrl = `${BASE_URL}?apikey=${API_KEY}&url=${encodeURIComponent(videoUrl)}`;
        const apiRes = await axios.get(finalApiUrl);
        
        // API Response එකෙන් Link එක ලබා ගැනීම
        const downloadUrl = apiRes.data.result?.download_url || apiRes.data.result?.dl_url || apiRes.data.url;

        if (!downloadUrl) throw new Error("Download URL not found");

        // Audio එක Download කර Group එකට යැවීම
        const audioRes = await axios.get(downloadUrl, { responseType: 'arraybuffer' });
        const audioBuffer = Buffer.from(audioRes.data);

        await socket.sendMessage(targetJid, {
            audio: audioBuffer,
            mimetype: "audio/mpeg",
            fileName: `${data.title}.mp3`
        });

        // සාර්ථක බව Sender හට දැනුම් දීම
        await socket.sendMessage(sender, { 
            text: `✅ *Successfully Sent!* \n🎵 *Song:* ${data.title}\n👥 *To JID:* ${targetJid}` 
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (e) {
        console.error("GSong error:", e);
        await socket.sendMessage(sender, { text: `❌ දෝෂයක් සිදුවිය: ${e.message}` }, { quoted: msg });
    }
    break;
}
case 'pp': {
  try {
    const q = args.join(' ');
    if (!q) {
      return socket.sendMessage(sender, {
        text: '❎ Please enter a pastpaper search term!\n\nExample: .pp o/l ict'
      }, { quoted: msg });
    }

    // Short reaction to show we're working
    await socket.sendMessage(sender, { react: { text: '🔎', key: msg.key } });

    // Search API (you provided)
    const searchApi = `https://pp-api-beta.vercel.app/api/pastpapers?q=${encodeURIComponent(q)}`;
    const { data } = await axios.get(searchApi);

    if (!data?.results || data.results.length === 0) {
      return socket.sendMessage(sender, { text: '❎ No results found for that query!' }, { quoted: msg });
    }

    // Filter out generic pages like Next Page / Contact Us / Terms / Privacy
    const filtered = data.results.filter(r => {
      const t = (r.title || '').toLowerCase();
      if (!r.link) return false;
      if (t.includes('next page') || t.includes('contact us') || t.includes('terms') || t.includes('privacy policy')) return false;
      return true;
    });

    if (filtered.length === 0) {
      return socket.sendMessage(sender, { text: '❎ No relevant pastpaper results found.' }, { quoted: msg });
    }

    // Take top 5 results
    const results = filtered.slice(0, 5);

    // Build caption
    let caption = `📚 *Top Pastpaper Results for:* ${q}\n\n`;
    results.forEach((r, i) => {
      caption += `*${i + 1}. ${r.title}*\n🔗 Preview: ${r.link}\n\n`;
    });
    caption += `*💬 Reply with number (1-${results.length}) to download/view.*`;

    // Send first result image if any thumbnail, else just send text with first link preview
    let sentMsg;
    if (results[0].thumbnail) {
      sentMsg = await socket.sendMessage(sender, {
        image: { url: results[0].thumbnail },
        caption
      }, { quoted: msg });
    } else {
      sentMsg = await socket.sendMessage(sender, {
        text: caption
      }, { quoted: msg });
    }

    // Listener for user choosing an item (1..n)
    const listener = async (update) => {
      try {
        const m = update.messages[0];
        if (!m.message) return;

        const text = m.message.conversation || m.message.extendedTextMessage?.text;
        const isReply =
          m.message.extendedTextMessage &&
          m.message.extendedTextMessage.contextInfo?.stanzaId === sentMsg.key.id;

        if (isReply && ['1','2','3','4','5'].includes(text)) {
          const index = parseInt(text, 10) - 1;
          const selected = results[index];
          if (!selected) return;

          // show processing reaction
          await socket.sendMessage(sender, { react: { text: '⏳', key: m.key } });

          // Call download API to get direct pdf(s)
          try {
            const dlApi = `https://pp-api-beta.vercel.app/api/download?url=${encodeURIComponent(selected.link)}`;
            const { data: dlData } = await axios.get(dlApi);

            if (!dlData?.found || !dlData.pdfs || dlData.pdfs.length === 0) {
              await socket.sendMessage(sender, { react: { text: '❌', key: m.key } });
              await socket.sendMessage(sender, { text: '❎ No direct PDF found for that page.' }, { quoted: m });
              // cleanup
              socket.ev.off('messages.upsert', listener);
              return;
            }

            const pdfs = dlData.pdfs; // array of URLs

            if (pdfs.length === 1) {
              // single pdf -> send directly
              const pdfUrl = pdfs[0];
              await socket.sendMessage(sender, { react: { text: '⬇️', key: m.key } });

              await socket.sendMessage(sender, {
                document: { url: pdfUrl },
                mimetype: 'application/pdf',
                fileName: `${selected.title}.pdf`,
                caption: `📄 ${selected.title}`
              }, { quoted: m });

              await socket.sendMessage(sender, { react: { text: '✅', key: m.key } });

              socket.ev.off('messages.upsert', listener);
            } else {
              // multiple pdfs -> list options and wait for choose
              let desc = `📄 *${selected.title}* — multiple PDFs found:\n\n`;
              pdfs.forEach((p, i) => {
                desc += `*${i+1}.* ${p.split('/').pop() || `PDF ${i+1}`}\n`;
              });
              desc += `\n💬 Reply with number (1-${pdfs.length}) to download that PDF.`;

              const infoMsg = await socket.sendMessage(sender, {
                text: desc
              }, { quoted: m });

              // nested listener for pdf choice
              const dlListener = async (dlUpdate) => {
                try {
                  const d = dlUpdate.messages[0];
                  if (!d.message) return;

                  const text2 = d.message.conversation || d.message.extendedTextMessage?.text;
                  const isReply2 =
                    d.message.extendedTextMessage &&
                    d.message.extendedTextMessage.contextInfo?.stanzaId === infoMsg.key.id;

                  if (isReply2) {
                    if (!/^\d+$/.test(text2)) return;
                    const dlIndex = parseInt(text2, 10) - 1;
                    if (dlIndex < 0 || dlIndex >= pdfs.length) {
                      return socket.sendMessage(sender, { text: '❎ Invalid option.' }, { quoted: d });
                    }

                    const finalPdf = pdfs[dlIndex];
                    await socket.sendMessage(sender, { react: { text: '⬇️', key: d.key } });

                    try {
                      await socket.sendMessage(sender, {
                        document: { url: finalPdf },
                        mimetype: 'application/pdf',
                        fileName: `${selected.title} (${dlIndex+1}).pdf`,
                        caption: `📄 ${selected.title} (${dlIndex+1})`
                      }, { quoted: d });

                      await socket.sendMessage(sender, { react: { text: '✅', key: d.key } });
                    } catch (err) {
                      await socket.sendMessage(sender, { react: { text: '❌', key: d.key } });
                      await socket.sendMessage(sender, { text: `❌ Download/send failed.\n\nDirect link:\n${finalPdf}` }, { quoted: d });
                    }

                    socket.ev.off('messages.upsert', dlListener);
                    socket.ev.off('messages.upsert', listener);
                  }
                } catch (err) {
                  // ignore inner errors but log if you want
                }
              };

              socket.ev.on('messages.upsert', dlListener);
              // keep outer listener off until user chooses or we cleanup inside dlListener
            }

          } catch (err) {
            await socket.sendMessage(sender, { react: { text: '❌', key: m.key } });
            await socket.sendMessage(sender, { text: `❌ Error fetching PDF: ${err.message}` }, { quoted: m });
            socket.ev.off('messages.upsert', listener);
          }
        }
      } catch (err) {
        // ignore per-message listener errors
      }
    };

    socket.ev.on('messages.upsert', listener);

  } catch (err) {
    await socket.sendMessage(sender, { react: { text: '❌', key: msg.key } });
    await socket.sendMessage(sender, { text: `❌ ERROR: ${err.message}` }, { quoted: msg });
  }
  break;
}

case 'apkdownload':
case 'apk': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const id = text.split(" ")[1]; // .apkdownload <id>

        // ✅ Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        // ✅ Fake Meta contact message
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_APKDL"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        if (!id) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please provide an APK package ID.*\n\nExample: .apkdownload com.whatsapp',
                buttons: [
                    { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📒Mᴇɴᴜ' }, type: 1 }
                ]
            }, { quoted: shonux });
        }

        // ⏳ Notify start
        await socket.sendMessage(sender, { text: '*⏳ Fetching APK info...*' }, { quoted: shonux });

        // 🔹 Call API
        const apiUrl = `https://tharuzz-ofc-apis.vercel.app/api/download/apkdownload?id=${encodeURIComponent(id)}`;
        const { data } = await axios.get(apiUrl);

        if (!data.success || !data.result) {
            return await socket.sendMessage(sender, { text: '*❌ Failed to fetch APK info.*' }, { quoted: shonux });
        }

        const result = data.result;
        const caption = `📱 *${result.name}*\n\n` +
                        `*🆔 𝐏ackage:* \`${result.package}\`\n` +
                        `*📦 𝐒ize:* ${result.size}\n` +
                        `*🕒 𝐋ast 𝐔pdate:* ${result.lastUpdate}\n\n` +
                        `*✅ 𝐃ownloaded 𝐁y:* ${botName}`;

        // 🔹 Send APK as document
        await socket.sendMessage(sender, {
            document: { url: result.dl_link },
            fileName: `${result.name}.apk`,
            mimetype: 'application/vnd.android.package-archive',
            caption: caption,
            jpegThumbnail: result.image ? await axios.get(result.image, { responseType: 'arraybuffer' }).then(res => Buffer.from(res.data)) : undefined
        }, { quoted: shonux });

    } catch (err) {
        console.error("Error in APK download:", err);

        // Catch block Meta mention
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_APKDL"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
    }
    break;
}

case 'xv':
case 'xvsearch':
case 'xvdl': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const query = text.split(" ").slice(1).join(" ").trim();

        // ✅ Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        // ✅ Fake Meta contact message
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_XV"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        if (!query) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please provide a search query.*\n\nExample: .xv mia',
                buttons: [
                    { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📒Mᴇɴᴜ' }, type: 1 }
                ]
            }, { quoted: shonux });
        }

        await socket.sendMessage(sender, { text: '*⏳ Searching XVideos...*' }, { quoted: shonux });

        // 🔹 Search API
        const searchUrl = `https://tharuzz-ofc-api-v2.vercel.app/api/search/xvsearch?query=${encodeURIComponent(query)}`;
        const { data } = await axios.get(searchUrl);

        if (!data.success || !data.result?.xvideos?.length) {
            return await socket.sendMessage(sender, { text: '*❌ No results found.*' }, { quoted: shonux });
        }

        // 🔹 Show top 10 results
        const results = data.result.xvideos.slice(0, 10);
        let listMessage = `🔍 *𝐗videos 𝐒earch 𝐑esults 𝐅or:* ${query}\n\n`;
        results.forEach((item, idx) => {
            listMessage += `*${idx + 1}.* ${item.title}\n${item.info}\n➡️ ${item.link}\n\n`;
        });
        listMessage += `*𝐏owered 𝐁y ${botName}*`;

        await socket.sendMessage(sender, {
            text: listMessage,
            buttons: [
                { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📒Mᴇɴᴜ' }, type: 1 }
            ],
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: shonux });

        // 🔹 Store search results for reply handling
        global.xvReplyCache = global.xvReplyCache || {};
        global.xvReplyCache[sender] = results.map(r => r.link);

    } catch (err) {
        console.error("Error in XVideos search/download:", err);
        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
    }
}
break;

// ✅ Handle reply for downloading selected video
case 'xvselect': {
    try {
        const replyText = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const selection = parseInt(replyText);

        const links = global.xvReplyCache?.[sender];
        if (!links || isNaN(selection) || selection < 1 || selection > links.length) {
            return await socket.sendMessage(sender, { text: '🚫 Invalid selection number.' }, { quoted: msg });
        }

        const videoUrl = links[selection - 1];
        await socket.sendMessage(sender, { text: '*⏳ Downloading video...*' }, { quoted: msg });

        // 🔹 Call XVideos download API
        const dlUrl = `https://tharuzz-ofc-api-v2.vercel.app/api/download/xvdl?url=${encodeURIComponent(videoUrl)}`;
        const { data } = await axios.get(dlUrl);

        if (!data.success || !data.result) {
            return await socket.sendMessage(sender, { text: '*❌ Failed to fetch video.*' }, { quoted: msg });
        }

        const result = data.result;
        await socket.sendMessage(sender, {
            video: { url: result.dl_Links.highquality || result.dl_Links.lowquality },
            caption: `🎥 *${result.title}*\n\n⏱ Duration: ${result.duration}s\n\n_© Powered by ${botName}_`,
            jpegThumbnail: result.thumbnail ? await axios.get(result.thumbnail, { responseType: 'arraybuffer' }).then(res => Buffer.from(res.data)) : undefined
        }, { quoted: msg });

        // 🔹 Clean cache
        delete global.xvReplyCache[sender];

    } catch (err) {
        console.error("Error in XVideos selection/download:", err);
        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: msg });
    }
}
break;


case 'දාපන්':
case 'ඔන':
case 'vv':
case 'save': {
  try {
    const quotedMsg = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
    if (!quotedMsg) {
      return await socket.sendMessage(sender, { text: '*❌ Please reply to a message (status/media) to save it.*' }, { quoted: msg });
    }

    try { await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } }); } catch(e){}

    // 🟢 Instead of bot’s own chat, use same chat (sender)
    const saveChat = sender;

    if (quotedMsg.imageMessage || quotedMsg.videoMessage || quotedMsg.audioMessage || quotedMsg.documentMessage || quotedMsg.stickerMessage) {
      const media = await downloadQuotedMedia(quotedMsg);
      if (!media || !media.buffer) {
        return await socket.sendMessage(sender, { text: '❌ Failed to download media.' }, { quoted: msg });
      }

      if (quotedMsg.imageMessage) {
        await socket.sendMessage(saveChat, { image: media.buffer, caption: media.caption || '✅ Status Saved' });
      } else if (quotedMsg.videoMessage) {
        await socket.sendMessage(saveChat, { video: media.buffer, caption: media.caption || '✅ Status Saved', mimetype: media.mime || 'video/mp4' });
      } else if (quotedMsg.audioMessage) {
        await socket.sendMessage(saveChat, { audio: media.buffer, mimetype: media.mime || 'audio/mp4', ptt: media.ptt || false });
      } else if (quotedMsg.documentMessage) {
        const fname = media.fileName || `saved_document.${(await FileType.fromBuffer(media.buffer))?.ext || 'bin'}`;
        await socket.sendMessage(saveChat, { document: media.buffer, fileName: fname, mimetype: media.mime || 'application/octet-stream' });
      } else if (quotedMsg.stickerMessage) {
        await socket.sendMessage(saveChat, { image: media.buffer, caption: media.caption || '✅ Sticker Saved' });
      }

      await socket.sendMessage(sender, { text: '🔥 *𝐒tatus 𝐒aved 𝐒uccessfully!*' }, { quoted: msg });

    } else if (quotedMsg.conversation || quotedMsg.extendedTextMessage) {
      const text = quotedMsg.conversation || quotedMsg.extendedTextMessage.text;
      await socket.sendMessage(saveChat, { text: `✅ *𝐒tatus 𝐒aved*\n\n${text}` });
      await socket.sendMessage(sender, { text: '🔥 *𝐓ext 𝐒tatus 𝐒aved 𝐒uccessfully!*' }, { quoted: msg });
    } else {
      if (typeof socket.copyNForward === 'function') {
        try {
          const key = msg.message?.extendedTextMessage?.contextInfo?.stanzaId || msg.key;
          await socket.copyNForward(saveChat, msg.key, true);
          await socket.sendMessage(sender, { text: '🔥 *𝐒aved (𝐅orwarded) 𝐒uccessfully!*' }, { quoted: msg });
        } catch (e) {
          await socket.sendMessage(sender, { text: '❌ Could not forward the quoted message.' }, { quoted: msg });
        }
      } else {
        await socket.sendMessage(sender, { text: '❌ Unsupported quoted message type.' }, { quoted: msg });
      }
    }

  } catch (error) {
    console.error('❌ Save error:', error);
    await socket.sendMessage(sender, { text: '*❌ Failed to save status*' }, { quoted: msg });
  }
  break;
}

case 'alive': {
    const fs = require('fs');
    const path = require('path');
    const ffmpeg = require('fluent-ffmpeg');
    const ffmpegPath = require('ffmpeg-static');
    const axios = require('axios');

    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const cfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = cfg.botName || BOT_NAME_FANCY;
        const logo = cfg.logo || config.RCD_IMAGE_PATH;

        
        const aliveAudioUrl = "https://www.movanest.xyz/YsekEa.mp3";
        const tempMp3 = path.join(__dirname, `temp_${Date.now()}.mp3`);
        const tempOpus = path.join(__dirname, `temp_${Date.now()}.opus`);

        
        const metaQuote = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ALIVE" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
        };

        
        const startTime = socketCreationTime.get(number) || Date.now();
        const uptime = Math.floor((Date.now() - startTime) / 1000);
        const hours = Math.floor(uptime / 3600);
        const minutes = Math.floor((uptime % 3600) / 60);
        const seconds = Math.floor(uptime % 60);

        const text = `𝑯𝒆𝒍𝒍𝒐𝒘 ${botName} 𝑼𝒔𝒆𝒓 𝑰 𝒂𝒎 𝑨𝒍𝒊𝒗𝒆 𝑪𝒎𝒅 𝑰𝒔 𝑾𝒆𝒍𝒄𝒐𝒎𝒆 𝑴𝒖𝒍𝒊𝒕𝒆 𝑩𝒐𝒕𝒔

*╭⬡─────────────┈⊷*
*┋𝑸𝑼𝑬𝑬𝑵 𝑰𝑴𝑨𝑳𝑺𝑯𝑨 𝑴𝑫 𝑨𝑳𝑰𝑽𝑬*
*┋┌────────────⊷
*┋▢👤ᴜꜱᴇʀ:*  
*┋▢👑ᴏᴡɴᴇʀ:* ${config.OWNER_NAME || 'ꜱᴀɴᴜ'}  
*┋▢⚙️ᴘʀᴇꜰɪx:* .  
*┋▢🧬ᴠᴇʀꜱɪᴏɴ:* 2.0.0  
*┋▢💻ᴘʟᴀᴛꜰʀᴏᴍ:* ${process.env.PLATFORM || 'Heroku'}  
*┋▢⏱️ᴜᴘᴛɪᴍᴇ:* ${hours}h ${minutes}m ${seconds}s 
*┋└────────────⊷ 
*╰⬡──────────────⊷*  

> ❯ ᴘᴏᴡᴇʀᴅ ʙʏ Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2`;

        const buttons = [
            { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📜Mᴇɴᴜ" }, type: 1 },
            { buttonId: `${config.PREFIX}owner`, buttonText: { displayText: "👑Oᴡɴᴇʀ" }, type: 1 }
        ];

        let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

        // 1. මුලින්ම Image සහ Text පණිවිඩය යැවීම
        await socket.sendMessage(sender, {
            image: imagePayload,
            caption: text,
            footer: ` *${botName} ᴀʟɪᴠᴇ ᴜꜱᴇʀ ʜʏ🎀🙊*`,
            buttons,
            headerType: 4
        }, { quoted: metaQuote });

        // 2. Audio එක Download කර Convert කිරීම
        const response = await axios({ method: 'get', url: aliveAudioUrl, responseType: 'arraybuffer' });
        fs.writeFileSync(tempMp3, Buffer.from(response.data));

        if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);

        await new Promise((resolve, reject) => {
            ffmpeg(tempMp3)
                .noVideo()
                .audioCodec('libopus')
                .format('opus')
                .on('end', () => resolve())
                .on('error', (err) => reject(err))
                .save(tempOpus);
        });

        // 3. Convert වූ Voice Note එක යැවීම
        const opusBuffer = fs.readFileSync(tempOpus);
        await socket.sendMessage(sender, { 
            audio: opusBuffer, 
            mimetype: 'audio/ogg; codecs=opus', 
            ptt: true 
        }, { quoted: metaQuote });

        // Temporary Files ඉවත් කිරීම
        if (fs.existsSync(tempMp3)) fs.unlinkSync(tempMp3);
        if (fs.existsSync(tempOpus)) fs.unlinkSync(tempOpus);

    } catch (e) {
        console.error('alive error', e);
        await socket.sendMessage(sender, { text: '❌ Failed to send alive status.' }, { quoted: msg });
    }
    break;
}
case 'ping': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = "Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ ᴠ3.0.0💗";

    // 1. Catalog Style Quote (dtzminibot) සැකසීම
    const dtzminibot = {
      key: {
        fromMe: false,
        participant: '0@s.whatsapp.net',
        remoteJid: "status@broadcast"
      },
      message: {
        orderMessage: {
          orderId: "9999",
          thumbnail: null,
          itemCount: 999,
          status: 1,
          surface: 1,
          message: `Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ ᴠ3.0.0💗`,
          orderTitle: 'ʙᴏᴛ ᴏɴʟɪɴᴇ',
          sellerJid: '94783162197@s.whatsapp.net',
          token: "AR6xBKbXZn0Xwmu76Ksyd7rnxI+Rx87HfinVlW4lwXa6JA=="
        }
      },
      contextInfo: {
        forwardingScore: 999,
        isForwarded: true
      }
    };

    // 2. Loading Message with Edit
    const { key } = await socket.sendMessage(sender, { text: 'Lᴏᴀᴅɪɴɢ....' });
    
    for (let i = 10; i <= 100; i += 10) {
      await new Promise(resolve => setTimeout(resolve, 150)); 
      await socket.sendMessage(sender, { 
        text: `Lᴏᴀᴅɪɴɢ Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ ᴘɪɴɢ❍.....${i}%`, 
        edit: key 
      });
    }

    const latency = Date.now() - (msg.messageTimestamp * 1000 || Date.now());

    // 3. Main Catalog Message
    const catalogMsg = await socket.sendMessage(sender, {
      text: `⚡ *${botName}ᴘɪɴɢ*\n\n🏓ʟᴀᴛᴇɴᴄʏ: ${latency}ᴍꜱ\n⏱ ꜱᴇʀᴠᴇ ᴛɪᴍᴇ: ${new Date().toLocaleString()}`,
      footer: `${botName}`,
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "ᴠ3.0.0💗ᴍᴇɴᴜ ᴄᴍᴅ" }, type: 1 }
      ],
      headerType: 1,
      contextInfo: {
        externalAdReply: {
          title: botName,
          body: "ᴀᴄᴛɪᴠᴇ ɴᴏᴡ✅",
          mediaType: 1,
          sourceUrl: "https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00", 
          showAdAttribution: true 
        }
      }
    }, { quoted: dtzminibot }); // මෙතැනට Catalog Quote එක සම්බන්ධ කළා

    // 4. Reaction (Recat)
    await socket.sendMessage(sender, { 
      react: { text: '📍', key: catalogMsg.key } 
    });

    // 5. Final Text Message
    await socket.sendMessage(sender, { text: 'ᴘɪɴɢ ʟᴏᴀᴅɪɴɢ ᴅᴏɴᴇ✅' });

  } catch(e) {
    console.error('ping error', e);
    await socket.sendMessage(sender, { text: '❌ Failed to get ping.' }, { quoted: msg });
  }
  break;
}
case 'activesessions':
case 'active':
case 'bots': {
    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const cfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = cfg.botName || BOT_NAME_FANCY;
        const logo = cfg.logo || config.RCD_IMAGE_PATH;

        // --- Permission Check ---
        const admins = await loadAdminsFromMongo();
        const normalizedAdmins = (admins || []).map(a => (a || '').toString());
        const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');
        const isAdmin = normalizedAdmins.includes(nowsender) || normalizedAdmins.includes(senderNumber) || normalizedAdmins.includes(senderIdSimple);

        if (!isOwner && !isAdmin) {
            await socket.sendMessage(sender, { text: '❌ Permission denied.' }, { quoted: msg });
            break;
        }

        // --- Data Gathering ---
        const activeCount = activeSockets.size;
        const activeNumbers = Array.from(activeSockets.keys());
        let text = `*📡 𝐀ᴄᴛɪᴠᴇ 𝐒ᴇꜱ|ꜱɪᴏɴ - ${botName}*\n\n📊 *𝐓otal 𝐀ctive 𝐒essions:* ${activeCount}\n\n`;
        if (activeCount > 0) {
            text += `📱 *𝐀ctive 𝐍umbers:*\n${activeNumbers.map((n, i) => `${i + 1}. ${n}`).join('\n')}\n`;
        } else {
            text += `⚠️ No active sessions found.`;
        }
        text += `\n*🕒 𝐂hecked 𝐀t:* ${getSriLankaTimestamp()}`;

        const metaQuote = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nFN:${botName}\nEND:VCARD` } }
        };

        // --- 1.. දෙවනුව Text Message එක (Image එක සමඟ) යැවීම ---
        let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);
        await socket.sendMessage(sender, {
            image: imagePayload,
            caption: text,
            footer: `📊 *${botName} 𝐒𝚃𝙰𝚃𝚄𝚂*`,
            buttons: [
                { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📒Mᴇɴᴜ" }, type: 1 }
            ],
            headerType: 4
        }, { quoted: metaQuote });

        // --- 2. අවසානයට Voice Note එක යැවීම ---
        const ffmpeg = require('fluent-ffmpeg');
        const ffmpegPath = require('ffmpeg-static');
        if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);

        const tempMp3 = `./temp_${Date.now()}.mp3`;
        const tempOpus = `./temp_${Date.now()}.opus`;

        const audioUrl = 'https://files.catbox.moe/wyreig.mp4';
        const axios = require('axios');
        const resp = await axios.get(audioUrl, { responseType: 'arraybuffer' });
        fs.writeFileSync(tempMp3, Buffer.from(resp.data));

        await new Promise((resolve, reject) => {
            ffmpeg(tempMp3)
                .noVideo()
                .audioCodec('libopus')
                .format('opus')
                .on('end', resolve)
                .on('error', reject)
                .save(tempOpus);
        });

        if (fs.existsSync(tempOpus)) {
            const opusBuffer = fs.readFileSync(tempOpus);
            await socket.sendMessage(sender, { 
                audio: opusBuffer, 
                mimetype: 'audio/ogg; codecs=opus', 
                ptt: true 
            }, { quoted: msg });
            
            // තාවකාලික ගොනු මැකීම
            if (fs.existsSync(tempMp3)) fs.unlinkSync(tempMp3);
            if (fs.existsSync(tempOpus)) fs.unlinkSync(tempOpus);
        }

    } catch (e) {
        console.error('Error in activesessions:', e);
        await socket.sendMessage(sender, { text: '❌ Failed to process the request.' }, { quoted: msg });
    }
    break;
}
case 'song':
case 'play': {
    try {
        const axios = require('axios');
        const yts = require('yt-search');

      
        const API_KEY = 'chama_f7b61e81701eeb7e860025e9f5af4f77'; 
        const BASE_URL = 'https://chama-api-hub.vercel.app/api/mp3_v3';
        // -------------------------------------------------------------------

        let text = (args.join(' ') || '').trim();
        if (!text) return await socket.sendMessage(sender, { text: "🎧 *කරුණාකර සින්දුවක නමක් හෝ YouTube ලින්ක් එකක් ලබා දෙන්න!*" }, { quoted: msg });

        // Reaction එකක් දැමීම
        await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } });

        // YouTube Search
        const search = await yts(text);
        const data = search.videos[0];
        if (!data) return await socket.sendMessage(sender, { text: "❌ *සොයාගත නොහැකි විය!*" }, { quoted: msg });

        const videoUrl = data.url;

        // Thumbnail සමඟ විස්තර යැවීම
        const desc = `*🎵 QUEEN IMALSHA MD SONG 🎵*\n\n*Title:* ${data.title}\n*Duration:* ${data.timestamp}\n\n> *Downloading... Please wait* 📥`;
        await socket.sendMessage(sender, { image: { url: data.thumbnail }, caption: desc }, { quoted: msg });

        // API URL එක සෑදීම (Key එක සහ URL එක වෙන වෙනම සම්බන්ධ වේ)
        const finalApiUrl = `${BASE_URL}?apikey=${API_KEY}&url=${encodeURIComponent(videoUrl)}`;
        
        const apiRes = await axios.get(finalApiUrl);
        
        // API Response එකෙන් Download Link එක ලබා ගැනීම
        const downloadUrl = apiRes.data.result?.download_url || apiRes.data.result?.dl_url || apiRes.data.url;

        if (!downloadUrl) {
            return await socket.sendMessage(sender, { text: "❌ *Download URL එක ලබා ගැනීමට නොහැකි විය.*" }, { quoted: msg });
        }

        // Audio එක Download කරගෙන යැවීම
        const audioRes = await axios.get(downloadUrl, { responseType: 'arraybuffer' });
        const audioBuffer = Buffer.from(audioRes.data);

        await socket.sendMessage(sender, {
            audio: audioBuffer,
            mimetype: "audio/mpeg",
            contextInfo: {
                externalAdReply: {
                    title: data.title,
                    body: "",
                    thumbnailUrl: data.thumbnail,
                    mediaType: 1,
                    renderLargerThumbnail: true
                }
            }
        }, { quoted: msg });

        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (e) {
        console.error("Song error:", e);
        await socket.sendMessage(sender, { text: `❌ දෝෂයක් සිදුවිය: ${e.message}` }, { quoted: msg });
    }
}
break;
case 'youtube':
case 'ytdl':
case 'video':
case 'yt':
case 'mp4': {
    try {
        const axios = require('axios');
        const yts = require('yt-search');

        // 1. Bot Name & Config Load
        const sanitized = (sender.split('@')[0] || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || '𝐐𝐔𝐄𝐄𝐍 𝐒𝐄𝐘𝐀 𝐗𝐌𝐃';

        // 2. Input Handling
        let text = (args.join(' ') || '').trim();

        if (!text) {
            return await socket.sendMessage(sender, {
                text: "❌ *Please provide a YouTube Name or URL!*"
            }, { quoted: msg });
        }

        // 3. Searching Reaction
        await socket.sendMessage(sender, { react: { text: '🔎', key: msg.key } });

        // 4. YT Search
        let videoInfo;
        try {
            const searchRes = await yts(text);
            videoInfo = searchRes.videos[0];
        } catch (e) {
            return await socket.sendMessage(sender, { text: "❌ *Video Not Found!*" }, { quoted: msg });
        }

        if (!videoInfo) {
            return await socket.sendMessage(sender, { text: "❌ *Video Not Found!*" }, { quoted: msg });
        }

        // 5. Fancy Caption
        const captionMessage = `
╭───「 👸 *${botName}* 」───◆
│
│ 🎬 *Title:* ${videoInfo.title}
│ 👤 *Author:* ${videoInfo.author.name}
│ ⏱️ *Duration:* ${videoInfo.timestamp}
│ 👁️ *Views:* ${videoInfo.views}
│ 📅 *Ago:* ${videoInfo.ago}
│
╰───────────────────────◆

👇 *ꜱᴇʟᴇᴄᴛ ʏᴏᴜʀ ᴅᴏᴡɴʟᴏᴀᴅ ᴛʏᴘᴇ* 👇`;

        // 6. Buttons Definition
        const buttons = [
            { buttonId: 'yt_360', buttonText: { displayText: '🎬 360P QUALITY' }, type: 1 },
            { buttonId: 'yt_480', buttonText: { displayText: '📹 480P QUALITY' }, type: 1 },
            { buttonId: 'yt_720', buttonText: { displayText: '🎥 720P QUALITY' }, type: 1 },
            { buttonId: 'yt_audio', buttonText: { displayText: '🎵 AUDIO FILE' }, type: 1 }
        ];

        // 7. Send Button Message
        const buttonMessage = {
            image: { url: videoInfo.thumbnail || config.RCD_IMAGE_PATH },
            caption: captionMessage,
            footer: `© ᴘᴏᴡᴇʀᴇᴅ ʙʏ ${botName}`,
            buttons: buttons,
            headerType: 4,
            contextInfo: {
                externalAdReply: {
                    title: "🎥 ＹＯＵＴＵＢＥ  ＤＯＷＮＬＯＡＤＥＲ",
                    body: videoInfo.title,
                    thumbnailUrl: videoInfo.thumbnail,
                    sourceUrl: videoInfo.url,
                    mediaType: 1,
                    renderLargerThumbnail: true
                }
            }
        };

        const sentMessage = await socket.sendMessage(sender, buttonMessage, { quoted: msg });
        const messageID = sentMessage.key.id;

        // 8. Handle User Selection (Button Click)
        const handleYouTubeSelection = async ({ messages: replyMessages }) => {
            const replyMek = replyMessages[0];
            if (!replyMek?.message) return;

            const selectedId = replyMek.message.buttonsResponseMessage?.selectedButtonId || 
                               replyMek.message.templateButtonReplyMessage?.selectedId || 
                               replyMek.message.conversation || 
                               replyMek.message.extendedTextMessage?.text;

            const isReplyToSentMsg = replyMek.message.extendedTextMessage?.contextInfo?.stanzaId === messageID || 
                                     replyMek.message.buttonsResponseMessage?.contextInfo?.stanzaId === messageID;

            if (isReplyToSentMsg && sender === replyMek.key.remoteJid) {
                
                await socket.sendMessage(sender, { react: { text: '⬇️', key: replyMek.key } });

                let selectedFormat = '';
                let type = 'video';
                let mimetype = 'video/mp4';

                // Map Selection
                switch (selectedId) {
                    case 'yt_360':
                    case '1':
                        selectedFormat = "360p";
                        break;
                    case 'yt_480':
                    case '2':
                        selectedFormat = "480p";
                        break;
                    case 'yt_720':
                    case '3':
                        selectedFormat = "720p";
                        break;
                    case 'yt_audio':
                    case '4':
                        selectedFormat = "mp3";
                        type = 'audio';
                        mimetype = 'audio/mpeg';
                        break;
                    default:
                        // Invalid selection ignored
                        return;
                }

                try {
                    // API Call
                    const apiRes = await axios.get("https://www.movanest.xyz/v2/dxz-ytdl", {
                        params: {
                            input: videoInfo.url,
                            format: selectedFormat === 'mp3' ? 'mp3' : selectedFormat,
                            your_api_key: "movanest-keySMAFE9R6ON"
                        }
                    });

                    if (!apiRes.data.status) throw new Error("API Error");
                    
                    const results = apiRes.data.results;
                    let downloadUrl = '';

                    // URL Extraction Logic
                    if (type === 'audio') {
                        // Try fetching audio url
                        const audioData = results.byFormat?.["mp3"]?.find(f => f.scraper === "ddownr" || f.scraper === "cobalt");
                        const anyAudio = audioData || results.byFormat?.["mp3"]?.[0];
                        downloadUrl = anyAudio?.url || anyAudio?.alternatives?.[0]?.url;
                    } else {
                        // Video Handling
                        const videoData = results.byFormat?.[selectedFormat]?.find(
                            f => f.scraper === "ddownr" && Array.isArray(f.alternatives)
                        );
                        // Fallback
                        const fallback = results.byFormat?.[selectedFormat]?.[0];
                        
                        if (videoData) {
                            const direct = videoData.alternatives.find(a => a.has_ssl) || videoData.alternatives[0];
                            downloadUrl = direct?.url;
                        } else if (fallback) {
                            downloadUrl = fallback.url;
                        }
                    }

                    if (!downloadUrl) {
                        return await socket.sendMessage(sender, { text: `❌ Could not find ${selectedFormat} link. Try another quality.` }, { quoted: replyMek });
                    }

                    // Buffer Download
                    const bufferRes = await axios.get(downloadUrl, {
                        responseType: 'arraybuffer',
                        headers: { "User-Agent": "Mozilla/5.0" }
                    });

                    const mediaBuffer = Buffer.from(bufferRes.data);
                    
                    // Size Check (100MB)
                    if (mediaBuffer.length > 100 * 1024 * 1024) {
                         return await socket.sendMessage(sender, { text: '❌ File too large (>100MB)!' }, { quoted: replyMek });
                    }

                    // Send Final Message
                    let msgContent = {};
                    if (type === 'audio') {
                        msgContent = { 
                            audio: mediaBuffer, 
                            mimetype: 'audio/mpeg', 
                            ptt: false,
                            contextInfo: {
                                externalAdReply: {
                                    title: "🎵 ᴀᴜᴅɪᴏ ᴅᴏᴡɴʟᴏᴀᴅᴇᴅ",
                                    body: videoInfo.title,
                                    thumbnailUrl: videoInfo.thumbnail,
                                    sourceUrl: videoInfo.url,
                                    mediaType: 1,
                                    renderLargerThumbnail: true
                                }
                            }
                        };
                    } else {
                        msgContent = { 
                            video: mediaBuffer, 
                            mimetype: 'video/mp4', 
                            caption: `╭──「 *${selectedFormat.toUpperCase()} VIDEO* 」──◆\n│ 🎬 ${videoInfo.title}\n╰─────────────────◆\n\n© ᴘᴏᴡᴇʀᴇᴅ ʙʏ ${botName}`
                        };
                    }

                    await socket.sendMessage(sender, msgContent, { quoted: replyMek });
                    await socket.sendMessage(sender, { react: { text: '✅', key: replyMek.key } });

                } catch (err) {
                    console.error(err);
                    await socket.sendMessage(sender, { text: '❌ Error Downloading. Try different quality.' }, { quoted: replyMek });
                }

                // Remove Listener
                socket.ev.removeListener('messages.upsert', handleYouTubeSelection);
            }
        };

        socket.ev.on('messages.upsert', handleYouTubeSelection);

    } catch (err) {
        console.error("YT Error:", err);
        await socket.sendMessage(sender, { text: '*❌ System Error.*' }, { quoted: msg });
    }
    break;
}
case 'system': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;
    const logo = cfg.logo || config.RCD_IMAGE_PATH;

    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SYSTEM" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    const os = require('os');
    const text = `
*╭⬡─────────────⬡┈⊷*
*┋𝑸𝑼𝑬𝑬𝑵 𝑰𝑴𝑨𝑳𝑺𝑯𝑨 𝑴𝑫 𝑺𝒀𝑺𝑻𝑬𝑴*
*┋┌─────────────⊷*
*┋▢🚀ᴏꜱ:* ${os.type()} ${os.release()}
*┋▢🏅ᴘʟᴀᴛꜰᴏʀᴍ:* ${os.platform()}
*┋▢⛓️ᴄᴘᴜ ᴄᴏʀᴇꜱ:* ${os.cpus().length}
*┋▢💽ᴍᴇᴍᴏʀʏ:* ${(os.totalmem()/1024/1024/1024).toFixed(2)} GB
*┋└─────────────⊷*
*╰⬡─────────────⬡┈⊷*
> *ᴘᴏᴡᴇʀᴅ ʙʏ Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2*
`;

    let imagePayload = String(logo).startsWith('http') ? { url: logo } : fs.readFileSync(logo);

    await socket.sendMessage(sender, {
      image: imagePayload,
      caption: text,
      footer: `*${botName} 𝐒ʏꜱᴛᴇᴍ 𝐈ɴꜰᴏ* `,
      buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📒Mᴇɴᴜ" },type: 1 },
				{ buttonId: `${config.PREFIX}owner`, buttonText: { displayText: "👑Oᴡɴᴇʀ" }, type: 1 }],
      headerType: 4
    }, { quoted: metaQuote });

  } catch(e) {
    console.error('system error', e);
    await socket.sendMessage(sender, { text: '❌ Failed to get system info.' }, { quoted: msg });
  }
  break;
}
case 'menu': {
    try { await socket.sendMessage(sender, { react: { text: "📡", key: msg.key } }); } catch(e){}
    
    try {
        const fs = require('fs');
        const path = require('path');
        const axios = require('axios');
        const ffmpeg = require('fluent-ffmpeg');
        const ffmpegPath = require('ffmpeg-static');
        if (ffmpegPath) ffmpeg.setFfmpegPath(ffmpegPath);

        const startTime = socketCreationTime.get(number) || Date.now();
        const uptime = Math.floor((Date.now() - startTime) / 1000);
        const hours = Math.floor(uptime / 3600);
        const minutes = Math.floor((uptime % 3600) / 60);
        const seconds = Math.floor(uptime % 60);

        let userCfg = {};
        try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; }
        catch(e){ userCfg = {}; }

        const title = userCfg.botName || '𝐒ᴀɴᴜ x 𝐌ɪɴɪ 𝐁ᴏᴛ📡ᴠ3';
        
        const MenuImg = 'https://www.movanest.xyz/BDhh39.jpg';
        const SecondaryImg = 'https://www.movanest.xyz/BDhh39.jpg';
        const ashiyaPDF = 'https://queen-imalsha-md-v2-3c1aa739ca78.herokuapp.com/';         
        const voiceUrl = 'https://www.movanest.xyz/YsekEa.mp3'; 
        // 🎥 Video Note (PTV)
        await socket.sendMessage(sender, {
            video: { url: 'https://www.movanest.xyz/unJuHD.mp4' },
            ptv: true 
        });

        await new Promise(resolve => setTimeout(resolve, 1500));

        const shonux = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "SANU_X_MINI_BOT" },
            message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Queen Imalsha\nTEL;type=CELL;type=VOICE;waid=94700000000:94700000000\nEND:VCARD` } }
        };

        const date = new Date();
        const slstDate = new Date(date.toLocaleString("en-US", { timeZone: "Asia/Colombo" }));
        const hour = slstDate.getHours();
        const greetings = hour < 12 ? 'සුභ උදෑසනක් 🌄' : hour < 17 ? 'සුභ දහවලක් 🏞️' : hour < 20 ? 'සුභ හැන්දෑවක් 🌅' : 'සුභ රාත්‍රියක් 🌌';

        const text = `
*╭⬡❲ 𝐒ᴀɴᴜ x 𝐌ɪɴɪ 𝐁ᴏᴛ📡ᴠ3 〕⊷*
*┋┌─────────────⊷*
*┋▢📍ɢʀᴇᴇᴛɪɴɢ:* *\`${greetings}\`*
*┋▢⏳ᴜᴘᴛɪᴍᴇ:* *${hours}ʜ ${minutes}ᴍ ${seconds}ꜱ*
*┋▢🥷ᴏᴡɴᴇʀ:* *『ꜱᴀɴᴜ x│ᴏʟᴅ ꜱɪᴛʜᴜᴡᴀ』*
*┋▢🚀ʙᴏᴛɴᴀᴍᴇ:* *『𝐒ᴀɴᴜ x 𝐌ɪɴɪ 𝐁ᴏᴛ📡ᴠ3』*
*┋└─────────────⊷*
*╰⬡─────────────❍┈⊷*
*⬡👋ඔබව සදරයෙන් පිළිගන්වා 𝐒ᴀɴᴜ x 𝐌ɪɴɪ 𝐁ᴏᴛ📡ᴠ3 වෙතට...❍*`.trim();

        let rows = [
            { title: "◇Dᴏᴡɴʟᴏᴀᴅ ᴄᴍᴅ◇", description: "•V3.0.0 ᴅᴏᴡɴʟᴏᴀᴅ ᴍᴇɴᴜ•", id: `${config.PREFIX}download` },
            { title: "◇Cʀᴇᴛɪᴠᴇ ᴄᴍᴅ◇", description: "•V3.0.0 ᴄʀᴇᴀᴛɪᴠᴇ ᴍᴇɴᴜ•", id: `${config.PREFIX}creative` },
            { title: "◇Tᴏᴏʟ ᴄᴍᴅ◇", description: "•V3.0.0 ᴛᴏᴏʟ ᴍᴇɴᴜ•", id: `${config.PREFIX}tool` },
            { title: "◇Sᴇᴛᴛɪɴɢꜱ ᴄᴍᴅ◇", description: "•V3.0.0 ʙᴏᴛ ꜱᴇᴛᴛɪɴɢꜱ•", id: `${config.PREFIX}settings` },
            { title: "◇Oᴡɴᴇʀ ɪɴꜰᴏ◇", description: "•V3.0.0 ᴏᴡɴᴇʀ ɪɴꜰᴘ•", id: `${config.PREFIX}owner` },
            { title: "◇Pɪɴɢ ᴄᴍᴅ◇", description: "•V3.0.0 ʙᴏᴛ ꜱᴘᴇᴇᴅ•", id: `${config.PREFIX}ping` }
        ];

        let buttons = [{
            buttonId: "action",
            buttonText: { displayText: "Sᴇʟᴇᴄᴛ Mᴇɴᴜ" },
            type: 4,
            nativeFlowInfo: {
                name: "single_select",
                paramsJson: JSON.stringify({
                    title: "𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3",
                    sections: [{ title: "◎𝑸𝑼𝑬𝑬𝑵 𝑿 𝑴𝑫 𝑴𝑬𝑵𝑼 𝑳𝑰𝑺𝑻◎", rows: rows }]
                })
            }
        }];
        await socket.sendMessage(sender, {
            image: { url: MenuImg }, 
            caption: text,
            footer: '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3',
            buttons: buttons,
            headerType: 4,
            contextInfo: {
                mentionedJid: [sender],
                forwardingScore: 999,
                isForwarded: true,
                externalAdReply: {
                    title: `${title}`,
                    body: `ᴜᴘᴛɪᴍᴇ: ${hours}ʜ ${minutes}ᴍ`,
                    mediaType: 1,
                    thumbnailUrl: SecondaryImg, 
                    sourceUrl: ashiyaPDF,
                    renderLargerThumbnail: true,
                    showAdAttribution: true 
                }
            }
        }, { quoted: shonux });

        // 🎵 Voice Note Section
        if (voiceUrl) {
            try {
                const tempMp3 = path.join(__dirname, `temp_${Date.now()}.mp3`);
                const tempOpus = path.join(__dirname, `temp_${Date.now()}.opus`);
                const resp = await axios({ method: 'get', url: voiceUrl, responseType: 'stream' });
                const writer = fs.createWriteStream(tempMp3);
                resp.data.pipe(writer);
                await new Promise((resolve) => writer.on('finish', resolve));
                await new Promise((resolve, reject) => {
                    ffmpeg(tempMp3).noVideo().audioCodec('libopus').format('opus').on('end', resolve).on('error', reject).save(tempOpus);
                });
                if (fs.existsSync(tempOpus)) {
                    await socket.sendMessage(sender, { 
                        audio: fs.readFileSync(tempOpus), 
                        mimetype: 'audio/ogg; codecs=opus', 
                        ptt: true 
                    }, { quoted: shonux });
                    fs.unlinkSync(tempMp3);
                    fs.unlinkSync(tempOpus);
                }
            } catch(e) {}
        }

    } catch (err) {
        console.error('Menu error:', err);
    }
    break;
}
// ==================== DOWNLOAD MENU ====================
case 'download': {
  try { await socket.sendMessage(sender, { react: { text: "📍", key: msg.key } }); } catch(e){}

  try {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';

    
    const mainImage = config.LOGO || 'https://www.movanest.xyz/BDhh39.jpg'; 
    const smallThumb = config.THUMB || 'https://www.movanest.xyz/BDhh39.jpg';

    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_DOWNLOAD"
        },
        message: {
            contactMessage: {
                displayName: title,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${title};;;;
FN:${title}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    const text =`
╭═〔 𝑫𝑶𝑾𝑵𝑳𝑶𝑨𝑫 𝑴𝑬𝑵𝑼 𝑳𝑰𝑺𝑻📍〕═╮
├▭▬▭▬▭▬▭▬▭▬▭▬╮
▯
▯•📍${config.PREFIX}song
◇▭
▯•📍${config.PREFIX}csong
◇▭
▯•📍${config.PREFIX}gsong
◇▭
▯•📍${config.PREFIX}cvideo
◇▭
▯•📍${config.PREFIX}video
◇▭
▯•📍${config.PREFIX}tiktok
◇▭
▯•📍${config.PREFIX}fb
◇▭
▯•📍${config.PREFIX}ig
◇▭
▯•📍${config.PREFIX}apk
◇▭
▯•📍${config.PREFIX}apksearch
◇▭
▯•📍${config.PREFIX}mediafire
◇▭
▯•📍${config.PREFIX}gdrive
◇▭
▯•📍${config.PREFIX}badword
◇▭
▯•📍${config.PREFIX}welcome boodby
◇▭
▯•📍${config.PREFIX}autovoise
◇▭
▯•📍${config.PREFIX}autoreply
◇▭
▯•📍${config.PREFIX}autorecat
◇▭
▯
╰▭▬▭▬▭▬▭▬▭▬▭▬╯
`.trim();

    const buttons = [
      { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📍ᴠ3.0.0💗ᴍᴇɴᴜ ᴄᴍᴅ" }, type: 1 },
      { buttonId: `${config.PREFIX}settings`, buttonText: { displayText: "📍ᴠ3.0.0💗ꜱᴇᴛᴛɪɴɢꜱ ᴄᴍᴅ" }, type: 1 }
    ];

    
    await socket.sendMessage(sender, {
      image: { url: mainImage }, 
      caption: text,    
      footer: "Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ ᴠ3.0.0",
      buttons: buttons,
      contextInfo: {
        externalAdReply: {
          title: title,
          body: "𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3",
          thumbnailUrl: smallThumb, 
          sourceUrl: "https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00", 
          mediaType: 1,
          renderLargerThumbnail: false 
        }
      }
    }, { quoted: shonux });

  } catch (err) {
    console.error('download command error:', err);
    try { await socket.sendMessage(sender, { text: '❌ Failed to show download menu.' }, { quoted: msg }); } catch(e){}
  }
  break;
}
// ==================== CREATIVE MENU ====================
case 'creative': {
  try { await socket.sendMessage(sender, { react: { text: "📍", key: msg.key } }); } catch(e){}

  try {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';

    
    const botLogo = config.LOGO || 'https://www.movanest.xyz/BDhh39.jpg'; 

    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_CREATIVE"
        },
        message: {
            contactMessage: {
                displayName: title,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${title};;;;
FN:${title}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    const text = `
╭═〔𝑪𝑹𝑬𝑻𝑰𝑽𝑬 𝑴𝑬𝑵𝑼 𝑳𝑰𝑺𝑻📍〕═╮
├▭▬▭▬▭▬▭▬▭▬▭▬╮
▯
▯•📍${config.PREFIX}ai
◇▭
▯•📍${config.PREFIX}aiimg
◇▭
▯•📍${config.PREFIX}aiimg2
◇▭
▯•📍${config.PREFIX}font
◇▭
▯•📍${config.PREFIX}getdp
◇▭
▯•📍${config.PREFIX}vv
◇▭
▯•📍${config.PREFIX}save
◇▭
▯•📍${config.PREFIX}tourl
◇▭
▯•📍${config.PREFIX}art
◇▭
▯
╰▭▬▭▬▭▬▭▬▭▬▭▬╯
`.trim();

    const buttons = [
      { buttonId: `${config.PREFIX}tool`, buttonText: { displayText: "📍ᴠ3.0.0💗ᴛᴏᴏʟ ᴄᴍᴅ" }, type: 1 },
      { buttonId: `${config.PREFIX}download`, buttonText: { displayText: "📍ᴠ3.0.0💗ᴅᴏᴡɴʟᴏᴀᴅ ᴄᴍᴅ" }, type: 1 }
    ];

    
    await socket.sendMessage(sender, {
      image: { url: botLogo }, 
      caption: text,
      footer: "𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3",
      buttons: buttons,
      contextInfo: {
        externalAdReply: {
          title: title,
          body: '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3',
          mediaType: 1,
          thumbnailUrl: botLogo, 
          sourceUrl: 'https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00', 
          renderLargerThumbnail: false 
        }
      }
    }, { quoted: shonux });

  } catch (err) {
    console.error('creative command error:', err);
    try { await socket.sendMessage(sender, { text: '❌ Failed to show creative menu.' }, { quoted: msg }); } catch(e){}
  }
  break;
}
// ==================== TOOLS MENU ====================
case 'tool': {
  try { await socket.sendMessage(sender, { react: { text: "📍", key: msg.key } }); } catch(e){}

  try {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';

    
    const mainLogo = config.ALIVE_IMG || "https://www.movanest.xyz/BDhh39.jpg"; 
    
    const smallLogo = config.THUMB || "https://www.movanest.xyz/BDhh39.jpg";

    const shonux = {
        key: {
            remoteJid: "status@broadcast",
            participant: "0@s.whatsapp.net",
            fromMe: false,
            id: "META_AI_FAKE_ID_TOOLS"
        },
        message: {
            contactMessage: {
                displayName: title,
                vcard: `BEGIN:VCARD
VERSION:3.0
N:${title};;;;
FN:${title}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
            }
        }
    };

    const text = `
╭═〔𝑻𝑶𝑶𝑳 𝑴𝑬𝑵𝑼 𝑳𝑰𝑺𝑻📍〕═╮
├▭▬▭▬▭▬▭▬▭▬▭▬╮
▯
▯•📍${config.PREFIX}jid
◇▭
▯•📍${config.PREFIX}cid
◇▭
▯•📍${config.PREFIX}system
◇▭
▯•📍${config.PREFIX}tagall
◇▭
▯•📍${config.PREFIX}online
◇▭
▯•📍${config.PREFIX}adanews
◇▭
▯•📍${config.PREFIX}sirasanews
◇▭
▯•📍${config.PREFIX}lankadeepanews
◇▭
▯•📍${config.PREFIX}gagananews
◇▭
▯•📍${config.PREFIX}block
◇▭
▯•📍${config.PREFIX}unblock
◇▭
▯•📍${config.PREFIX}prefix
◇▭
▯•📍${config.PREFIX}autorecording
◇▭
▯•📍${config.PREFIX}mread
◇▭
▯•📍${config.PREFIX}creject
◇▭
▯•📍${config.PREFIX}wtyp
◇▭
▯•📍${config.PREFIX}pp
◇▭
▯•📍${config.PREFIX}arm
◇▭
▯•📍${config.PREFIX}rstatus
◇▭
▯•📍${config.PREFIX}botpresence
◇▭
▯•📍${config.PREFIX}img
◇▭
▯•📍${config.PREFIX}google
◇▭
▯•📍${config.PREFIX}ping
◇▭
▯•📍${config.PREFIX}alive
◇▭
▯
╰▭▬▭▬▭▬▭▬▭▬▭▬╯
`.trim();

    const buttons = [
      { buttonId: `${config.PREFIX}cretive`, buttonText: { displayText: "📍ᴠ3.0.0💗ᴄʀᴇᴛɪᴠᴇ ᴄᴍᴅ" }, type: 1 },
      { buttonId: `${config.PREFIX}owner`, buttonText: { displayText: "📍ᴠ3.0.0💗ᴏᴡɴᴇʀ ɪɴꜰᴏ" }, type: 1 }
    ];

   
    await socket.sendMessage(sender, {
      image: { url: mainLogo }, 
      caption: text,            
      footer: "𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3",
      buttons: buttons,
      contextInfo: {
        externalAdReply: {
          title: title,
          body: "𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3",
          thumbnailUrl: smallLogo, 
          sourceUrl: "https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00", 
          mediaType: 1,
          renderLargerThumbnail: false 
        }
      }
    }, { quoted: shonux });

  } catch (err) {
    console.error('tools command error:', err);
    try { await socket.sendMessage(sender, { text: '❌ Failed to show tools menu.' }, { quoted: msg }); } catch(e){}
  }
  break;
}
                   
case 'getdp': {
    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const cfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = cfg.botName || BOT_NAME_FANCY;
        const logo = cfg.logo || config.RCD_IMAGE_PATH;

        const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');

        let q = msg.message?.conversation?.split(" ")[1] || 
                msg.message?.extendedTextMessage?.text?.split(" ")[1];

        if (!q) return await socket.sendMessage(sender, { text: "❌ Please provide a number.\n\nUsage: .getdp <number>" });

        // 🔹 Format number into JID
        let jid = q.replace(/[^0-9]/g, '') + "@s.whatsapp.net";

        // 🔹 Try to get profile picture
        let ppUrl;
        try {
            ppUrl = await socket.profilePictureUrl(jid, "image");
        } catch {
            ppUrl = "https://telegra.ph/file/4cc2712eaba1c5c1488d3.jpg"; // default dp
        }

        // 🔹 BotName meta mention
        const metaQuote = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_GETDP" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
        };

        // 🔹 Send DP with botName meta mention
        await socket.sendMessage(sender, { 
            image: { url: ppUrl }, 
            caption: `🖼 *✨𝙿𝚁𝙾𝙵𝙸𝙻𝙴 𝙿𝙸𝙲𝚃𝚄𝚁𝙴 𝙾𝙵* +${q}\n𝙵𝙴𝚃𝙲𝙷𝙴𝙳 𝙱𝚈💗 ${botName}`,
            footer: `📌 ${botName} 𝙶𝙴𝚃𝙳𝙿🦋`,
            buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: "📋 MENU" }, type: 1 }],
            headerType: 4
        }, { quoted: metaQuote }); // <-- botName meta mention

    } catch (e) {
        console.log("❌ getdp error:", e);
        await socket.sendMessage(sender, { text: "⚠️ Error: Could not fetch profile picture." });
    }
    break;
}
case 'showconfig': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  try {
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SHOWCONFIG" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let txt = `*Session config for ${sanitized}:*\n`;
    txt += `• Bot name: ${botName}\n`;
    txt += `• Logo: ${cfg.logo || config.RCD_IMAGE_PATH}\n`;
    await socket.sendMessage(sender, { text: txt }, { quoted: shonux });
  } catch (e) {
    console.error('showconfig error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SHOWCONFIG2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Failed to load config.' }, { quoted: shonux });
  }
  break;
}

case 'resetconfig': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can reset configs.' }, { quoted: shonux });
    break;
  }

  try {
    await setUserConfigInMongo(sanitized, {});

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '✅ Session config reset to defaults.' }, { quoted: shonux });
  } catch (e) {
    console.error('resetconfig error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_RESETCONFIG3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '❌ Failed to reset config.' }, { quoted: shonux });
  }
  break;
}
case 'owner': {
    const axios = require('axios'); // පින්තූරය download කිරීමට මෙය අවශ්‍යයි

    const ownerName1 = 'ᬊꪶOʟ͠͠͠ᴅ Ｃʀꫝᴢʏ ꜱᴀɴᴜ';
    const ownerNumber1 = '94785893445'; 
    const ownerName2 = 'ᬊꪶOʟ͠͠͠ᴅ Kɪɴɢ ꜱɪᴛʜᴜᴡᴀ'; 
    const ownerNumber2 = '94743769118'; 
    const logoUrl = 'https://www.movanest.xyz/BDhh39.jpg';
    const organization = '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';

    try {
        // 1. මුලින්ම React එකක් යැවීම
        await socket.sendMessage(from, { 
            react: { text: '🛡️', key: msg.key } 
        });

        // ලොගෝ එක Buffer එකක් ලෙස ලබා ගැනීම (Fixing Logo issue)
        let response = await axios.get(logoUrl, { responseType: 'arraybuffer' });
        let logoBuffer = Buffer.from(response.data, 'utf-8');

        // vCards සෑදීම
        const vcard1 = 'BEGIN:VCARD\n' + 'VERSION:3.0\n' + `FN:${ownerName1}\n` + `ORG:${organization};\n` + `TEL;type=CELL;type=VOICE;waid=${ownerNumber1}:${ownerNumber1}\n` + 'END:VCARD';
        const vcard2 = 'BEGIN:VCARD\n' + 'VERSION:3.0\n' + `FN:${ownerName2}\n` + `ORG:${organization};\n` + `TEL;type=CELL;type=VOICE;waid=${ownerNumber2}:${ownerNumber2}\n` + 'END:VCARD';

        // 2. Location Message එක සමඟ Logo එක (Buffer එකක් ලෙස) යැවීම
        await socket.sendMessage(from, {
            location: { 
                degreesLatitude: 6.9271, 
                degreesLongitude: 79.8612 
            },
            caption: `👤 *𝑶𝑾𝑵𝑬𝑹 𝑰𝑵𝑭𝑶*\n\n1️⃣ ${ownerName1}\n📞 ${ownerNumber1}\n\n2️⃣ ${ownerName2}\n📞 ${ownerNumber2}\n\n> ᴘᴏᴡᴇʀᴅ ʙʏ 𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3`,
            contextInfo: {
                externalAdReply: {
                    title: `𝑶𝑾𝑵𝑬𝑹𝑺: ${ownerName1} & ${ownerName2}`,
                    body: '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3',
                    thumbnail: logoBuffer, // මෙතනට buffer එක ලබා දීමෙන් ලොගෝ එක අනිවාර්යයෙන්ම පෙන්වයි
                    sourceUrl: `https://wa.me/${ownerNumber1}`,
                    mediaType: 1,
                    renderLargerThumbnail: true
                }
            }
        }, { quoted: msg });

        // 3. Contact Card එක යැවීම
        await socket.sendMessage(from, {
            contacts: {
                displayName: 'Our Owners',
                contacts: [
                    { vcard: vcard1 },
                    { vcard: vcard2 }
                ]
            }
        }, { quoted: msg });

        // 4. අවසානයේ සාර්ථක බව පෙන්වීමට ✅ React එක
        await socket.sendMessage(from, { 
            react: { text: '✅', key: msg.key } 
        });

    } catch (err) {
        console.error('❌ Owner command error:', err.message);
        await socket.sendMessage(from, { react: { text: '❌', key: msg.key } });
    }
    break;
}
case 'google':
case 'gsearch':
case 'search':
    try {
        if (!args || args.length === 0) {
            await socket.sendMessage(sender, {
                text: '⚠️ *Please provide a search query.*\n\n*Example:*\n.google how to code in javascript'
            });
            break;
        }

        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userCfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = userCfg.botName || BOT_NAME_FANCY;

        const botMention = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_GOOGLE" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
        };

        const query = args.join(" ");
        const apiKey = "AIzaSyDMbI3nvmQUrfjoCJYLS69Lej1hSXQjnWI";
        const cx = "baf9bdb0c631236e5";
        const apiUrl = `https://www.googleapis.com/customsearch/v1?q=${encodeURIComponent(query)}&key=${apiKey}&cx=${cx}`;

        const response = await axios.get(apiUrl);

        if (response.status !== 200 || !response.data.items || response.data.items.length === 0) {
            await socket.sendMessage(sender, { text: `⚠️ *No results found for:* ${query}` }, { quoted: botMention });
            break;
        }

        let results = `🔍 *𝐆oogle 𝐒earch 𝐑esults 𝐅or:* "${query}"\n\n`;
        response.data.items.slice(0, 5).forEach((item, index) => {
            results += `*${index + 1}. ${item.title}*\n\n🔗 ${item.link}\n\n📝 ${item.snippet}\n\n`;
        });

        const firstResult = response.data.items[0];
        const thumbnailUrl = firstResult.pagemap?.cse_image?.[0]?.src || firstResult.pagemap?.cse_thumbnail?.[0]?.src || 'https://via.placeholder.com/150';

        await socket.sendMessage(sender, {
            image: { url: thumbnailUrl },
            caption: results.trim(),
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: botMention });

    } catch (error) {
        console.error(`Google search error:`, error);
        await socket.sendMessage(sender, { text: `⚠️ *An error occurred while fetching search results.*\n\n${error.message}` });
    }
    break;

case 'img': {
    const q = body.replace(/^[.\/!]img\s*/i, '').trim();
    if (!q) return await socket.sendMessage(sender, {
        text: '🔍 Please provide a search query. Ex: `.img sunset`'
    }, { quoted: msg });

    try {
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userCfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = userCfg.botName || BOT_NAME_FANCY;

        const botMention = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_IMG" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
        };

        const res = await axios.get(`https://allstars-apis.vercel.app/pinterest?search=${encodeURIComponent(q)}`);
        const data = res.data.data;
        if (!data || data.length === 0) return await socket.sendMessage(sender, { text: '❌ No images found for your query.' }, { quoted: botMention });

        const randomImage = data[Math.floor(Math.random() * data.length)];

        const buttons = [{ buttonId: `${config.PREFIX}img ${q}`, buttonText: { displayText: "ᴠ3.0.0💗ɴᴇxᴛ ɪᴍᴀɢᴇ" }, type: 1 }];

        const buttonMessage = {
            image: { url: randomImage },
            caption: `🖼️ *ɪᴍᴀɢᴇ ꜱᴇᴀʀᴄʜ:* ${q}\n\n*ᴘʀᴏᴠɪᴅᴇᴅ ʙʏ ${botName}*`,
            footer: config.FOOTER || '> *Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍᴅ ᴠ3.0.0💗*',
            buttons: buttons,
             headerType: 4,
            contextInfo: { mentionedJid: [sender] }
        };

        await socket.sendMessage(from, buttonMessage, { quoted: botMention });

    } catch (err) {
        console.error("Image search error:", err);
        await socket.sendMessage(sender, { text: '❌ Failed to fetch images.' }, { quoted: botMention });
    }
    break;
}
case 'gdrive': {
    try {
        const text = args.join(' ').trim();
        if (!text) return await socket.sendMessage(sender, { text: '⚠️ Please provide a Google Drive link.\n\nExample: `.gdrive <link>`' }, { quoted: msg });

        // 🔹 Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        const userCfg = await loadUserConfigFromMongo(sanitized) || {};
        const botName = userCfg.botName || BOT_NAME_FANCY;

        // 🔹 Meta AI fake contact mention
        const botMention = {
            key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_GDRIVE" },
            message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
        };

        // 🔹 Fetch Google Drive file info
        const res = await axios.get(`https://saviya-kolla-api.koyeb.app/download/gdrive?url=${encodeURIComponent(text)}`);
        if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch file info.' }, { quoted: botMention });

        const file = res.data.result;

        // 🔹 Send as document
        await socket.sendMessage(sender, {
            document: { 
                url: file.downloadLink, 
                mimetype: file.mimeType || 'application/octet-stream', 
                fileName: file.name 
            },
            caption: `📂 *𝐅ile 𝐍ame:* ${file.name}\n💾 *𝐒ize:* ${file.size}\n\n*𝐏owered 𝐁y ${botName}*`,
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: botMention });

    } catch (err) {
        console.error('GDrive command error:', err);
        await socket.sendMessage(sender, { text: '❌ Error fetching Google Drive file.' }, { quoted: botMention });
    }
    break;
}
case 'adanews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/ada');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Ada News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [Read more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('adanews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Ada News.' }, { quoted: botMention });
  }
  break;
}
case 'sirasanews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_SIRASA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/sirasa');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Sirasa News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [Read more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('sirasanews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Sirasa News.' }, { quoted: botMention });
  }
  break;
}
case 'lankadeepanews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_LANKADEEPA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/lankadeepa');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Lankadeepa News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [𝐑ead more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('lankadeepanews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Lankadeepa News.' }, { quoted: botMention });
  }
  break;
}
case 'gagananews': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_GAGANA" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD` } }
    };

    const res = await axios.get('https://saviya-kolla-api.koyeb.app/news/gagana');
    if (!res.data?.status || !res.data.result) return await socket.sendMessage(sender, { text: '❌ Failed to fetch Gagana News.' }, { quoted: botMention });

    const n = res.data.result;
    const caption = `📰 *${n.title}*\n\n*📅 𝐃ate:* ${n.date}\n*⏰ 𝐓ime:* ${n.time}\n\n${n.desc}\n\n*🔗 [Read more]* (${n.url})\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`;

    await socket.sendMessage(sender, { image: { url: n.image }, caption, contextInfo: { mentionedJid: [sender] } }, { quoted: botMention });

  } catch (err) {
    console.error('gagananews error:', err);
    await socket.sendMessage(sender, { text: '❌ Error fetching Gagana News.' }, { quoted: botMention });
  }
  break;
}


//💐💐💐💐💐💐

        case 'unfollow': {
  const jid = args[0] ? args[0].trim() : null;
  if (!jid) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❗ Provide channel JID to unfollow. Example:\n.unfollow 120363396379901844@newsletter' }, { quoted: shonux });
  }

  const admins = await loadAdminsFromMongo();
  const normalizedAdmins = admins.map(a => (a || '').toString());
  const senderIdSimple = (nowsender || '').includes('@') ? nowsender.split('@')[0] : (nowsender || '');
  const isAdmin = normalizedAdmins.includes(nowsender) || normalizedAdmins.includes(senderNumber) || normalizedAdmins.includes(senderIdSimple);
  if (!(isOwner || isAdmin)) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW2" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    return await socket.sendMessage(sender, { text: '❌ Permission denied. Only owner or admins can remove channels.' }, { quoted: shonux });
  }

  if (!jid.endsWith('@newsletter')) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW3" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    return await socket.sendMessage(sender, { text: '❗ Invalid JID. Must end with @newsletter' }, { quoted: shonux });
  }

  try {
    if (typeof socket.newsletterUnfollow === 'function') {
      await socket.newsletterUnfollow(jid);
    }
    await removeNewsletterFromMongo(jid);

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW4" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Unfollowed and removed from DB: ${jid}` }, { quoted: shonux });
  } catch (e) {
    console.error('unfollow error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_UNFOLLOW5" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `❌ Failed to unfollow: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}

case 'tt': 
case 'tiktokdl': {
    const axios = require('axios');
    
    await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });
    
    const q = msg.message?.conversation ||
              msg.message?.extendedTextMessage?.text ||
              msg.message?.imageMessage?.caption ||
              msg.message?.videoMessage?.caption || '';
    
    // Extract the TikTok URL
    const url = q.replace(/^[.\/!]?(tt|tiktokdl)\s*/i, '').trim();
    
    if (!url) {
        return await socket.sendMessage(sender, {
            text: '*📌 Usage:* .tt <tiktok_url>\n*Example:* .tt https://vt.tiktok.com/ZS57nHKP8/'
        }, { quoted: msg });
    }
    
    // Check if it's a TikTok URL
    if (!url.includes('tiktok.com') && !url.includes('vt.tiktok')) {
        return await socket.sendMessage(sender, {
            text: '❌ *Invalid TikTok URL.*\nඔබ TikTok video link එකක් දෙන්න ඕනෙ!'
        }, { quoted: msg });
    }
    
    try {
        // Send processing message
        await socket.sendMessage(sender, {
            text: '*⏳ᴅᴏᴡɴʟᴏᴀᴅɪɴɢ ʏᴏᴜʀ ᴛɪᴋᴛᴏᴋ ᴠɪᴅᴇᴏ.....*'
        }, { quoted: msg });
        
        // Use tikwm.com API for downloading (same as your search function)
        const downloadUrl = `https://tikwm.com/api/?url=${encodeURIComponent(url)}&hd=1`;
        
        const response = await axios.get(downloadUrl, {
            headers: {
                'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36',
                'Accept': 'application/json'
            }
        });
        
        const data = response.data;
        
        if (data.code !== 0 || !data.data) {
            throw new Error(data.msg || 'Failed to fetch video');
        }
        
        const videoData = data.data;
        
        // Get video URL (prefer HD, then play/wm)
        const videoUrl = videoData.hdplay || videoData.play || videoData.wm || videoData.download;
        
        if (!videoUrl) {
            throw new Error('No video URL found');
        }
        
        // Get bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';
        
        // Create caption
        const caption = `*${botName} ᴛɪᴋᴛᴏᴋ ᴠɪᴅᴇᴏ ᴅᴏᴡɴʟᴏᴀᴅ📥*
*╭────────────────*
*○─╮*
*├○📝Tɪᴛʟᴇ:* ${videoData.title || 'No Title'}
*├○👤Aᴜᴛʜᴏʀ:* ${videoData.author?.nickname || 'Unknown'}
*├○👍Lɪᴋᴇ:* ${videoData.digg_count || 0}
*├○💬Cᴏᴍᴍᴇɴᴛꜱ:* ${videoData.comment_count || 0}
*├○🔁Sʜᴀʀᴇ:* ${videoData.share_count || 0}
*├○📥Dᴏᴡɴʟᴏᴀᴅ:* ${videoData.download_count || 0}
*○─╯*
*╰────────────────*
> *𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3*`;
        
        // Send the video
        await socket.sendMessage(sender, {
            video: { url: videoUrl },
            caption: caption,
            gifPlayback: false
        }, { quoted: msg });
        
    } catch (error) {
        console.error('TikTok Download Error:', error);
        
        // Try alternative API if first one fails
        try {
            await socket.sendMessage(sender, {
                text: '*🔄 Trying alternative method...*'
            }, { quoted: msg });
            
            // Alternative API
            const altResponse = await axios.get(`https://api.tiklydown.eu.org/api/download?url=${encodeURIComponent(url)}`);
            const altData = altResponse.data;
            
            if (altData.data && altData.data.play) {
                const sanitized = (number || '').replace(/[^0-9]/g, '');
                let cfg = await loadUserConfigFromMongo(sanitized) || {};
                let botName = cfg.botName || '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';
                
                const caption = `*${botName} 𝗧ɪᴋᴛᴛᴏᴋ 𝗗ᴏᴡɴʟᴏᴀᴅᴇʀ*\n\nTitle: ${altData.data.title || 'No Title'}\nAuthor: ${altData.data.author.nickname || 'Unknown'}`;
                
                await socket.sendMessage(sender, {
                    video: { url: altData.data.play },
                    caption: caption
                }, { quoted: msg });
            } else {
                throw new Error('Alternative API also failed');
            }
            
        } catch (altError) {
            console.error('Alternative API Error:', altError);
            
            await socket.sendMessage(sender, {
                text: `❌ *Download Failed!*\n\nError: ${error.message}\n\nඔබට අවශ්‍ය නම්:\n1. TikTok link එක නිවැරදිද බලන්න\n2. Video එක public එකක්ද බලන්න\n3. නැත්තම් නැවත උත්සාහ කරන්න`
            }, { quoted: msg });
        }
    }
    
    break;
}
case 'sketch':
case 'art':
case 'pencil': {
    try {
        const axios = require('axios');
        const FormData = require('form-data');
        const { Readable } = require('stream');
        const { downloadContentFromMessage } = require('baileys');

        // 1. Check for Image (Reply or Caption)
        const quoted = msg.message?.extendedTextMessage?.contextInfo?.quotedMessage;
        const mime = msg.message?.imageMessage?.mimetype || quoted?.imageMessage?.mimetype;

        if (!mime || !mime.includes('image')) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please reply to an image to convert it to a sketch.*'
            }, { quoted: msg });
        }

        // 2. React & Notify
        await socket.sendMessage(sender, { react: { text: '🎨', key: msg.key } });
        await socket.sendMessage(sender, { text: '*⏳ Creating Sketch...*' }, { quoted: msg });

        // 3. Download Image
        const mediaStream = await downloadContentFromMessage(
            msg.message?.imageMessage || quoted?.imageMessage,
            'image'
        );
        let buffer = Buffer.from([]);
        for await (const chunk of mediaStream) buffer = Buffer.concat([buffer, chunk]);

        // 4. Upload to Catbox to get URL (Required for API)
        const form = new FormData();
        form.append('reqtype', 'fileupload');
        form.append('fileToUpload', Readable.from(buffer), { filename: 'image.jpg' });

        const uploadRes = await axios.post('https://catbox.moe/user/api.php', form, {
            headers: { ...form.getHeaders() }
        });
        const inputUrl = uploadRes.data.trim();

        // 5. Call Sketch API
        const apiUrl = `https://www.movanest.xyz/v2/sketch?image_url=${inputUrl}`;
        const apiRes = await axios.get(apiUrl);
        
        // Check API Response
        if (!apiRes.data.status || !apiRes.data.data) {
             throw new Error('API Error: Failed to create sketch.');
        }

        const resultUrl = apiRes.data.data;

        // 6. Config & Bot Details
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ꜱᴇʏᴀ xᴍᴅ 🧸';

        // 7. Prepare Interactive Button Message
        const { generateWAMessageFromContent, proto, prepareWAMessageMedia } = require('baileys');

        const imageMsg = await prepareWAMessageMedia({ image: { url: resultUrl } }, { upload: socket.waUploadToServer });

        const msgParams = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: {
                        deviceListMetadata: {},
                        deviceListMetadataVersion: 2
                    },
                    interactiveMessage: proto.Message.InteractiveMessage.create({
                        body: proto.Message.InteractiveMessage.Body.create({
                            text: `🎨 *SKETCH CREATED SUCCESSFULLY*\n\n🖌️ *Style:* Pencil Art\n🤖 *Engine:* Movanest AI\n\n*👑 POWERED BY ${botName}*`
                        }),
                        footer: proto.Message.InteractiveMessage.Footer.create({
                            text: "Queen Seya XMD • AI Tools"
                        }),
                        header: proto.Message.InteractiveMessage.Header.create({
                            title: "",
                            subtitle: "AI Result",
                            hasMediaAttachment: true,
                            imageMessage: imageMsg.imageMessage
                        }),
                        nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
                            buttons: [
                                {
                                    name: "cta_url",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "🔗 VIEW HD SKETCH",
                                        url: resultUrl,
                                        merchant_url: resultUrl
                                    })
                                },
                                {
                                    name: "quick_reply",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "📜 MAIN MENU",
                                        id: `${config.PREFIX}menu`
                                    })
                                }
                            ]
                        })
                    })
                }
            }
        }, { quoted: msg });

        // 8. Send Final Button Message
        await socket.relayMessage(sender, msgParams.message, { messageId: msgParams.key.id });

        // 9. Send as Document (Optional Backup)
        await socket.sendMessage(sender, { 
            document: { url: resultUrl }, 
            mimetype: "image/jpeg", 
            fileName: "Queen-imalsha-Sketch.jpg",
            caption: "> *Here is your sketch file* 📁\n> *𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3*"
        }, { quoted: msg });

    } catch (err) {
        console.error("Sketch Error:", err);
        await socket.sendMessage(sender, { 
            text: `❌ *Error:* ${err.message || "Failed to process image."}` 
        }, { quoted: msg });
    }
    break;
}
case 'xvideo': {
  try {
    // ---------------------------
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_XVIDEO" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    // ---------------------------

    if (!args[0]) return await socket.sendMessage(sender, { text: '*❌ Usage: .xvideo <url/query>*' }, { quoted: botMention });

    let video, isURL = false;
    if (args[0].startsWith('http')) { video = args[0]; isURL = true; } 
    else {
      await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } }, { quoted: botMention });
      const s = await axios.get(`https://saviya-kolla-api.koyeb.app/search/xvideos?query=${encodeURIComponent(args.join(' '))}`);
      if (!s.data?.status || !s.data.result?.length) throw new Error('No results');
      video = s.data.result[0];
    }

    const dlRes = await axios.get(`https://saviya-kolla-api.koyeb.app/download/xvideos?url=${encodeURIComponent(isURL ? video : video.url)}`);
    if (!dlRes.data?.status) throw new Error('Download API failed');

    const dl = dlRes.data.result;

    await socket.sendMessage(sender, {
      video: { url: dl.url },
      caption: `*📹 ${dl.title}*\n\n⏱️ ${isURL ? '' : `*𝐃uration:* ${video.duration}`}\n*👁️ 𝐕iews:* ${dl.views}\n👍 ${dl.likes} | 👎 ${dl.dislikes}\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`,
      mimetype: 'video/mp4'
    }, { quoted: botMention });

  } catch (err) {
    console.error('xvideo error:', err);
    await socket.sendMessage(sender, { text: '*❌ Failed to fetch video*' }, { quoted: botMention });
  }
  break;
}
case 'xvideo2': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_XVIDEO2" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    if (!args[0]) return await socket.sendMessage(sender, { text: '*❌ Usage: .xvideo2 <url/query>*' }, { quoted: botMention });

    let video = null, isURL = false;
    if (args[0].startsWith('http')) { video = args[0]; isURL = true; } 
    else {
      await socket.sendMessage(sender, { react: { text: '🔍', key: msg.key } }, { quoted: botMention });
      const s = await axios.get(`https://saviya-kolla-api.koyeb.app/search/xvideos?query=${encodeURIComponent(args.join(' '))}`);
      if (!s.data?.status || !s.data.result?.length) throw new Error('No results');
      video = s.data.result[0];
    }

    const dlRes = await axios.get(`https://saviya-kolla-api.koyeb.app/download/xvideos?url=${encodeURIComponent(isURL ? video : video.url)}`);
    if (!dlRes.data?.status) throw new Error('Download API failed');

    const dl = dlRes.data.result;

    await socket.sendMessage(sender, {
      video: { url: dl.url },
      caption: `*📹 ${dl.title}*\n\n⏱️ ${isURL ? '' : `*𝐃uration:* ${video.duration}`}\n*👁️ 𝐕iews:* ${dl.views}\n*👍 𝐋ikes:* ${dl.likes} | *👎 𝐃islikes:* ${dl.dislikes}\n\n*𝐏ᴏᴡᴇʀᴇᴅ 𝐁ʏ ${botName}*`,
      mimetype: 'video/mp4'
    }, { quoted: botMention });

  } catch (err) {
    console.error('xvideo2 error:', err);
    await socket.sendMessage(sender, { text: '*❌ Failed to fetch video*' }, { quoted: botMention });
  }
  break;
}
case 'xnxx':
case 'xnxxvideo': {
  try {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const userCfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = userCfg.botName || BOT_NAME_FANCY;

    const botMention = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_XNXX" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    if (!Array.isArray(config.PREMIUM) || !config.PREMIUM.includes(senderNumber)) 
      return await socket.sendMessage(sender, { text: '❗ This command is for Premium users only.' }, { quoted: botMention });

    if (!text) return await socket.sendMessage(sender, { text: '❌ Provide a search name. Example: .xnxx <name>' }, { quoted: botMention });

    await socket.sendMessage(from, { react: { text: "🎥", key: msg.key } }, { quoted: botMention });

    const res = await axios.get(`https://api.genux.me/api/download/xnxx-download?query=${encodeURIComponent(text)}&apikey=GENUX-SANDARUX`);
    const d = res.data?.result;
    if (!d || !d.files) return await socket.sendMessage(sender, { text: '❌ No results.' }, { quoted: botMention });

    await socket.sendMessage(from, { image: { url: d.image }, caption: `💬 *Title*: ${d.title}\n👀 *Duration*: ${d.duration}\n🗯 *Desc*: ${d.description}\n💦 *Tags*: ${d.tags || ''}` }, { quoted: botMention });

    await socket.sendMessage(from, { video: { url: d.files.high, fileName: d.title + ".mp4", mimetype: "video/mp4", caption: "*Done ✅*" } }, { quoted: botMention });

    await socket.sendMessage(from, { text: "*Uploaded ✅*" }, { quoted: botMention });

  } catch (err) {
    console.error('xnxx error:', err);
    await socket.sendMessage(sender, { text: "❌ Error fetching video." }, { quoted: botMention });
  }
  break;
}
case 'gjid':
case 'groupjid':
case 'grouplist': {
  try {
    // ✅ Owner check removed — now everyone can use it!

    await socket.sendMessage(sender, { 
      react: { text: "📝", key: msg.key } 
    });

    await socket.sendMessage(sender, { 
      text: "📝 Fetching group list..." 
    }, { quoted: msg });

    const groups = await socket.groupFetchAllParticipating();
    const groupArray = Object.values(groups);

    // Sort by creation time (oldest to newest)
    groupArray.sort((a, b) => a.creation - b.creation);

    if (groupArray.length === 0) {
      return await socket.sendMessage(sender, { 
        text: "❌ No groups found!" 
      }, { quoted: msg });
    }

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY || "Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2";

    // ✅ Pagination setup — 10 groups per message
    const groupsPerPage = 10;
    const totalPages = Math.ceil(groupArray.length / groupsPerPage);

    for (let page = 0; page < totalPages; page++) {
      const start = page * groupsPerPage;
      const end = start + groupsPerPage;
      const pageGroups = groupArray.slice(start, end);

      // ✅ Build message for this page
      const groupList = pageGroups.map((group, index) => {
        const globalIndex = start + index + 1;
        const memberCount = group.participants ? group.participants.length : 'N/A';
        const subject = group.subject || 'Unnamed Group';
        const jid = group.id;
        return `*${globalIndex}. ${subject}*\n*👥 𝐌embers:* ${memberCount}\n🆔 ${jid}`;
      }).join('\n\n');

      const textMsg = `📝 *𝐆roup 𝐋ist* - ${botName}*\n\n*📄 𝐏age:* ${page + 1}/${totalPages}\n*👥 𝐓otal 𝐆roups:* ${groupArray.length}\n\n${groupList}`;

      await socket.sendMessage(sender, {
        text: textMsg,
        footer: `🤖 Powered by ${botName}`
      });

      // Add short delay to avoid spam
      if (page < totalPages - 1) {
        await delay(1000);
      }
    }

  } catch (err) {
    console.error('GJID command error:', err);
    await socket.sendMessage(sender, { 
      text: "❌ Failed to fetch group list. Please try again later." 
    }, { quoted: msg });
  }
  break;
}
case 'nanobanana': {
  const fs = require('fs');
  const path = require('path');
  const { GoogleGenAI } = require("@google/genai");

  // 🧩 Helper: Download quoted image
  async function downloadQuotedImage(socket, msg) {
    try {
      const ctx = msg.message?.extendedTextMessage?.contextInfo;
      if (!ctx || !ctx.quotedMessage) return null;

      const quoted = ctx.quotedMessage;
      const imageMsg = quoted.imageMessage || quoted[Object.keys(quoted).find(k => k.endsWith('Message'))];
      if (!imageMsg) return null;

      if (typeof socket.downloadMediaMessage === 'function') {
        const quotedKey = {
          remoteJid: msg.key.remoteJid,
          id: ctx.stanzaId,
          participant: ctx.participant || undefined
        };
        const fakeMsg = { key: quotedKey, message: ctx.quotedMessage };
        const stream = await socket.downloadMediaMessage(fakeMsg, 'image');
        const bufs = [];
        for await (const chunk of stream) bufs.push(chunk);
        return Buffer.concat(bufs);
      }

      return null;
    } catch (e) {
      console.error('downloadQuotedImage err', e);
      return null;
    }
  }

  // ⚙️ Main command logic
  try {
    const promptRaw = args.join(' ').trim();
    if (!promptRaw && !msg.message?.extendedTextMessage?.contextInfo?.quotedMessage) {
      return await socket.sendMessage(sender, {
        text: "📸 *Usage:* `.nanobanana <prompt>`\n💬 Or reply to an image with `.nanobanana your prompt`"
      }, { quoted: msg });
    }

    await socket.sendMessage(sender, { react: { text: "🎨", key: msg.key } });

    const imageBuf = await downloadQuotedImage(socket, msg);
    await socket.sendMessage(sender, {
      text: `🔮 *Generating image...*\n🖊️ Prompt: ${promptRaw || '(no text)'}\n📷 Mode: ${imageBuf ? 'Edit (Image + Prompt)' : 'Text to Image'}`
    }, { quoted: msg });

    // 🧠 Setup Gemini SDK
    const ai = new GoogleGenAI({
      apiKey: process.env.GEMINI_API_KEY || "AIzaSyB6ZQwLHZFHxDCbBFJtc0GIN2ypdlga4vw"
    });

    // 🧩 Build contents
    const contents = imageBuf
      ? [
          { role: "user", parts: [{ inlineData: { mimeType: "image/jpeg", data: imageBuf.toString("base64") } }, { text: promptRaw }] }
        ]
      : [
          { role: "user", parts: [{ text: promptRaw }] }
        ];

    // ✨ Generate Image using Gemini SDK
    const response = await ai.models.generateContent({
      model: "gemini-2.5-flash-image",
      contents,
    });

    // 🖼️ Extract Image Data
    const part = response.candidates?.[0]?.content?.parts?.find(p => p.inlineData);
    if (!part) {
      console.log('Gemini response:', response);
      throw new Error('⚠️ No image data returned from Gemini API.');
    }

    const imageData = part.inlineData.data;
    const buffer = Buffer.from(imageData, "base64");

    const tmpPath = path.join(__dirname, `gemini-nano-${Date.now()}.png`);
    fs.writeFileSync(tmpPath, buffer);

    await socket.sendMessage(sender, {
      image: fs.readFileSync(tmpPath),
      caption: `✅ *Here you go!*\n🎨 Prompt: ${promptRaw}`
    }, { quoted: msg });

    try { fs.unlinkSync(tmpPath); } catch {}

  } catch (err) {
    console.error('nanobanana error:', err);
    await socket.sendMessage(sender, { text: `❌ *Error:* ${err.message || err}` }, { quoted: msg });
  }
  break;
}

//ᴄᴀʀʏ ꜱᴀɴᴜ xᴅ ᴏʟᴅ ʀᴇᴘʟʏ ᴄᴀꜱᴇ ᴀᴅᴅ📍

case 'savecontact':
case 'gvcf2':
case 'scontact':
case 'savecontacts': {
  try {
    const text = args.join(" ").trim(); // ✅ Define text variable

    if (!text) {
      return await socket.sendMessage(sender, { 
        text: "🍁 *Usage:* .savecontact <group JID>\n📥 Example: .savecontact 9477xxxxxxx-123@g.us" 
      }, { quoted: msg });
    }

    const groupJid = text.trim();

    // ✅ Validate JID
    if (!groupJid.endsWith('@g.us')) {
      return await socket.sendMessage(sender, { 
        text: "❌ *Invalid group JID*. Must end with @g.us" 
      }, { quoted: msg });
    }

    let groupMetadata;
    try {
      groupMetadata = await socket.groupMetadata(groupJid);
    } catch {
      return await socket.sendMessage(sender, { 
        text: "❌ *Invalid group JID* or bot not in that group.*" 
      }, { quoted: msg });
    }

    const { participants, subject } = groupMetadata;
    let vcard = '';
    let index = 1;

    await socket.sendMessage(sender, { 
      text: `🔍 Fetching contact names from *${subject}*...` 
    }, { quoted: msg });

    // ✅ Loop through each participant
    for (const participant of participants) {
      const num = participant.id.split('@')[0];
      let name = num; // default name = number

      try {
        // Try to fetch from contacts or participant
        const contact = socket.contacts?.[participant.id] || {};
        if (contact?.notify) name = contact.notify;
        else if (contact?.vname) name = contact.vname;
        else if (contact?.name) name = contact.name;
        else if (participant?.name) name = participant.name;
      } catch {
        name = `Contact-${index}`;
      }

      // ✅ Add vCard entry
      vcard += `BEGIN:VCARD\n`;
      vcard += `VERSION:3.0\n`;
      vcard += `FN:${index}. ${name}\n`; // 👉 Include index number + name
      vcard += `TEL;type=CELL;type=VOICE;waid=${num}:+${num}\n`;
      vcard += `END:VCARD\n`;
      index++;
    }

    // ✅ Create a safe file name from group name
    const safeSubject = subject.replace(/[^\w\s]/gi, "_");
    const tmpDir = path.join(os.tmpdir(), `contacts_${Date.now()}`);
    fs.ensureDirSync(tmpDir);

    const filePath = path.join(tmpDir, `contacts-${safeSubject}.vcf`);
    fs.writeFileSync(filePath, vcard.trim());

    await socket.sendMessage(sender, { 
      text: `📁 *${participants.length}* contacts found in group *${subject}*.\n💾 Preparing VCF file...`
    }, { quoted: msg });

    await delay(1500);

    // ✅ Send the .vcf file
    await socket.sendMessage(sender, {
      document: fs.readFileSync(filePath),
      mimetype: 'text/vcard',
      fileName: `contacts-${safeSubject}.vcf`,
      caption: `✅ *Contacts Exported Successfully!*\n👥 Group: *${subject}*\n📇 Total Contacts: *${participants.length}*\n\n> ᴘᴏᴡᴇʀᴇᴅ ʙʏ 𝙲𝙷𝙼𝙰 𝙼𝙳`
    }, { quoted: msg });

    // ✅ Cleanup temp file
    try {
      if (fs.existsSync(filePath)) fs.unlinkSync(filePath);
    } catch (cleanupError) {
      console.warn('Failed to cleanup temp file:', cleanupError);
    }

  } catch (err) {
    console.error('Save contact error:', err);
    await socket.sendMessage(sender, { 
      text: `❌ Error: ${err.message || err}` 
    }, { quoted: msg });
  }
  break;
}

case 'fancy':
case 'fancytext':
case 'font': {
    try {
        const axios = require('axios');

        // 1. Get User Input
        const text = (args.join(' ') || '').trim();
        
        // 2. Config & Bot Details
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ꜱᴇʏᴀ xᴍᴅ 🧸';

        // 3. Fake Contact for Quoting
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FANCY"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        if (!text) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please provide text to convert.*\n\n📌 *Example:* .fancy Queen Seya'
            }, { quoted: shonux });
        }

        // 4. React & Fetch Data
        await socket.sendMessage(sender, { react: { text: '✍️', key: msg.key } });

        const apiUrl = `https://www.movanest.xyz/v2/fancytext?word=${encodeURIComponent(text)}`;
        const res = await axios.get(apiUrl);

        if (!res.data.status || !res.data.data) {
            throw new Error('API Error: No data found.');
        }

        // 5. Format the Output List
        // The API likely returns an array of strings or objects. We handle both.
        let fancyList = res.data.data;
        let msgBody = `🎨 *FANCY TEXT GENERATOR*\n\n🖊️ *Original:* ${text}\n\n`;

        if (Array.isArray(fancyList)) {
            fancyList.slice(0, 50).forEach((style, index) => {
                // If the API returns objects, use style.result, otherwise use style directly
                const styleText = typeof style === 'object' ? (style.result || style.text) : style;
                msgBody += `*${index + 1}.* ${styleText}\n`;
            });
        } else {
            msgBody += fancyList;
        }

        msgBody += `\n*👑 POWERED BY ${botName}*`;

        // 6. Send Interactive Message (Card Style)
        const { generateWAMessageFromContent, proto } = require('baileys');

        const msgParams = generateWAMessageFromContent(sender, {
            viewOnceMessage: {
                message: {
                    messageContextInfo: {
                        deviceListMetadata: {},
                        deviceListMetadataVersion: 2
                    },
                    interactiveMessage: proto.Message.InteractiveMessage.create({
                        body: proto.Message.InteractiveMessage.Body.create({
                            text: msgBody
                        }),
                        footer: proto.Message.InteractiveMessage.Footer.create({
                            text: "Queen imalsha md • Typography"
                        }),
                        header: proto.Message.InteractiveMessage.Header.create({
                            title: "",
                            subtitle: "Fancy Fonts",
                            hasMediaAttachment: false // Text only mode for easy copying
                        }),
                        contextInfo: {
                            externalAdReply: {
                                title: "✍️ FANCY FONT STUDIO",
                                body: botName,
                                thumbnailUrl: "https://files.catbox.moe/4jjn1i.jpg", // Using your image
                                sourceUrl: apiUrl,
                                mediaType: 1,
                                renderLargerThumbnail: true
                            }
                        },
                        nativeFlowMessage: proto.Message.InteractiveMessage.NativeFlowMessage.create({
                            buttons: [
                                {
                                    name: "quick_reply",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "📜 MAIN MENU",
                                        id: `${config.PREFIX}menu`
                                    })
                                },
                                {
                                    name: "cta_copy",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "📋 COPY BOT NAME",
                                        copy_code: botName,
                                        id: "copy_name"
                                    })
                                }
                            ]
                        })
                    })
                }
            }
        }, { quoted: shonux });

        await socket.relayMessage(sender, msgParams.message, { messageId: msgParams.key.id });

    } catch (err) {
        console.error("Fancy Text Error:", err);
        await socket.sendMessage(sender, { 
            text: `❌ *Error:* ${err.message || "Failed to generate fonts."}` 
        }, { quoted: msg });
    }
    break;
}
case 'mediafire':
case 'mf':
case 'mfdl': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const url = text.split(" ")[1]; // .mediafire <link>

        // ✅ Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        // ✅ Fake Meta contact message (like Facebook style)
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_MEDIAFIRE"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        if (!url) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please send a MediaFire link.*\n\nExample: .mediafire <url>'
            }, { quoted: shonux });
        }

        // ⏳ Notify start
        await socket.sendMessage(sender, { react: { text: '📥', key: msg.key } });
        await socket.sendMessage(sender, { text: '*⏳ Fetching MediaFire file info...*' }, { quoted: shonux });

        // 🔹 Call API
        let api = `https://tharuzz-ofc-apis.vercel.app/api/download/mediafire?url=${encodeURIComponent(url)}`;
        let { data } = await axios.get(api);

        if (!data.success || !data.result) {
            return await socket.sendMessage(sender, { text: '❌ *Failed to fetch MediaFire file.*' }, { quoted: shonux });
        }

        const result = data.result;
        const title = result.title || result.filename;
        const filename = result.filename;
        const fileSize = result.size;
        const downloadUrl = result.url;

        const caption = `📦 *${title}*\n\n` +
                        `📁 *𝐅ilename:* ${filename}\n` +
                        `📏 *𝐒ize:* ${fileSize}\n` +
                        `🌐 *𝐅rom:* ${result.from}\n` +
                        `📅 *𝐃ate:* ${result.date}\n` +
                        `🕑 *𝐓ime:* ${result.time}\n\n` +
                        `*✅ 𝐃ownloaded 𝐁y ${botName}*`;

        // 🔹 Send file automatically (document type for .zip etc.)
        await socket.sendMessage(sender, {
            document: { url: downloadUrl },
            fileName: filename,
            mimetype: 'application/octet-stream',
            caption: caption
        }, { quoted: shonux });

    } catch (err) {
        console.error("Error in MediaFire downloader:", err);

        // ✅ In catch also send Meta mention style
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_MEDIAFIRE"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
    }
    break;
}
case 'apksearch':
case 'apks':
case 'apkfind': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const query = text.split(" ").slice(1).join(" ").trim();

        // ✅ Load bot name dynamically
        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        // ✅ Fake Meta contact message
        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_APK"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        if (!query) {
            return await socket.sendMessage(sender, {
                text: '🚫 *Please provide an app name to search.*\n\nExample: .apksearch whatsapp',
                buttons: [
                    { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📋 MENU' }, type: 1 }
                ]
            }, { quoted: shonux });
        }

        await socket.sendMessage(sender, { text: '*⏳ Searching APKs...*' }, { quoted: shonux });

        // 🔹 Call API
        const apiUrl = `https://tharuzz-ofc-apis.vercel.app/api/search/apksearch?query=${encodeURIComponent(query)}`;
        const { data } = await axios.get(apiUrl);

        if (!data.success || !data.result || !data.result.length) {
            return await socket.sendMessage(sender, { text: '*❌ No APKs found for your query.*' }, { quoted: shonux });
        }

        // 🔹 Format results
        let message = `🔍 *APK Search Results for:* ${query}\n\n`;
        data.result.slice(0, 20).forEach((item, idx) => {
            message += `*${idx + 1}.* ${item.name}\n➡️ ID: \`${item.id}\`\n\n`;
        });
        message += `*𝐏owered 𝐁y ${botName}*`;

        // 🔹 Send results
        await socket.sendMessage(sender, {
            text: message,
            buttons: [
                { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 },
                { buttonId: `${config.PREFIX}alive`, buttonText: { displayText: '📡 𝐁𝙾𝚃 𝐈𝙽𝙵𝙾' }, type: 1 }
            ],
            contextInfo: { mentionedJid: [sender] }
        }, { quoted: shonux });

    } catch (err) {
        console.error("Error in APK search:", err);

        const sanitized = (number || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

        const shonux = {
            key: {
                remoteJid: "status@broadcast",
                participant: "0@s.whatsapp.net",
                fromMe: false,
                id: "META_AI_FAKE_ID_APK"
            },
            message: {
                contactMessage: {
                    displayName: botName,
                    vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002
END:VCARD`
                }
            }
        };

        await socket.sendMessage(sender, { text: '*❌ Internal Error. Please try again later.*' }, { quoted: shonux });
    }
    break;
}

case 'xvdl2':
case 'xvnew': {
    try {
        const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const query = text.split(" ").slice(1).join(" ").trim();

        if (!query) return await socket.sendMessage(sender, { text: '🚫 Please provide a search query.\nExample: .xv mia' }, { quoted: msg });

        // 1️⃣ Send searching message
        await socket.sendMessage(sender, { text: '*⏳ Searching XVideos...*' }, { quoted: msg });

        // 2️⃣ Call search API
        const searchRes = await axios.get(`https://tharuzz-ofc-api-v2.vercel.app/api/search/xvsearch?query=${encodeURIComponent(query)}`);
        const videos = searchRes.data.result?.xvideos?.slice(0, 10);
        if (!videos || videos.length === 0) return await socket.sendMessage(sender, { text: '*❌ No results found.*' }, { quoted: msg });

        // 3️⃣ Prepare list message
        let listMsg = `🔍 *XVideos Results for:* ${query}\n\n`;
        videos.forEach((vid, idx) => {
            listMsg += `*${idx + 1}.* ${vid.title}\n${vid.info}\n➡️ ${vid.link}\n\n`;
        });
        listMsg += '_Reply with the number to download the video._';

        await socket.sendMessage(sender, { text: listMsg }, { quoted: msg });

        // 4️⃣ Cache results for reply handling
        global.xvCache = global.xvCache || {};
        global.xvCache[sender] = videos.map(v => v.link);

    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '*❌ Error occurred.*' }, { quoted: msg });
    }
}
break;


// Handle reply to download selected video
case 'xvselect': {
    try {
        const replyText = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
        const selection = parseInt(replyText);

        const links = global.xvCache?.[sender];
        if (!links || isNaN(selection) || selection < 1 || selection > links.length) {
            return await socket.sendMessage(sender, { text: '🚫 Invalid selection number.' }, { quoted: msg });
        }

        const videoUrl = links[selection - 1];

        await socket.sendMessage(sender, { text: '*⏳ Downloading video...*' }, { quoted: msg });

        // Call download API
        const dlRes = await axios.get(`https://tharuzz-ofc-api-v2.vercel.app/api/download/xvdl?url=${encodeURIComponent(videoUrl)}`);
        const result = dlRes.data.result;

        if (!result) return await socket.sendMessage(sender, { text: '*❌ Failed to fetch video.*' }, { quoted: msg });

        // Send video
        await socket.sendMessage(sender, {
            video: { url: result.dl_Links.highquality },
            caption: `🎥 *${result.title}*\n⏱ Duration: ${result.duration}s`,
            jpegThumbnail: result.thumbnail ? await axios.get(result.thumbnail, { responseType: 'arraybuffer' }).then(res => Buffer.from(res.data)) : undefined
        }, { quoted: msg });

        // Clear cache
        delete global.xvCache[sender];

    } catch (err) {
        console.error(err);
        await socket.sendMessage(sender, { text: '*❌ Error downloading video.*' }, { quoted: msg });
    }
}
break;

// ---------------- list saved newsletters (show emojis) ----------------
case 'newslist': {
  try {
    const docs = await listNewslettersFromMongo();
    if (!docs || docs.length === 0) {
      let userCfg = {};
      try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
      const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
      const shonux = {
          key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_NEWSLIST" },
          message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      return await socket.sendMessage(sender, { text: '📭 No channels saved in DB.' }, { quoted: shonux });
    }

    let txt = '*📚 Saved Newsletter Channels:*\n\n';
    for (const d of docs) {
      txt += `• ${d.jid}\n  Emojis: ${Array.isArray(d.emojis) && d.emojis.length ? d.emojis.join(' ') : '(default)'}\n\n`;
    }

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_NEWSLIST2" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: txt }, { quoted: shonux });
  } catch (e) {
    console.error('newslist error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_NEWSLIST3" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Failed to list channels.' }, { quoted: shonux });
  }
  break;
}
case 'cid': {
    const { prepareWAMessageMedia, generateWAMessageFromContent, proto } = require('baileys'); // Added imports

    try {
        const sanitized = (sender || '').replace(/[^0-9]/g, '');
        let cfg = await loadUserConfigFromMongo(sanitized) || {};
        let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
        const q = msg.message?.conversation ||
                  msg.message?.extendedTextMessage?.text ||
                  msg.message?.imageMessage?.caption ||
                  msg.message?.videoMessage?.caption || '';

        const channelLink = q.replace(/^[.\/!]cid\s*/i, '').trim();
        if (!channelLink) {
            return await socket.sendMessage(sender, {
                text: '❎ *Please provide a WhatsApp Channel link.*\n\n📌 *Example:* `.cid https://whatsapp.com/channel/xxx`'
            }, { quoted: msg });
        }

        const match = channelLink.match(/whatsapp\.com\/channel\/([\w-]+)/);
        if (!match) {
            return await socket.sendMessage(sender, { text: '⚠️ Invalid Link Format.' }, { quoted: msg });
        }

        const inviteId = match[1];
        await socket.sendMessage(sender, { react: { text: '🔎', key: msg.key } });
        const metadata = await socket.newsletterMetadata("invite", inviteId);

        if (!metadata || !metadata.id) {
            return await socket.sendMessage(sender, { text: '❌ Channel Not Found.' }, { quoted: msg });
        }
        let profileUrl = metadata.preview 
            ? (metadata.preview.startsWith('http') ? metadata.preview : `https://pps.whatsapp.net${metadata.preview}`) 
            : "https://cdn-icons-png.flaticon.com/512/10631/10631897.png";

        const createdDate = metadata.creation_time 
            ? new Date(metadata.creation_time * 1000).toLocaleDateString("en-US") 
            : 'Unknown';

        const subscribers = metadata.subscribers ? metadata.subscribers.toLocaleString() : 'Hidden';
        const mediaMessage = await prepareWAMessageMedia(
            { image: { url: profileUrl } }, 
            { upload: socket.waUploadToServer }
        );
        const msgContent = {
            viewOnceMessage: {
                message: {
                    interactiveMessage: {
                        header: {
                            title: `📡𝖈𝖍𝖆𝖓𝖓𝖊𝖑 𝖎𝖓𝖋𝖔`,
                            hasMediaAttachment: true,
                            imageMessage: mediaMessage.imageMessage
                        },
                        body: {
                            text: `
📌 *𝖓𝖆𝖒𝖊* ${metadata.name}
👥 *𝖘𝖚𝖇𝖘:* ${subscribers}
📅 *𝖈𝖗𝖊𝖆𝖙𝖊𝖉:* ${createdDate}
🆔 *𝖏𝖎𝖉* ${metadata.id}

_𝖈𝖑𝖎𝖈𝖐 𝖙𝖍𝖊 𝖇𝖚𝖙𝖙𝖔𝖓 𝖇𝖊𝖑𝖔𝖜 𝖙𝖔 𝖈𝖔𝖕𝖞 𝖙𝖍𝖊 𝖏𝖎𝖉_`
                        },
                        footer: {
                            text: `© ${botName}`
                        },
                        nativeFlowMessage: {
                            buttons: [
                                {
                                    name: "cta_copy",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "COPY JID",
                                        id: "copy_jid",
                                        copy_code: metadata.id
                                    })
                                },
                              
                            ]
                        }
                    }
                }
            }
        };
        const generatedMsg = generateWAMessageFromContent(sender, msgContent, { 
            userJid: sender, 
            quoted: msg 
        });
        await socket.relayMessage(sender, generatedMsg.message, { messageId: generatedMsg.key.id });
        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (err) {
        console.error("CID Error:", err);
        await socket.sendMessage(sender, { text: '❌ Error fetching channel data.' });
    }
    break;
}

case 'owner': {
  try {
    // vCard with multiple details
    let vcard = 
      'BEGIN:VCARD\n' +
      'VERSION:3.0\n' +
      'FN:YASAS\n' + // Name
      'ORG:WhatsApp Bot Developer;\n' + // Organization
      'TITLE:Founder & CEO of Dtec  Mini Bot;\n' + // Title / Role
      'EMAIL;type=INTERNET:hirunx@gmail.com\n' + // Email
      'ADR;type=WORK:;;Ratnapura;;Sri Lanka\n' + // Address
      'URL:https://github.com\n' + // Website
      'TEL;type=CELL;type=VOICE;waid=94721017862\n' + // WhatsApp Number
      'TEL;type=CELL;type=VOICE;waid=94721017862\n' + // Second Number (Owner)
      'END:VCARD';

    await conn.sendMessage(
      m.chat,
      {
        contacts: {
          displayName: 'ꜱᴀɴᴜ xᴅ',
          contacts: [{ vcard }]
        }
      },
      { quoted: m }
    );

  } catch (err) {
    console.error(err);
    await conn.sendMessage(m.chat, { text: '⚠️ Owner info fetch error.' }, { quoted: m });
  }
}
break;

case 'addadmin': {
  if (!args || args.length === 0) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❗ Provide a jid or number to add as admin\nExample: .addadmin 9477xxxxxxx' }, { quoted: shonux });
  }

  const jidOr = args[0].trim();
  if (!isOwner) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN2" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❌ Only owner can add admins.' }, { quoted: shonux });
  }

  try {
    await addAdminToMongo(jidOr);

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN3" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Added admin: ${jidOr}` }, { quoted: shonux });
  } catch (e) {
    console.error('addadmin error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADDADMIN4" },
        message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `❌ Failed to add admin: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}

case 'tagall': {
  try {
    if (!from || !from.endsWith('@g.us')) return await socket.sendMessage(sender, { text: '❌ This command can only be used in groups.' }, { quoted: msg });

    let gm = null;
    try { gm = await socket.groupMetadata(from); } catch(e) { gm = null; }
    if (!gm) return await socket.sendMessage(sender, { text: '❌ Failed to fetch group info.' }, { quoted: msg });

    const participants = gm.participants || [];
    if (!participants.length) return await socket.sendMessage(sender, { text: '❌ No members found in the group.' }, { quoted: msg });

    const text = args && args.length ? args.join(' ') : '📢 Announcement';

    let groupPP = 'https://i.ibb.co/9q2mG0Q/default-group.jpg';
    try { groupPP = await socket.profilePictureUrl(from, 'image'); } catch(e){}

    const mentions = participants.map(p => p.id || p.jid);
    const groupName = gm.subject || 'Group';
    const totalMembers = participants.length;

    const emojis = ['💞','💓','💖','💜','🤍','💔','❤️‍🔥','💕','❤️','💛','❤️‍🩹','💚','💙','💗'];
    const randomEmoji = emojis[Math.floor(Math.random() * emojis.length)];

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;

    // BotName meta mention
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_TAGALL" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let caption = `╭─❰ *🙊𝙶𝚁𝙾𝚄𝙿 𝙰𝙽𝙽𝙾𝚄𝙽𝙲𝙴𝙼𝙴𝙽𝚃* ❱─╮\n`;
    caption += `│ 📌 *𝐆roup:* ${groupName}\n`;
    caption += `│ 👥 *𝐌embers:* ${totalMembers}\n`;
    caption += `│ 💬 *𝐌essage:* ${text}\n`;
    caption += `╰───────────────╯\n\n`;
    caption += `📍 *𝙼𝙴𝙽𝚃𝙸𝙾𝙽𝙸𝙽𝙶 𝙰𝙻𝙻 𝙼𝙴𝙼𝙱𝙴𝚁𝚂 𝙱𝙴𝙻𝙾𝚆🙊:*\n\n`;
    for (const m of participants) {
      const id = (m.id || m.jid);
      if (!id) continue;
      caption += `${randomEmoji} @${id.split('@')[0]}\n`;
    }
    caption += `\n━━⊱ *${botName}* ⊰━━`;

    await socket.sendMessage(from, {
      image: { url: groupPP },
      caption,
      mentions,
    }, { quoted: metaQuote }); // <-- botName meta mention

  } catch (err) {
    console.error('tagall error', err);
    await socket.sendMessage(sender, { text: '❌ Error running tagall.' }, { quoted: msg });
  }
  break;
}

case 'ig':
case 'insta':
case 'instagram': {
  try {
    const text = (msg.message.conversation || msg.message.extendedTextMessage?.text || '').trim();
    const q = text.split(" ").slice(1).join(" ").trim();

    // Validate
    if (!q) {
      await socket.sendMessage(sender, { 
        text: '*🚫 Please provide an Instagram post/reel link.*',
        buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 }]
      });
      return;
    }

    const igRegex = /(?:https?:\/\/)?(?:www\.)?instagram\.com\/[^\s]+/;
    if (!igRegex.test(q)) {
      await socket.sendMessage(sender, { 
        text: '*🚫 Invalid Instagram link.*',
        buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 }]
      });
      return;
    }

    await socket.sendMessage(sender, { react: { text: '🎥', key: msg.key } });
    await socket.sendMessage(sender, { text: '*⏳ Downloading Instagram media...*' });

    // 🔹 Load session bot name
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    let botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    // 🔹 Meta style fake contact
    const shonux = {
      key: {
        remoteJid: "status@broadcast",
        participant: "0@s.whatsapp.net",
        fromMe: false,
        id: "META_AI_FAKE_ID_002"
      },
      message: {
        contactMessage: {
          displayName: botName, // dynamic bot name
          vcard: `BEGIN:VCARD
VERSION:3.0
N:${botName};;;;
FN:${botName}
ORG:Meta Platforms
TEL;type=CELL;type=VOICE;waid=13135550003:+1 313 555 0003
END:VCARD`
        }
      }
    };

    // API request
    let apiUrl = `https://delirius-apiofc.vercel.app/download/instagram?url=${encodeURIComponent(q)}`;
    let { data } = await axios.get(apiUrl).catch(() => ({ data: null }));

    // Backup API if first fails
    if (!data?.status || !data?.downloadUrl) {
      const backupUrl = `https://api.tiklydown.me/api/instagram?url=${encodeURIComponent(q)}`;
      const backup = await axios.get(backupUrl).catch(() => ({ data: null }));
      if (backup?.data?.video) {
        data = {
          status: true,
          downloadUrl: backup.data.video
        };
      }
    }

    if (!data?.status || !data?.downloadUrl) {
      await socket.sendMessage(sender, { 
        text: '*🚩 Failed to fetch Instagram video.*',
        buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📄 𝐌𝙰𝙸𝙽 𝐌𝙴𝙽𝚄' }, type: 1 }]
      });
      return;
    }

    // Caption (Dynamic Bot Name)
    const titleText = `*📸 ${botName} 𝐈ɴꜱᴛᴀɢʀᴀᴍ 𝐃ᴏᴡɴʟᴏᴀᴅᴇʀ*`;
    const content = `┏━━━━━━━━━━━━━━━━\n` +
                    `┃📌 \`𝐒ource\` : Instagram\n` +
                    `┃📹 \`𝐓ype\` : Video/Reel\n` +
                    `┗━━━━━━━━━━━━━━━━`;

    const footer = `🤖 ${botName}`;
    const captionMessage = typeof formatMessage === 'function'
      ? formatMessage(titleText, content, footer)
      : `${titleText}\n\n${content}\n${footer}`;

    // Send video with fake contact quoted
    await socket.sendMessage(sender, {
      video: { url: data.downloadUrl },
      caption: captionMessage,
      contextInfo: { mentionedJid: [sender] },
      buttons: [
        { buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📋 MENU' }, type: 1 },
        { buttonId: `${config.PREFIX}alive`, buttonText: { displayText: '🤖 BOT INFO' }, type: 1 }
      ]
    }, { quoted: shonux }); // 🔹 fake contact quoted

  } catch (err) {
    console.error("Error in Instagram downloader:", err);
    await socket.sendMessage(sender, { 
      text: '*❌ Internal Error. Please try again later.*',
      buttons: [{ buttonId: `${config.PREFIX}menu`, buttonText: { displayText: '📋 MENU' }, type: 1 }]
    });
  }
  break;
}

case 'online': {
  try {
    if (!(from || '').endsWith('@g.us')) {
      await socket.sendMessage(sender, { text: '❌ This command works only in group chats.' }, { quoted: msg });
      break;
    }

    let groupMeta;
    try { groupMeta = await socket.groupMetadata(from); } catch (err) { console.error(err); break; }

    const callerJid = (nowsender || '').replace(/:.*$/, '');
    const callerId = callerJid.includes('@') ? callerJid : `${callerJid}@s.whatsapp.net`;
    const ownerNumberClean = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    const isOwnerCaller = callerJid.startsWith(ownerNumberClean);
    const groupAdmins = (groupMeta.participants || []).filter(p => p.admin === 'admin' || p.admin === 'superadmin').map(p => p.id);
    const isGroupAdminCaller = groupAdmins.includes(callerId);

    if (!isOwnerCaller && !isGroupAdminCaller) {
      await socket.sendMessage(sender, { text: '❌ Only group admins or the bot owner can use this command.' }, { quoted: msg });
      break;
    }

    try { await socket.sendMessage(sender, { text: '🔄 Scanning for online members... please wait ~15 seconds' }, { quoted: msg }); } catch(e){}

    const participants = (groupMeta.participants || []).map(p => p.id);
    const onlineSet = new Set();
    const presenceListener = (update) => {
      try {
        if (update?.presences) {
          for (const id of Object.keys(update.presences)) {
            const pres = update.presences[id];
            if (pres?.lastKnownPresence && pres.lastKnownPresence !== 'unavailable') onlineSet.add(id);
            if (pres?.available === true) onlineSet.add(id);
          }
        }
      } catch (e) { console.warn('presenceListener error', e); }
    };

    for (const p of participants) {
      try { if (typeof socket.presenceSubscribe === 'function') await socket.presenceSubscribe(p); } catch(e){}
    }
    socket.ev.on('presence.update', presenceListener);

    const checks = 3; const intervalMs = 5000;
    await new Promise((resolve) => { let attempts=0; const iv=setInterval(()=>{ attempts++; if(attempts>=checks){ clearInterval(iv); resolve(); } }, intervalMs); });
    try { socket.ev.off('presence.update', presenceListener); } catch(e){}

    if (onlineSet.size === 0) {
      await socket.sendMessage(sender, { text: '⚠️ No online members detected (they may be hiding presence or offline).' }, { quoted: msg });
      break;
    }

    const onlineArray = Array.from(onlineSet).filter(j => participants.includes(j));
    const mentionList = onlineArray.map(j => j);

    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || BOT_NAME_FANCY;

    // BotName meta mention
    const metaQuote = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_ONLINE" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    let txt = `🟢 *𝐎nline 𝐌embers* — ${onlineArray.length}/${participants.length}\n\n`;
    onlineArray.forEach((jid, i) => {
      txt += `${i+1}. @${jid.split('@')[0]}\n`;
    });

    await socket.sendMessage(sender, {
      text: txt.trim(),
      mentions: mentionList
    }, { quoted: metaQuote }); // <-- botName meta mention

  } catch (err) {
    console.error('Error in online command:', err);
    try { await socket.sendMessage(sender, { text: '❌ An error occurred while checking online members.' }, { quoted: msg }); } catch(e){}
  }
  break;
}



case 'deladmin': {
  if (!args || args.length === 0) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN1" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❗ Provide a jid/number to remove\nExample: .deladmin 9477xxxxxxx' }, { quoted: shonux });
  }

  const jidOr = args[0].trim();
  if (!isOwner) {
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN2" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    return await socket.sendMessage(sender, { text: '❌ Only owner can remove admins.' }, { quoted: shonux });
  }

  try {
    await removeAdminFromMongo(jidOr);

    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN3" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Removed admin: ${jidOr}` }, { quoted: shonux });
  } catch (e) {
    console.error('deladmin error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_DELADMIN4" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `❌ Failed to remove admin: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}
case 'admins': {
  try {
    const list = await loadAdminsFromMongo();
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADMINS" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    if (!list || list.length === 0) {
      return await socket.sendMessage(sender, { text: 'No admins configured.' }, { quoted: shonux });
    }

    let txt = '*👑 Admins:*\n\n';
    for (const a of list) txt += `• ${a}\n`;

    await socket.sendMessage(sender, { text: txt }, { quoted: shonux });
  } catch (e) {
    console.error('admins error', e);
    let userCfg = {};
    try { if (number && typeof loadUserConfigFromMongo === 'function') userCfg = await loadUserConfigFromMongo((number || '').replace(/[^0-9]/g, '')) || {}; } catch(e){ userCfg = {}; }
    const title = userCfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2';
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_FAKE_ID_ADMINS2" },
      message: { contactMessage: { displayName: title, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${title};;;;\nFN:${title}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: '❌ Failed to list admins.' }, { quoted: shonux });
  }
  break;
}
case 'setlogo': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change this session logo.' }, { quoted: shonux });
    break;
  }

  const ctxInfo = (msg.message.extendedTextMessage || {}).contextInfo || {};
  const quotedMsg = ctxInfo.quotedMessage;
  const media = await downloadQuotedMedia(quotedMsg).catch(()=>null);
  let logoSetTo = null;

  try {
    if (media && media.buffer) {
      const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
      fs.ensureDirSync(sessionPath);
      const mimeExt = (media.mime && media.mime.split('/').pop()) || 'jpg';
      const logoPath = path.join(sessionPath, `logo.${mimeExt}`);
      fs.writeFileSync(logoPath, media.buffer);
      let cfg = await loadUserConfigFromMongo(sanitized) || {};
      cfg.logo = logoPath;
      await setUserConfigInMongo(sanitized, cfg);
      logoSetTo = logoPath;
    } else if (args && args[0] && (args[0].startsWith('http') || args[0].startsWith('https'))) {
      let cfg = await loadUserConfigFromMongo(sanitized) || {};
      cfg.logo = args[0];
      await setUserConfigInMongo(sanitized, cfg);
      logoSetTo = args[0];
    } else {
      const shonux = {
        key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO2" },
        message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
      };
      await socket.sendMessage(sender, { text: '❗ Usage: Reply to an image with `.setlogo` OR provide an image URL: `.setlogo https://example.com/logo.jpg`' }, { quoted: shonux });
      break;
    }

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Logo set for this session: ${logoSetTo}` }, { quoted: shonux });
  } catch (e) {
    console.error('setlogo error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETLOGO4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `❌ Failed to set logo: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}

case 'webcode':
case 'getcode': {
    const axios = require("axios");

    try {
        // ලින්ක් එකක් දීලා නැත්නම්
        if (!q) return reply("⚠️ කරුණාකර වෙබ් අඩවියක ලින්ක් එකක් ලබා දෙන්න. \n\n*උදාහරණ:* .viewcode https://google.com");

        // React එකක් දාමු
        try { await socket.sendMessage(from, { react: { text: "🔍", key: msg.key } }); } catch(e){}

        reply("🚀 *Fetching source code... Please wait!*");

        // API එක හරහා Code එක ගන්නවා
        const response = await axios.get(`https://api.allorigins.win/raw?url=${encodeURIComponent(q)}`);
        const sourceCode = response.data;

        if (!sourceCode) return reply("❌ කිසිදු කෝඩ් එකක් සොයාගත නොහැකි විය. ලින්ක් එක නිවැරදිදැයි බලන්න.");

        // කෝඩ් එක Text එකක් විදියට එවන්න බැරි තරම් දිග වෙන්න පුළුවන් නිසා .txt file එකක් විදියට යවනවා
        const fileName = 'source_code.txt';
        
        await socket.sendMessage(from, { 
            document: Buffer.from(sourceCode), 
            mimetype: 'text/plain', 
            fileName: fileName,
            caption: `✅ *Source Code of:* ${q}\n\n> *𝐏𝐎𝐖𝐄𝐑𝐃 𝘽𝙔 𝐀𝐒𝐇𝐈𝐘𝐀-𝐌𝐃 🥷*`
        }, { quoted: msg });

        await socket.sendMessage(from, { react: { text: "✅", key: msg.key } });

    } catch (error) {
        console.error('ViewCode Error:', error);
        reply("❌ දෝෂයක් ඇති විය. බොහෝ විට එම වෙබ් අඩවිය ආරක්ෂිත බැවින් කෝඩ් එක ලබාගත නොහැක.");
    }
    break;
}

case 'tourl':
case 'url':
case 'upload': {
    const axios = require('axios');
    const FormData = require('form-data');
    const fs = require('fs');
    const os = require('os');
    const path = require('path');
    const { downloadContentFromMessage, generateWAMessageFromContent, proto } = require('baileys');
    const quoted = msg.message?.extendedTextMessage?.contextInfo;
    const mime = quoted?.quotedMessage?.imageMessage?.mimetype || 
                 quoted?.quotedMessage?.videoMessage?.mimetype || 
                 quoted?.quotedMessage?.audioMessage?.mimetype || 
                 quoted?.quotedMessage?.documentMessage?.mimetype;

    if (!quoted || !mime) {
        return await socket.sendMessage(sender, { text: '❌ *Please reply to an image or video.*' }, { quoted: msg });
    }

    let mediaType;
    let msgKey;
    
    if (quoted.quotedMessage.imageMessage) {
        mediaType = 'image';
        msgKey = quoted.quotedMessage.imageMessage;
    } else if (quoted.quotedMessage.videoMessage) {
        mediaType = 'video';
        msgKey = quoted.quotedMessage.videoMessage;
    } else if (quoted.quotedMessage.audioMessage) {
        mediaType = 'audio';
        msgKey = quoted.quotedMessage.audioMessage;
    } else if (quoted.quotedMessage.documentMessage) {
        mediaType = 'document';
        msgKey = quoted.quotedMessage.documentMessage;
    }

    try {
        await socket.sendMessage(sender, { react: { text: '⬆️', key: msg.key } });

        const stream = await downloadContentFromMessage(msgKey, mediaType);
        let buffer = Buffer.alloc(0);
        for await (const chunk of stream) {
            buffer = Buffer.concat([buffer, chunk]);
        }

        const ext = mime.split('/')[1] || 'tmp';
        const tempFilePath = path.join(os.tmpdir(), `upload_${Date.now()}.${ext}`);
        fs.writeFileSync(tempFilePath, buffer);

        const form = new FormData();
        form.append('fileToUpload', fs.createReadStream(tempFilePath));
        form.append('reqtype', 'fileupload');

        const response = await axios.post('https://catbox.moe/user/api.php', form, { 
            headers: form.getHeaders() 
        });

        fs.unlinkSync(tempFilePath); 

        const mediaUrl = response.data.trim();
        const fileSize = (buffer.length / 1024 / 1024).toFixed(2) + ' MB';
        const typeStr = mediaType.charAt(0).toUpperCase() + mediaType.slice(1);


        let msgContent = {
            viewOnceMessage: {
                message: {
                    interactiveMessage: {
                        body: {
                            text: `📂 *Type:* ${typeStr}\n📊 *Size:* ${fileSize}\n\n🚀 *URL:* ${mediaUrl}\n\nᴘᴏᴡᴇʀᴅ ʙʏ 𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3`
                        },
                        footer: {
                            text: "Press button below to copy link✅"
                        },
                        header: {
                            title: "🔗 MEDIA UPLOADED",
                            hasMediaAttachment: false
                        },
                        nativeFlowMessage: {
                            buttons: [
                                {
                                    name: "cta_copy",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: "COPY LINK",
                                        id: "copy_url",
                                        copy_code: mediaUrl
                                    })
                                },
                                {
                                    name: "cta_url",
                                    buttonParamsJson: JSON.stringify({
                                        display_text: " OPEN LINK",
                                        url: mediaUrl,
                                        merchant_url: mediaUrl
                                    })
                                }
                            ]
                        }
                    }
                }
            }
        };


        const generatedMsg = generateWAMessageFromContent(sender, msgContent, { 
            userJid: sender, 
            quoted: msg 
        });

        
        await socket.relayMessage(sender, generatedMsg.message, { messageId: generatedMsg.key.id });
        await socket.sendMessage(sender, { react: { text: '✅', key: msg.key } });

    } catch (e) {
        console.error(e);
        await socket.sendMessage(sender, { text: `❌ *Error uploading media: ${e.message}*` }, { quoted: msg });
    }
    break;
}

case 'jid': {
    const sanitized = (number || '').replace(/[^0-9]/g, '');
    const cfg = await loadUserConfigFromMongo(sanitized) || {};
    const botName = cfg.botName || 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2'; // dynamic bot name

    const userNumber = sender.split('@')[0]; 

    // Reaction
    await socket.sendMessage(sender, { 
        react: { text: "🆔", key: msg.key } 
    });

    // Fake contact quoting for meta style
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_FAKE_ID" },
      message: { contactMessage: { displayName: botName, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Meta Platforms\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, {
        text: `*🆔 𝐂hat 𝐉ID:* ${sender}\n*📞 𝐘our 𝐍umber:* +${userNumber}`,
    }, { quoted: shonux });
    break;
}

// use inside your switch(command) { ... } block

case 'block': {
  try {
    // caller number (who sent the command)
    const callerNumberClean = (senderNumber || '').replace(/[^0-9]/g, '');
    const ownerNumberClean = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    const sessionOwner = (number || '').replace(/[^0-9]/g, '');

    // allow if caller is global owner OR this session's owner
    if (callerNumberClean !== ownerNumberClean && callerNumberClean !== sessionOwner) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ ඔබට මෙය භාවිත කිරීමට අවසර නැත. (Owner හෝ මෙහි session owner විය යුතුයි)' }, { quoted: msg });
      break;
    }

    // determine target JID: reply / mention / arg
    let targetJid = null;
    const ctx = msg.message?.extendedTextMessage?.contextInfo;

    if (ctx?.participant) targetJid = ctx.participant; // replied user
    else if (ctx?.mentionedJid && ctx.mentionedJid.length) targetJid = ctx.mentionedJid[0]; // mentioned
    else if (args && args.length > 0) {
      const possible = args[0].trim();
      if (possible.includes('@')) targetJid = possible;
      else {
        const digits = possible.replace(/[^0-9]/g,'');
        if (digits) targetJid = `${digits}@s.whatsapp.net`;
      }
    }

    if (!targetJid) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❗ කරුණාකර reply කරන හෝ mention කරන හෝ number එක යොදන්න. උදාහරණය: .block 9477xxxxxxx' }, { quoted: msg });
      break;
    }

    // normalize
    if (!targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;
    if (!targetJid.endsWith('@s.whatsapp.net') && !targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;

    // perform block
    try {
      if (typeof socket.updateBlockStatus === 'function') {
        await socket.updateBlockStatus(targetJid, 'block');
      } else {
        // some bailey builds use same method name; try anyway
        await socket.updateBlockStatus(targetJid, 'block');
      }
      try { await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: `✅ @${targetJid.split('@')[0]} blocked successfully.`, mentions: [targetJid] }, { quoted: msg });
    } catch (err) {
      console.error('Block error:', err);
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ Failed to block the user. (Maybe invalid JID or API failure)' }, { quoted: msg });
    }

  } catch (err) {
    console.error('block command general error:', err);
    try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
    await socket.sendMessage(sender, { text: '❌ Error occurred while processing block command.' }, { quoted: msg });
  }
  break;
}

case 'unblock': {
  try {
    // caller number (who sent the command)
    const callerNumberClean = (senderNumber || '').replace(/[^0-9]/g, '');
    const ownerNumberClean = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
    const sessionOwner = (number || '').replace(/[^0-9]/g, '');

    // allow if caller is global owner OR this session's owner
    if (callerNumberClean !== ownerNumberClean && callerNumberClean !== sessionOwner) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ ඔබට මෙය භාවිත කිරීමට අවසර නැත. (Owner හෝ මෙහි session owner විය යුතුයි)' }, { quoted: msg });
      break;
    }

    // determine target JID: reply / mention / arg
    let targetJid = null;
    const ctx = msg.message?.extendedTextMessage?.contextInfo;

    if (ctx?.participant) targetJid = ctx.participant;
    else if (ctx?.mentionedJid && ctx.mentionedJid.length) targetJid = ctx.mentionedJid[0];
    else if (args && args.length > 0) {
      const possible = args[0].trim();
      if (possible.includes('@')) targetJid = possible;
      else {
        const digits = possible.replace(/[^0-9]/g,'');
        if (digits) targetJid = `${digits}@s.whatsapp.net`;
      }
    }

    if (!targetJid) {
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❗ කරුණාකර reply කරන හෝ mention කරන හෝ number එක යොදන්න. උදාහරණය: .unblock 9477xxxxxxx' }, { quoted: msg });
      break;
    }

    // normalize
    if (!targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;
    if (!targetJid.endsWith('@s.whatsapp.net') && !targetJid.includes('@')) targetJid = `${targetJid}@s.whatsapp.net`;

    // perform unblock
    try {
      if (typeof socket.updateBlockStatus === 'function') {
        await socket.updateBlockStatus(targetJid, 'unblock');
      } else {
        await socket.updateBlockStatus(targetJid, 'unblock');
      }
      try { await socket.sendMessage(sender, { react: { text: "✅", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: `🔓 @${targetJid.split('@')[0]} unblocked successfully.`, mentions: [targetJid] }, { quoted: msg });
    } catch (err) {
      console.error('Unblock error:', err);
      try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
      await socket.sendMessage(sender, { text: '❌ Failed to unblock the user.' }, { quoted: msg });
    }

  } catch (err) {
    console.error('unblock command general error:', err);
    try { await socket.sendMessage(sender, { react: { text: "❌", key: msg.key } }); } catch(e){}
    await socket.sendMessage(sender, { text: '❌ Error occurred while processing unblock command.' }, { quoted: msg });
  }
  break;
}

case 'setbotname': {
  const sanitized = (number || '').replace(/[^0-9]/g, '');
  const senderNum = (nowsender || '').split('@')[0];
  const ownerNum = config.OWNER_NUMBER.replace(/[^0-9]/g, '');
  if (senderNum !== sanitized && senderNum !== ownerNum) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME1" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: '❌ Permission denied. Only the session owner or bot owner can change this session bot name.' }, { quoted: shonux });
    break;
  }

  const name = args.join(' ').trim();
  if (!name) {
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME2" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    return await socket.sendMessage(sender, { text: '❗ Provide bot name. Example: `.setbotname Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ Mᴅ- 01`' }, { quoted: shonux });
  }

  try {
    let cfg = await loadUserConfigFromMongo(sanitized) || {};
    cfg.botName = name;
    await setUserConfigInMongo(sanitized, cfg);

    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME3" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };

    await socket.sendMessage(sender, { text: `✅ Bot display name set for this session: ${name}` }, { quoted: shonux });
  } catch (e) {
    console.error('setbotname error', e);
    const shonux = {
      key: { remoteJid: "status@broadcast", participant: "0@s.whatsapp.net", fromMe: false, id: "META_AI_SETBOTNAME4" },
      message: { contactMessage: { displayName: BOT_NAME_FANCY, vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${BOT_NAME_FANCY};;;;\nFN:${BOT_NAME_FANCY}\nORG:Meta Platforms\nTEL;type=CELL;type=VOICE;waid=13135550002:+1 313 555 0002\nEND:VCARD` } }
    };
    await socket.sendMessage(sender, { text: `❌ Failed to set bot name: ${e.message || e}` }, { quoted: shonux });
  }
  break;
}

        // default
        default:
          break;
      }
    } catch (err) {
      console.error('Command handler error:', err);
      try { await socket.sendMessage(sender, { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('❌ ERROR', 'An error occurred while processing your command. Please try again.', BOT_NAME_FANCY) }); } catch(e){}
    }

  });
}
   // --- ANTI-BADWORD FUNCTION (LOGO + FAKE HEADER + REACT + ALL WORDS) ---

async function handleBadWords(socket, msg, body, sender, isGroup, groupAdmins, isOwner, sessionNumber) {
    try {
        // 1. Config Load
        const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
        const userConfig = await loadUserConfigFromMongo(sanitized) || {};
        
        // Check if Enabled
        if (userConfig.ANTI_BADWORD !== 'true') return;

        // 2. Bypass Admins & Owner
        if (!isGroup) return; 
        const senderNum = sender.split('@')[0];
        const isAdmin = groupAdmins.includes(sender);
        if (isAdmin || isOwner) return; 

        // 3. Bot Settings (Logo & Name)
        const botName = userConfig.botName || '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';
        const botLogo = userConfig.logo || "https://www.movanest.xyz/BDhh39.jpg"; // Default Logo URL

        // 4. THE ULTIMATE BAD WORD LIST
        const badWords = [
            // ➤ SINGLISH
            "hutto", "hutta", "huththa", "huththo", "huththi", "hutthi", "huth", 
            "pakaya", "paka", "pako", "pake", "pka", "pkaya", "pak",
            "kariya", "kari", "kariyo", "kary",
            "wesi", "wesige", "wesie", "wesa", "wsige", "wesiy",
            "ponnaya", "ponna", "ponnya", "ponnayo",
            "hukanna", "hukana", "hukapan", "hukanawa", "hkpn", "hukan",
            "bijja", "bijjo", "topa", "thuk", "thoo",
            "balli", "balla", "gona", "gon", "buruwa", "nakiya", "naki",

            // ➤ SINHALA
            "හුත්ත", "හුත්තා", "හුත්තෝ", "හුත්ති", "හු****",
            "පක", "පකයා", "පකෝ", "පකය", "පකේ",
            "වේසි", "වේස", "වේසියේ", "වෙසි",
            "හුකන්න", "හුකනවා", "හුකපන්", "හු***", "හුකහන්",
            "කැරියා", "කැරි",
            "පොන්නයා", "පොන්න", "පොන්නයෝ",
            "බිජ්ජ", "ටොපා", "බල්ලා", "බූරුවා", "ගොනා", "මී හරකා", "තූ",

            // ➤ ENGLISH
            "fuck", "fucker", "fucking", "fck", "fk",
            "bitch", "sex", "porn", "pussy", "dick", "cock",
            "asshole", "bastard", "whore", "slut", "cunt", "motherfucker"
        ];

        const text = body.toLowerCase().replace(/\s+/g, ''); // Remove spaces

        // 5. Detection Logic
        const isBad = badWords.some(word => {
            if (text.includes(word)) return true;
            return body.toLowerCase().includes(word);
        });

        if (isBad) {
            // A. React First (🤬)
            await socket.sendMessage(msg.key.remoteJid, { react: { text: '🛡️', key: msg.key } });

            // B. Delete Message
            await socket.sendMessage(msg.key.remoteJid, { delete: msg.key });

            // C. Prepare Warning Text
            const warnText = `
╭━━━〔 ⚠️ 𝐖𝐀𝐑𝐍𝐈𝐍𝐆 〕━━━┈
┃
┃ 👤 *𝐔𝐬𝐞𝐫 :* @${senderNum}
┃ 🚫 *𝐑𝐞𝐚𝐬𝐨𝐧 :* Bad Word Detected
┃ ⚙️ *𝐀𝐜𝐭𝐢𝐨𝐧 :* Message Deleted
┃
╰━━━━━━━━━━━━━━━━━━┈
> 𝐏𝐥𝐞𝐚𝐬𝐞 𝐮𝐬𝐞 𝐫𝐞𝐬𝐩𝐞𝐜𝐭𝐟𝐮𝐥 𝐥𝐚𝐧𝐠𝐮𝐚𝐠𝐞!
> © ${botName}
`;

            // D. Send Message with Fake Header (AdReply) & Logo
            await socket.sendMessage(msg.key.remoteJid, { 
                text: warnText,
                contextInfo: {
                    mentionedJid: [sender],
                    externalAdReply: {
                        title: "🚫 𝐀𝐍𝐓𝐈-𝐁𝐀𝐃𝐖𝐎𝐑𝐃 𝐒𝐘𝐒𝐓𝐄𝐌",  // Fake Header Title
                        body: botName,                     // Body Text
                        thumbnailUrl: botLogo,             // Bot Logo
                        sourceUrl: "https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00",
                        mediaType: 1,
                        renderLargerThumbnail: true
                    }
                }
            });
            
            console.log(`🚫 Bad word deleted from ${senderNum}`);
        }

    } catch (e) {
        console.error("Anti-Badword Error:", e);
    }
}
// ---------------- Call Rejection Handler ----------------

// ---------------- Simple Call Rejection Handler ----------------

async function setupCallRejection(socket, sessionNumber) {
    socket.ev.on('call', async (calls) => {
        try {
            // Load user-specific config from MongoDB
            const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
            const userConfig = await loadUserConfigFromMongo(sanitized) || {};
            if (userConfig.ANTI_CALL !== 'on') return;

            console.log(`📞 Incoming call detected for ${sanitized} - Auto rejecting...`);

            for (const call of calls) {
                if (call.status !== 'offer') continue;

                const id = call.id;
                const from = call.from;

                // Reject the call
                await socket.rejectCall(id, from);
                
                // Send rejection message to caller
                await socket.sendMessage(from, {
                    text: '*🔕 Auto call rejection is enabled. Calls are automatically rejected.*'
                });
                
                console.log(`✅ Auto-rejected call from ${from}`);

                // Send notification to bot user
                const userJid = jidNormalizedUser(socket.user.id);
                const rejectionMessage = formatMessage(
                    '📞 CALL REJECTED',
                    `Auto call rejection is active.\n\nCall from: ${from}\nTime: ${getSriLankaTimestamp()}`,
                    BOT_NAME_FANCY
                );

                await socket.sendMessage(userJid, { 
                    image: { url: config.RCD_IMAGE_PATH }, 
                    caption: rejectionMessage 
                });
            }
        } catch (err) {
            console.error(`Call rejection error for ${sessionNumber}:`, err);
        }
    });
}

// ---------------- Auto Message Read Handler ----------------

async function setupAutoMessageRead(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg || !msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    // Quick return if no need to process
    const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
    const userConfig = await loadUserConfigFromMongo(sanitized) || {};
    const autoReadSetting = userConfig.AUTO_READ_MESSAGE || 'off';

    if (autoReadSetting === 'off') return;

    const from = msg.key.remoteJid;
    
    // Simple message body extraction
    let body = '';
    try {
      const type = getContentType(msg.message);
      const actualMsg = (type === 'ephemeralMessage') 
        ? msg.message.ephemeralMessage.message 
        : msg.message;

      if (type === 'conversation') {
        body = actualMsg.conversation || '';
      } else if (type === 'extendedTextMessage') {
        body = actualMsg.extendedTextMessage?.text || '';
      } else if (type === 'imageMessage') {
        body = actualMsg.imageMessage?.caption || '';
      } else if (type === 'videoMessage') {
        body = actualMsg.videoMessage?.caption || '';
      }
    } catch (e) {
      // If we can't extract body, treat as non-command
      body = '';
    }

    // Check if it's a command message
    const prefix = userConfig.PREFIX || config.PREFIX;
    const isCmd = body && body.startsWith && body.startsWith(prefix);

    // Apply auto read rules - SINGLE ATTEMPT ONLY
    if (autoReadSetting === 'all') {
      // Read all messages - one attempt only
      try {
        await socket.readMessages([msg.key]);
        console.log(`✅ Message read: ${msg.key.id}`);
      } catch (error) {
        console.warn('Failed to read message (single attempt):', error?.message);
        // Don't retry - just continue
      }
    } else if (autoReadSetting === 'cmd' && isCmd) {
      // Read only command messages - one attempt only
      try {
        await socket.readMessages([msg.key]);
        console.log(`✅ Command message read: ${msg.key.id}`);
      } catch (error) {
        console.warn('Failed to read command message (single attempt):', error?.message);
        // Don't retry - just continue
      }
    }
  });
}

// ---------------- message handlers ----------------
function setupMessageHandlers(socket, sessionNumber) {
  socket.ev.on('messages.upsert', async ({ messages }) => {
    const msg = messages[0];
    if (!msg.message || msg.key.remoteJid === 'status@broadcast' || msg.key.remoteJid === config.NEWSLETTER_JID) return;

    try {
      // 1. Settings ලබා ගැනීම (Default values from config)
      let autoTyping = config.AUTO_TYPING;
      let autoRecording = config.AUTO_RECORDING;
      let autoReply = config.AUTO_REPLY;  // Text සඳහා
      let autoVoice = config.AUTO_VOICE;  // Voice සඳහා

      const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
      const userConfig = await loadUserConfigFromMongo(sanitized) || {};

      // MongoDB එකේ settings තියෙනවා නම් ඒවා ගමු
      if (userConfig.AUTO_TYPING !== undefined) autoTyping = userConfig.AUTO_TYPING;
      if (userConfig.AUTO_RECORDING !== undefined) autoRecording = userConfig.AUTO_RECORDING;
      if (userConfig.AUTO_REPLY !== undefined) autoReply = userConfig.AUTO_REPLY;
      if (userConfig.AUTO_VOICE !== undefined) autoVoice = userConfig.AUTO_VOICE;

      const messageContent = msg.message.conversation || msg.message.extendedTextMessage?.text || "";
      const prefix = ".";
      const isCmd = messageContent.startsWith(prefix);

      // --- 2. Auto Reply Logic ---
      // Command එකක් නෙවෙයි නම් පමණක් ඇතුළට යයි
      if (!isCmd) {
        
              const combinedResponses = {
        "Hi": { text: "*~අනේහ් ඉතිම් සූටි පැටියෝහ් හායි💗⃝🙈⃝🌼~*", audio: "https://www.movanest.xyz/zorc20.mp3" },
        "හායි": { text: "*~අනේහ් ඉතිම් සූටි පැටියෝහ් හායි💗⃝🙈⃝🌼~*", audio: "https://www.movanest.xyz/zorc20.mp3" },
        "hi": { text: "*~අනේහ් ඉතිම් සූටි පැටියෝහ් හායි💗⃝🙈⃝🌼~*", audio: "https://www.movanest.xyz/zorc20.mp3" },            
       "Mk": { text: "*මොනා කරන්නද අනේ ඔහේ ඉන්නවා🧏‍♂️*\n\n`මොකද කරන්නෙ ඔයා💗🙈`", audio: "https://www.movanest.xyz/j7ZxI8.mp3" },
        "mk": { text: "*මොනා කරන්නද අනේ ඔහේ ඉන්නවා🧏‍♂️*\n\n`මොකද කරන්නෙ ඔයා💗🙈`", audio: "https://www.movanest.xyz/j7ZxI8.mp3" },
        "මොකද කරන්නෙ": { text: "*මොනා කරන්නද අනේ ඔහේ ඉන්නවා🧏‍♂️*", audio: "https://www.movanest.xyz/j7ZxI8.mp3" },        
        "Mn": { text: "*මොකුත්ම නැද්ද ඉතිම් ආ🥹🙈🌼*", audio: "https://www.movanest.xyz/UjqTiG.mp3" },
        "මුකුත් නැ": { text: "*මොකුත්ම නැද්ද ඉතිම් ආ🥹🙈🌼*", audio: "https://www.movanest.xyz/UjqTiG.mp3" },
        "Hmm": { text: "*මොකද බස්සෙක් වෙන්න ඕනි වෙලාද හලූ ඔයාත 🫣💗*", audio: null },        
        "Gn": { text: "🧘⃞🖤̸྄̅̄̂ɢᷓ͜͡ᴏᴏ҉ᴅ ͜͡ ̶ɴⷬɪᷢɢ̸ⷶ͢ʜᷤᴛⷶ͜͡🍃⃦⃝🧘", audio: null },
        "Good Night": { text: "🧘⃞🖤̸྄̅̄̂ɢᷓ͜͡ᴏᴏ҉ᴅ ͜͡ ̶ɴⷬɪᷢɢ̸ⷶ͢ʜᷤᴛⷶ͜͡🍃⃦⃝🧘", audio: null },
        "ගුඩ් නයිට්": { text: "🧘⃞🖤̸྄̅̄̂ɢᷓ͜͡ᴏᴏ҉ᴅ ͜͡ ̶ɴⷬɪᷢɢ̸ⷶ͢ʜᷤᴛⷶ͜͡🍃⃦⃝🧘", audio: null },        
       "Gm": { text: "🍀⃝ගුට් මෝනිම් සූටි ලමයෝහ්🌼⃝⃕🥇̶⃮⃖😻̶᭄", audio: "https://www.movanest.xyz/8sPSzZ.mp3" },
       "Good Morning": { text: "🍀⃝⃕ගුට්මෝනිම් ලමයෝහ්🥇̶⃮⃖😻̶᭄", audio: "https://www.movanest.xyz/8sPSzZ.mp3" },
       "ගුඩ් මොනිම්": { text: "🍀⃝⃕ගුට්මෝනිම් ලමයෝහ්🥇̶⃮⃖😻̶᭄", audio: "https://www.movanest.xyz/8sPSzZ.mp3" },        
       "Bye": { text: "*හා හා යන්නකු මම ආයෙ ඔය ළමයා එක්ක කතා කලන්නෙ නෑ නෑ නෑ නෑ නෑමයි☹️*\n\n\nටිකක් වෙලා ඉන්නවකු අනේ😫💗", audio: "https://www.movanest.xyz/foouPC.mp3" },
       "බායි": { text: "*හා හා යන්නකු මම ආයෙ ඔය ළමයා එක්ක කතා කලන්නෙ නෑ නෑ නෑ නෑ නෑමයි☹️*\n\n\nටිකක් වෙලා ඉන්නවකු අනේ😫💗", audio: "https://www.movanest.xyz/foouPC.mp3" },    
       "Ai": { text: "*ඇයි උබ ඇයි ඇයි ගාන්නේ මෝඩයා*", audio: "https://www.movanest.xyz/0tjn36.mp3" },
       "ඇයි": { text: "*ඇයි උබ ඇයි ඇයි ගාන්නේ මෝඩයා*", audio: "https://www.movanest.xyz/0tjn36.mp3" },        
       "sududa": { text: "*අනේ මෝඩයෝ මම ලන්කාවේ ඒකනිසා මගේ කොහොමද පු# සුදුවෙන්නේ කියපන්කෝ*", audio: "https://www.movanest.xyz/uSle1o.mp3" },
       "pi": { text: "*හ්ම්ම්ම්🥺 ඔයත් පරිස්සමින් ඉන්න හොදද හොද ළමයා වගේ💗🥰🌼*", audio: "https://www.movanest.xyz/yHObUf.mp3" },
       "parissemin": { text: "*හ්ම්ම්ම්🥺 ඔයත් පරිස්සමින් ඉන්න හොදද හොද ළමයා වගේ💗🥰🌼*", audio: "https://www.movanest.xyz/yHObUf.mp3" },
       "පරිස්සෙමින්": { text: "*හ්ම්ම්ම්🥺 ඔයත් පරිස්සමින් ඉන්න හොදද හොද ළමයා වගේ💗🥰🌼*", audio: "https://www.movanest.xyz/yHObUf.mp3" },        
       "ewpn": { text: "*උබට ඔන්නම් ගනිම්😹උබ ඉල්ලපු ගමන් දෙන්න මම උබේ කව්ද🤣*", audio: "https://www.movanest.xyz/CmqSyq.mp3" },        
       "ewannako": { text: "මට බැ 😹!ඇවිත් අරගෙන යන්නකො අනෙ😫", audio: "https://www.movanest.xyz/5hr5lx.mp3" },
       "එවන්නකො": { text: "මට බැ 😹!ඇවිත් අරගෙන යන්නකො අනේ🙈", audio: "https://www.movanest.xyz/5hr5lx.mp3" },        
       "😂": { text: "මොකු අනෙ හිනා වෙන්නෙ ඔයත ලෙඩක්ද මැට්ටෝ🙈*", audio: "https://www.movanest.xyz/6ecDhh.mp3" },
       "😹": { text: "මොකු අනෙ හිනා වෙන්නෙ ඔයත ලෙඩක්ද මැට්ටෝ🙈*", audio: "https://www.movanest.xyz/6ecDhh.mp3" },
       "🤣": { text: "මොකු අනෙ හිනා වෙන්නෙ ඔයත ලෙඩක්ද මැට්ටෝ🙈*", audio: "https://www.movanest.xyz/6ecDhh.mp3" },
       "😅": { text: "මොකු අනෙ හිනා වෙන්නෙ ඔයත ලෙඩක්ද මැට්ටෝ🙈*", audio: "https://www.movanest.xyz/6ecDhh.mp3" },    
        "👍": { text: "`එලම` *පිට් තමා ඈ*", audio: null }, 
        "කෑවද බන්": { text: "ඔව් අනේ සූට්ටක් කෑවා අනීහ් 😫❤️මම තම සුටී බබෙක් නෙ අනේ🙈", audio: "https://www.movanest.xyz/wx8Aht.mp3" },     
        "kawada": { text: "ඔව් අනේ සූට්ටක් කෑවා අනීහ් 😫❤️මම තම සුටී බබෙක් නේ🙈", audio: "https://www.movanest.xyz/wx8Aht.mp3" },        
        "I Love You": { text: "*මට ඔව කිව්වට වැඩක් නැ අනේ😹*", audio: "" },        
        "adarei": { text: "*මට ඔව කිව්වට වැඩක් නැ අනේ😹*", audio: "" },        
        "සුදූ": { text: "සුදූ සුදූ කිය කිය ඉන්නෙ නැතුව කියන්න අනේ🙈ඇයි දැන් ම්ම්ම්🥺💗", audio: null },        
        "sudu": { text: "සුදූ සුදූ කිය කිය ඉන්නෙ නැතුව කියන්න අනේ🙈ඇයි දැන් ම්ම්ම්🥺💗", audio: null },        
        "🙈": { text: null, audio: null },
        "😒": { text: null, audio: null },
        "Ane me": { text: null, audio: null },
        "අනේ මේ": { text: null, audio: null },
        "🥵": { text: null, audio: null },
        "😡": { text: "අනෙ අනෙ චුටි පැටියෝ☺️කෙන්ති ගියද අනෙ ඔයාට", audio: "" },
      };

        const response = combinedResponses[messageContent];

        if (response) {
          if (response.text && autoReply === 'true') {
            await socket.sendMessage(msg.key.remoteJid, { text: response.text }, { quoted: msg });
          }
          if (response.audio && autoVoice === 'true') {
            await socket.sendMessage(msg.key.remoteJid, {
              audio: { url: response.audio },
              mimetype: 'audio/mp4',
              ptt: true
            }, { quoted: msg });
          }
        }
      }

      // --- 3. Presence Updates (Typing/Recording) ---
      if (autoTyping === 'true') {
        await socket.sendPresenceUpdate('composing', msg.key.remoteJid);
        setTimeout(async () => {
          try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) {}
        }, 3000);
      }

      if (autoRecording === 'true') {
        await socket.sendPresenceUpdate('recording', msg.key.remoteJid);
        setTimeout(async () => {
          try { await socket.sendPresenceUpdate('paused', msg.key.remoteJid); } catch (e) {}
        }, 3000);
      }

    } catch (error) {
      console.error('Message handler error:', error);
    }
  });
}
// ---------------- cleanup helper ----------------

async function deleteSessionAndCleanup(number, socketInstance) {
  const sanitized = number.replace(/[^0-9]/g, '');
  try {
    const sessionPath = path.join(os.tmpdir(), `session_${sanitized}`);
    try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
    activeSockets.delete(sanitized); socketCreationTime.delete(sanitized);
    try { await removeSessionFromMongo(sanitized); } catch(e){}
    try { await removeNumberFromMongo(sanitized); } catch(e){}
    try {
      const ownerJid = `${config.OWNER_NUMBER.replace(/[^0-9]/g,'')}@s.whatsapp.net`;
      const caption = formatMessage('*🥷 OWNER NOTICE — SESSION REMOVED*', `*𝐍umber:* ${sanitized}\n*𝐒ession 𝐑emoved 𝐃ue 𝐓o 𝐋ogout.*\n\n*𝐀ctive 𝐒essions 𝐍ow:* ${activeSockets.size}`, BOT_NAME_FANCY);
      if (socketInstance && socketInstance.sendMessage) await socketInstance.sendMessage(ownerJid, { image: { url: config.RCD_IMAGE_PATH }, caption });
    } catch(e){}
    console.log(`Cleanup completed for ${sanitized}`);
  } catch (err) { console.error('deleteSessionAndCleanup error:', err); }
}

// ---------------- auto-restart ----------------

function setupAutoRestart(socket, number) {
  socket.ev.on('connection.update', async (update) => {
    const { connection, lastDisconnect } = update;
    if (connection === 'close') {
      const statusCode = lastDisconnect?.error?.output?.statusCode
                         || lastDisconnect?.error?.statusCode
                         || (lastDisconnect?.error && lastDisconnect.error.toString().includes('401') ? 401 : undefined);
      const isLoggedOut = statusCode === 401
                          || (lastDisconnect?.error && lastDisconnect.error.code === 'AUTHENTICATION')
                          || (lastDisconnect?.error && String(lastDisconnect.error).toLowerCase().includes('logged out'))
                          || (lastDisconnect?.reason === DisconnectReason?.loggedOut);
      if (isLoggedOut) {
        console.log(`User ${number} logged out. Cleaning up...`);
        try { await deleteSessionAndCleanup(number, socket); } catch(e){ console.error(e); }
      } else {
        console.log(`Connection closed for ${number} (not logout). Attempt reconnect...`);
        try { await delay(10000); activeSockets.delete(number.replace(/[^0-9]/g,'')); socketCreationTime.delete(number.replace(/[^0-9]/g,'')); const mockRes = { headersSent:false, send:() => {}, status: () => mockRes }; await EmpirePair(number, mockRes); } catch(e){ console.error('Reconnect attempt failed', e); }
      }

    }

  });
}

//---------------- GROUP WELCOME & BYE HANDLER (NO DESCRIPTION - FANCY V2) ----------------
async function setupGroupEvents(socket, sessionNumber) {
    socket.ev.on('group-participants.update', async (update) => {
        try {
            const { id, participants, action } = update;
            
            // 1. Config & Check ON/OFF
            const sanitized = (sessionNumber || '').replace(/[^0-9]/g, '');
            const userConfig = await loadUserConfigFromMongo(sanitized) || {};

            // ⚠️ Welcome OFF නම් නවතින්න
            if (userConfig.WELCOME_MSG !== 'true') return;

            const botName = userConfig.botName || '𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3';

            // 2. Fancy Fake Header (Contact Card)
            const fakeContact = {
                key: {
                    remoteJid: "status@broadcast",
                    participant: "0@s.whatsapp.net",
                    fromMe: false,
                    id: "QUEEN_IMALSHA_EVENT"
                },
                message: {
                    contactMessage: {
                        displayName: botName,
                        vcard: `BEGIN:VCARD\nVERSION:3.0\nN:${botName};;;;\nFN:${botName}\nORG:Queen Seya Kingdom\nTEL;type=CELL;type=VOICE;waid=94700000000:+94 70 000 0000\nEND:VCARD`
                    }
                }
            };

            // 3. Get Group Metadata
            let groupMetadata;
            try {
                groupMetadata = await socket.groupMetadata(id);
            } catch (e) {
                return; 
            }

            const groupName = groupMetadata.subject;
            const memberCount = groupMetadata.participants.length;

            // 4. Loop Participants
            for (const participant of participants) {
                let ppUrl;
                try {
                    ppUrl = await socket.profilePictureUrl(participant, 'image');
                } catch {
                    ppUrl = 'https://telegra.ph/file/24fa902ead26340f3df2c.png'; 
                }

                const date = new Date().toLocaleDateString("si-LK");
                const time = new Date().toLocaleTimeString("si-LK");
                const userName = participant.split('@')[0];

                // ✅ WELCOME MESSAGE (NO DESCRIPTION)
                if (action === 'add') {
                    const welcomeCaption = `
╭▭▬〔 💗 𝐖𝐄𝐋𝐂𝐎𝐌𝐄 🙈〕▭▬┈⊷
┃
👤 *User:* @${userName}
👥 *Group:* ${groupName}
📅 *Date:* ${date}
⌚ *Time:* ${time}
┃
📍 *හායි හායි ලස්සන ළමයෝ....!*
📍 *ᴡᴇʟᴄᴏᴍᴇ ᴛᴏ ɢʀᴏᴜᴘ...!*
┃
╰▭▬▭▬▭▬▭▬▭▬▭▬┈⊷
> *ᴘᴏᴡᴇʀᴅ ʙʏ ${botName}* 👑`;

                    await socket.sendMessage(id, {
                        image: { url: ppUrl },
                        caption: welcomeCaption,
                        mentions: [participant],
                        contextInfo: {
                            mentionedJid: [participant],
                            externalAdReply: {
                                title: `ɴᴇᴡ ᴍᴇᴍʙᴀʀ ɢʀᴏᴜᴘ ᴊᴏɪɴ💗`,
                                body: groupName,
                                thumbnailUrl: ppUrl,
                                sourceUrl: config.CHANNEL_LINK || 'https://whatsapp.com/channel/0029VbB3MA53mFYFjSOhzn00',
                                mediaType: 1,
                                renderLargerThumbnail: true
                            }
                        }
                    }, { quoted: fakeContact });

                    await socket.sendMessage(id, { react: { text: "📍", key: { remoteJid: id, fromMe: true } } });
                } 
                
                // ❌ GOODBYE MESSAGE (NO DESCRIPTION)
                else if (action === 'remove') {
                    const byeCaption = `
╭▭▬〔 ɢᴏᴏᴅʙʏᴇ 👋〕▭▬┈⊷
┃
👤 *User:* @${userName}
👥 *Group:* ${groupName}
📅 *Date:* ${date}
⌚ *Time:* ${time}
┃
📍 *පරිස්සෙමින් පැටියෝ🙈💗*
📍 *ɢᴏᴏᴅ ʙʏᴇ ꜱᴜᴅᴜ👋*
┃
╰▭▬▭▬▭▬▭▬▭▬▭▬┈⊷
> *ᴘᴏᴡᴇʀᴅ ʙʏ ${botName}* 👑`;

                    await socket.sendMessage(id, {
                        image: { url: ppUrl },
                        caption: byeCaption,
                        mentions: [participant],
                        contextInfo: {
                            mentionedJid: [participant],
                            externalAdReply: {
                                title: `📍ᴍᴇᴍʙᴀʀ.ɢʀᴏᴜᴘ ʟᴇꜰᴛ👋`,
                                body: `ᴡᴇ ᴍɪꜱꜱ ʏᴏᴜ !`,
                                thumbnailUrl: ppUrl,
                                sourceUrl: config.CHANNEL_LINK || 'https://whatsapp.com/channel/0029VbBgQLO2ZjClTBWN3s1C',
                                mediaType: 1,
                                renderLargerThumbnail: true
                            }
                        }
                    }, { quoted: fakeContact });

                    await socket.sendMessage(id, { react: { text: "📍", key: { remoteJid: id, fromMe: true } } });
                }
            }
        } catch (err) {
            console.error('Group Welcome/Bye Error:', err);
        }
    });
}
// ---------------- EmpirePair (pairing, temp dir, persist to Mongo) ----------------

async function EmpirePair(number, res) {
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const sessionPath = path.join(os.tmpdir(), `session_${sanitizedNumber}`);
  await initMongo().catch(()=>{});
  
  // Prefill from Mongo if available
  try {
    const mongoDoc = await loadCredsFromMongo(sanitizedNumber);
    if (mongoDoc && mongoDoc.creds) {
      fs.ensureDirSync(sessionPath);
      fs.writeFileSync(path.join(sessionPath, 'creds.json'), JSON.stringify(mongoDoc.creds, null, 2));
      if (mongoDoc.keys) fs.writeFileSync(path.join(sessionPath, 'keys.json'), JSON.stringify(mongoDoc.keys, null, 2));
      console.log('Prefilled creds from Mongo');
    }
  } catch (e) { console.warn('Prefill from Mongo failed', e); }

  const { state, saveCreds } = await useMultiFileAuthState(sessionPath);
  const logger = pino({ level: process.env.NODE_ENV === 'production' ? 'fatal' : 'debug' });

  try {
        const socket = makeWASocket({
        logger: pino({ level: "silent" }),
        printQRInTerminal: false,
        auth: state,
        version: [2, 3000, 1033105955],
        connectTimeoutMs: 60000,
        defaultQueryTimeoutMs: 0,
        keepAliveIntervalMs: 10000,
        emitOwnEvents: true,
        fireInitQueries: true,
        generateHighQualityLinkPreview: true,
        syncFullHistory: true,
        markOnlineOnConnect: true,
        browser: ['Mac OS', 'Safari', '10.15.7'] });

        socketCreationTime.set(sanitizedNumber, Date.now());
        
    setupStatusHandlers(socket, sanitizedNumber);
    setupCommandHandlers(socket, sanitizedNumber);
    setupMessageHandlers(socket, sanitizedNumber);
    setupAutoRestart(socket, sanitizedNumber);
    setupNewsletterHandlers(socket, sanitizedNumber);
    handleMessageRevocation(socket, sanitizedNumber);
    setupAutoMessageRead(socket, sanitizedNumber);
    setupCallRejection(socket, sanitizedNumber);

    if (!socket.authState.creds.registered) {
      let retries = config.MAX_RETRIES;
      let code;
      let sanu=`queenv30`;
      while (retries > 0) {
        try { await delay(1500); code = await socket.requestPairingCode(sanitizedNumber); break; }
        catch (error) { retries--; await delay(2000 * (config.MAX_RETRIES - retries)); }
      }
      if (!res.headersSent) res.send({ code });
    }

    // Save creds to Mongo when updated
    socket.ev.on('creds.update', async () => {
      try {
        await saveCreds();
        
        const credsPath = path.join(sessionPath, 'creds.json');
        
        if (!fs.existsSync(credsPath)) return;
        const fileStats = fs.statSync(credsPath);
        if (fileStats.size === 0) return;
        
        const fileContent = await fs.readFile(credsPath, 'utf8');
        const trimmedContent = fileContent.trim();
        if (!trimmedContent || trimmedContent === '{}' || trimmedContent === 'null') return;
        
        let credsObj;
        try { credsObj = JSON.parse(trimmedContent); } catch (e) { return; }
        
        if (!credsObj || typeof credsObj !== 'object') return;
        
        const keysObj = state.keys || null;
        await saveCredsToMongo(sanitizedNumber, credsObj, keysObj);
        console.log('✅ Creds saved to MongoDB successfully');
        
      } catch (err) { 
        console.error('Failed saving creds on creds.update:', err);
      }
    });

    socket.ev.on('connection.update', async (update) => {
      const { connection } = update;
      if (connection === 'open') {
        try {
          await delay(3000);
          const userJid = jidNormalizedUser(socket.user.id);
          const groupResult = await joinGroup(socket).catch(()=>({ status: 'failed', error: 'joinGroup not configured' }));

          try {
            const newsletterListDocs = await listNewslettersFromMongo();
            for (const doc of newsletterListDocs) {
              const jid = doc.jid;
              try { if (typeof socket.newsletterFollow === 'function') await socket.newsletterFollow(jid); } catch(e){}
            }
          } catch(e){}

          activeSockets.set(sanitizedNumber, socket);
          const groupStatus = groupResult.status === 'success' ? 'Joined successfully' : `Failed to join group: ${groupResult.error}`;

          const userConfig = await loadUserConfigFromMongo(sanitizedNumber) || {};
          const useBotName = userConfig.botName || BOT_NAME_FANCY;
          const useLogo = userConfig.logo || config.RCD_IMAGE_PATH;

          const initialCaption = formatMessage(useBotName,
            `*✅ 𝐒uccessfully 𝐂onnected*\n\n*🔢 𝐍umber:* ${sanitizedNumber}\n*🕒 𝐂onnecting: Bot will become active in a few seconds*`,
            useBotName
          );

          let sentMsg = null;
          try {
            if (String(useLogo).startsWith('http')) {
              sentMsg = await socket.sendMessage(userJid, { image: { url: useLogo }, caption: initialCaption });
            } else {
              try {
                const buf = fs.readFileSync(useLogo);
                sentMsg = await socket.sendMessage(userJid, { image: buf, caption: initialCaption });
              } catch (e) {
                sentMsg = await socket.sendMessage(userJid, { image: { url: config.RCD_IMAGE_PATH }, caption: initialCaption });
              }
            }
          } catch (e) {
            try { sentMsg = await socket.sendMessage(userJid, { text: initialCaption }); } catch(e){}
          }

          await delay(4000);
            const updatedCaption = formatMessage(useBotName,
`𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3 ꜱᴜᴄᴄᴇꜱꜱꜰᴜʟʟʏ🙊👻
*©:අලුත් ම අලුත් UPDATE ගොඩකින් 𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3 ඔන්න අවා😫*\n*• \`ᴠᴇʀꜱɪᴏɴ\` : ᴠ3.0.0*\n*• \`ʙᴏᴛ ᴄᴏɴɴᴇᴄᴛ ɴʙ\` : ${number}*\n*• \`ᴘᴏᴡᴇʀᴇᴅ ʙʏ\` : ꜱᴀɴᴜ x│ᴏʟᴅ ꜱɪᴛʜᴜᴡᴀ*\n\n*•𝐐ᴜᴇᴇɴ 𝐈ᴍᴀʟꜱʜᴀ 𝐌ᴅ 𝐌ɪɴɪ📡ᴠ3 වෙතට සදරයෙන් පිලිගන්නවා......💗👻*\n\n*🌐ᴠ3.0.0💗ᴡᴇʙ ꜱɪᴛᴇ :*\n> https://queen-imalsha-md-official-site.netlify.app/`,
                            '© ᴄʀᴇᴀᴛᴇᴅ ʙʏ ꜱᴀɴᴜ xᴅ│ꜱɪᴛʜᴜᴡᴀ xᴅ',
            useBotName
          );
          
          try {
            if (sentMsg && sentMsg.key) {
              try { await socket.sendMessage(userJid, { delete: sentMsg.key }); } catch (delErr) {}
            }
            try {
              if (String(useLogo).startsWith('http')) {
                await socket.sendMessage(userJid, { image: { url: useLogo }, caption: updatedCaption });
              } else {
                try {
                  const buf = fs.readFileSync(useLogo);
                  await socket.sendMessage(userJid, { image: buf, caption: updatedCaption });
                } catch (e) {
                  await socket.sendMessage(userJid, { text: updatedCaption });
                }
              }
            } catch (imgErr) {
              await socket.sendMessage(userJid, { text: updatedCaption });
            }
          } catch (e) {}


          await addNumberToMongo(sanitizedNumber);

        } catch (e) { 
          console.error('Connection open error:', e); 
          try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'}`); } catch(e) {}
        }
      }
      if (connection === 'close') {
        try { if (fs.existsSync(sessionPath)) fs.removeSync(sessionPath); } catch(e){}
      }
    });

    activeSockets.set(sanitizedNumber, socket);

  } catch (error) {
    console.error('Pairing error:', error);
    socketCreationTime.delete(sanitizedNumber);
    if (!res.headersSent) res.status(503).send({ error: 'Service Unavailable' });
  }
}


// ---------------- endpoints (admin/newsletter management + others) ----------------

router.post('/newsletter/add', async (req, res) => {
  const { jid, emojis } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  if (!jid.endsWith('@newsletter')) return res.status(400).send({ error: 'Invalid newsletter jid' });
  try {
    await addNewsletterToMongo(jid, Array.isArray(emojis) ? emojis : []);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.post('/newsletter/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeNewsletterFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.get('/newsletter/list', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.status(200).send({ status: 'ok', channels: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


// admin endpoints

router.post('/admin/add', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await addAdminToMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.post('/admin/remove', async (req, res) => {
  const { jid } = req.body;
  if (!jid) return res.status(400).send({ error: 'jid required' });
  try {
    await removeAdminFromMongo(jid);
    res.status(200).send({ status: 'ok', jid });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


router.get('/admin/list', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.status(200).send({ status: 'ok', admins: list });
  } catch (e) { res.status(500).send({ error: e.message || e }); }
});


// existing endpoints (connect, reconnect, active, etc.)

router.get('/', async (req, res) => {
  const { number } = req.query;
  if (!number) return res.status(400).send({ error: 'Number parameter is required' });
  if (activeSockets.has(number.replace(/[^0-9]/g, ''))) return res.status(200).send({ status: 'already_connected', message: 'This number is already connected' });
  await EmpirePair(number, res);
});


router.get('/active', (req, res) => {
  res.status(200).send({ botName: BOT_NAME_FANCY, count: activeSockets.size, numbers: Array.from(activeSockets.keys()), timestamp: getSriLankaTimestamp() });
});


router.get('/ping', (req, res) => {
  res.status(200).send({ status: 'active', botName: BOT_NAME_FANCY, message: 'Qᴜᴇᴇɴ ɪᴍᴀʟꜱʜᴀ ᴍD ᴠ2', activesession: activeSockets.size });
});

router.get('/connect-all', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No numbers found to connect' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      await EmpirePair(number, mockRes);
      results.push({ number, status: 'connection_initiated' });
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Connect all error:', error); res.status(500).send({ error: 'Failed to connect all bots' }); }
});


router.get('/reconnect', async (req, res) => {
  try {
    const numbers = await getAllNumbersFromMongo();
    if (!numbers || numbers.length === 0) return res.status(404).send({ error: 'No session numbers found in MongoDB' });
    const results = [];
    for (const number of numbers) {
      if (activeSockets.has(number)) { results.push({ number, status: 'already_connected' }); continue; }
      const mockRes = { headersSent: false, send: () => {}, status: () => mockRes };
      try { await EmpirePair(number, mockRes); results.push({ number, status: 'connection_initiated' }); } catch (err) { results.push({ number, status: 'failed', error: err.message }); }
      await delay(1000);
    }
    res.status(200).send({ status: 'success', connections: results });
  } catch (error) { console.error('Reconnect error:', error); res.status(500).send({ error: 'Failed to reconnect bots' }); }
});


router.get('/update-config', async (req, res) => {
  const { number, config: configString } = req.query;
  if (!number || !configString) return res.status(400).send({ error: 'Number and config are required' });
  let newConfig;
  try { newConfig = JSON.parse(configString); } catch (error) { return res.status(400).send({ error: 'Invalid config format' }); }
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const otp = generateOTP();
  otpStore.set(sanitizedNumber, { otp, expiry: Date.now() + config.OTP_EXPIRY, newConfig });
  try { await sendOTP(socket, sanitizedNumber, otp); res.status(200).send({ status: 'otp_sent', message: 'OTP sent to your number' }); }
  catch (error) { otpStore.delete(sanitizedNumber); res.status(500).send({ error: 'Failed to send OTP' }); }
});


router.get('/verify-otp', async (req, res) => {
  const { number, otp } = req.query;
  if (!number || !otp) return res.status(400).send({ error: 'Number and OTP are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const storedData = otpStore.get(sanitizedNumber);
  if (!storedData) return res.status(400).send({ error: 'No OTP request found for this number' });
  if (Date.now() >= storedData.expiry) { otpStore.delete(sanitizedNumber); return res.status(400).send({ error: 'OTP has expired' }); }
  if (storedData.otp !== otp) return res.status(400).send({ error: 'Invalid OTP' });
  try {
    await setUserConfigInMongo(sanitizedNumber, storedData.newConfig);
    otpStore.delete(sanitizedNumber);
    const sock = activeSockets.get(sanitizedNumber);
    if (sock) await sock.sendMessage(jidNormalizedUser(sock.user.id), { image: { url: config.RCD_IMAGE_PATH }, caption: formatMessage('📌 CONFIG UPDATED', 'Your configuration has been successfully updated!', BOT_NAME_FANCY) });
    res.status(200).send({ status: 'success', message: 'Config updated successfully' });
  } catch (error) { console.error('Failed to update config:', error); res.status(500).send({ error: 'Failed to update config' }); }
});


router.get('/getabout', async (req, res) => {
  const { number, target } = req.query;
  if (!number || !target) return res.status(400).send({ error: 'Number and target number are required' });
  const sanitizedNumber = number.replace(/[^0-9]/g, '');
  const socket = activeSockets.get(sanitizedNumber);
  if (!socket) return res.status(404).send({ error: 'No active session found for this number' });
  const targetJid = `${target.replace(/[^0-9]/g, '')}@s.whatsapp.net`;
  try {
    const statusData = await socket.fetchStatus(targetJid);
    const aboutStatus = statusData.status || 'No status available';
    const setAt = statusData.setAt ? moment(statusData.setAt).tz('Asia/Colombo').format('YYYY-MM-DD HH:mm:ss') : 'Unknown';
    res.status(200).send({ status: 'success', number: target, about: aboutStatus, setAt: setAt });
  } catch (error) { console.error(`Failed to fetch status for ${target}:`, error); res.status(500).send({ status: 'error', message: `Failed to fetch About status for ${target}.` }); }
});


// ---------------- Dashboard endpoints & static ----------------

const dashboardStaticDir = path.join(__dirname, 'dashboard_static');
if (!fs.existsSync(dashboardStaticDir)) fs.ensureDirSync(dashboardStaticDir);
router.use('/dashboard/static', express.static(dashboardStaticDir));
router.get('/dashboard', async (req, res) => {
  res.sendFile(path.join(dashboardStaticDir, 'index.html'));
});


// API: sessions & active & delete

router.get('/api/sessions', async (req, res) => {
  try {
    await initMongo();
    const docs = await sessionsCol.find({}, { projection: { number: 1, updatedAt: 1 } }).sort({ updatedAt: -1 }).toArray();
    res.json({ ok: true, sessions: docs });
  } catch (err) {
    console.error('API /api/sessions error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.get('/api/active', async (req, res) => {
  try {
    const keys = Array.from(activeSockets.keys());
    res.json({ ok: true, active: keys, count: keys.length });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.post('/api/session/delete', async (req, res) => {
  try {
    const { number } = req.body;
    if (!number) return res.status(400).json({ ok: false, error: 'number required' });
    const sanitized = ('' + number).replace(/[^0-9]/g, '');
    const running = activeSockets.get(sanitized);
    if (running) {
      try { if (typeof running.logout === 'function') await running.logout().catch(()=>{}); } catch(e){}
      try { running.ws?.close(); } catch(e){}
      activeSockets.delete(sanitized);
      socketCreationTime.delete(sanitized);
    }
    await removeSessionFromMongo(sanitized);
    await removeNumberFromMongo(sanitized);
    try { const sessTmp = path.join(os.tmpdir(), `session_${sanitized}`); if (fs.existsSync(sessTmp)) fs.removeSync(sessTmp); } catch(e){}
    res.json({ ok: true, message: `Session ${sanitized} removed` });
  } catch (err) {
    console.error('API /api/session/delete error', err);
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


router.get('/api/newsletters', async (req, res) => {
  try {
    const list = await listNewslettersFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});
router.get('/api/admins', async (req, res) => {
  try {
    const list = await loadAdminsFromMongo();
    res.json({ ok: true, list });
  } catch (err) {
    res.status(500).json({ ok: false, error: err.message || err });
  }
});


// ---------------- cleanup + process events ----------------

process.on('exit', () => {
  activeSockets.forEach((socket, number) => {
    try { socket.ws.close(); } catch (e) {}
    activeSockets.delete(number);
    socketCreationTime.delete(number);
    try { fs.removeSync(path.join(os.tmpdir(), `session_${number}`)); } catch(e){}
  });
});


process.on('uncaughtException', (err) => {
  console.error('Uncaught exception:', err);
  try { exec(`pm2.restart ${process.env.PM2_NAME || 'CHATUWA-MINI-main'}`); } catch(e) { console.error('Failed to restart pm2:', e); }
});


// initialize mongo & auto-reconnect attempt

initMongo().catch(err => console.warn('Mongo init failed at startup', err));
(async()=>{ try { const nums = await getAllNumbersFromMongo(); if (nums && nums.length) { for (const n of nums) { if (!activeSockets.has(n)) { const mockRes = { headersSent:false, send:()=>{}, status:()=>mockRes }; await EmpirePair(n, mockRes); await delay(500); } } } } catch(e){} })();

module.exports = router;


