import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { Api } from 'telegram/tl/index.js';
import {Account} from '../models/db.js';
import { devMessages } from '../utils/devMessages.js';

// Global control variables
let preachingActive = false;
let preachingController = null;
let activeClients = new Map();

// Comprehensive user agent pool
const getUserAgent = () => {
  const userAgents = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:121.0) Gecko/20100101 Firefox/121.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.1 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36 Edg/120.0.0.0",
  ];
  return userAgents[Math.floor(Math.random() * userAgents.length)];
};

const getRandomDeviceModel = () => {
  const devices = [
    "Desktop", "Laptop", "iPhone15,2", "SM-G998B", "Pixel 8",
    "MacBookPro18,1", "ThinkPad X1", "Dell XPS 13", "iPad13,1"
  ];
  return devices[Math.floor(Math.random() * devices.length)];
};

const getRandomSystemVersion = () => {
  const versions = ["10.0", "11.0", "14.1.1", "10.15.7", "13.6.1", "Ubuntu 22.04"];
  return versions[Math.floor(Math.random() * versions.length)];
};

const getRandomAppVersion = () => {
  const major = Math.floor(Math.random() * 5) + 8;
  const minor = Math.floor(Math.random() * 10);
  const patch = Math.floor(Math.random() * 20);
  return `${major}.${minor}.${patch}`;
};

const getRandomLangCode = () => {
  const langs = ["en", "en-US", "en-GB", "es", "fr"];
  return langs[Math.floor(Math.random() * langs.length)];
};

const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));
const CHANNEL_ID_BIAS = 1000000000000n;

const normalizeUsername = (value = '') => {
  if (!value) return null;
  const cleaned = value.replace('@', '').trim().toLowerCase();
  return cleaned || null;
};

const extractUsernameFromLink = (link = '') => {
  if (!link) return null;
  let normalized = link.trim();
  normalized = normalized.replace(/^https?:\/\//i, '');
  normalized = normalized.replace(/^t\.me\//i, '');
  normalized = normalized.replace(/^telegram\.me\//i, '');
  normalized = normalized.split('/')[0];
  normalized = normalized.split('?')[0];
  return normalizeUsername(normalized);
};

const buildEntityCache = (dialogs = []) => {
  const cache = new Map();
  for (const dialog of dialogs) {
    const entity = dialog?.entity;
    if (!entity) continue;

    if (entity.id !== undefined && entity.id !== null) {
      cache.set(entity.id.toString(), entity);
    }

    if (entity.username) {
      cache.set(normalizeUsername(entity.username), entity);
    }
  }
  return cache;
};

const isNumericId = (value) => {
  if (typeof value === 'bigint') return true;
  if (typeof value === 'number') return Number.isFinite(value);
  if (typeof value === 'string') {
    return /^-?\d+$/.test(value.trim());
  }
  return false;
};

const toBigIntId = (value) => {
  if (typeof value === 'bigint') return value;
  if (typeof value === 'number') return BigInt(Math.trunc(value));
  return BigInt(value);
};

const resolveEntityFromCache = (entityCache, group) => {
  if (!entityCache || !group) return null;
  const candidates = [];
  if (group.id) candidates.push(group.id.toString());
  const usernameFromLink = extractUsernameFromLink(group.link);
  if (usernameFromLink) candidates.push(usernameFromLink);
  if (group.username) candidates.push(normalizeUsername(group.username));

  for (const key of candidates) {
    if (key && entityCache.has(key)) {
      return entityCache.get(key);
    }
  }
  return null;
};

const resolveGroupEntity = async (client, group, entityCache) => {
  if (!group) {
    throw new Error('Missing group information');
  }

  const cachedEntity = resolveEntityFromCache(entityCache, group);
  if (cachedEntity) {
    return cachedEntity;
  }

  const usernameFromLink = extractUsernameFromLink(group.link);
  const usernameCandidates = [];
  if (usernameFromLink) usernameCandidates.push(usernameFromLink);
  if (group.username) usernameCandidates.push(normalizeUsername(group.username));

  for (const username of usernameCandidates) {
    if (!username) continue;
    try {
      const entity = await client.getEntity(username);
      if (entityCache) {
        entityCache.set(group.id?.toString() || username, entity);
      }
      return entity;
    } catch (error) {
      // Try next candidate
    }
  }

  if (group.id && isNumericId(group.id)) {
    const numericId = toBigIntId(group.id);
    const idCandidates = new Set();
    idCandidates.add(numericId);
    idCandidates.add(-numericId);
    idCandidates.add(-CHANNEL_ID_BIAS - numericId);
    idCandidates.add(-CHANNEL_ID_BIAS + numericId);

    for (const candidate of idCandidates) {
      try {
        const entity = await client.getEntity(candidate);
        if (entityCache) {
          entityCache.set(group.id.toString(), entity);
        }
        return entity;
      } catch (error) {
        continue;
      }
    }
  }

  throw new Error(`Unable to resolve entity for ${group.name || group.id}`);
};

// Get today's date string (YYYY-MM-DD)
const getTodayDate = () => {
  const today = new Date();
  return today.toISOString().split('T')[0];
};

// Get remaining hours until end of day
const getRemainingHoursToday = () => {
  const now = new Date();
  const endOfDay = new Date(now);
  endOfDay.setHours(23, 59, 59, 999);
  
  const remainingMs = endOfDay - now;
  const remainingHours = remainingMs / (1000 * 60 * 60);
  
  return remainingHours;
};

// Calculate interval between messages for a group
const calculateMessageInterval = (group) => {
  const today = getTodayDate();
  // Only consider today's tracker - ignore old trackers from previous days
  const todayTracker = group.dailyTracker?.find(t => t.date === today);
  
  const messagesSent = todayTracker?.messageCount || 0;
  const messagesRemaining = group.msgPerDay - messagesSent;
  
  if (messagesRemaining <= 0) {
    return null;
  }
  
  const remainingHours = getRemainingHoursToday();
  let intervalHours = remainingHours / messagesRemaining;
  
  // If there's very little time left, allow sending immediately (minimum 0.1 hours = 6 minutes)
  if (intervalHours < 0.1) {
    intervalHours = 0.1;
  } else if (intervalHours < 1) {
    intervalHours = 1;
  }
  
  if (intervalHours > 6) {
    intervalHours = 6;
  }
  
  const intervalMs = intervalHours * 60 * 60 * 1000;
  
  return intervalMs;
};

// Calculate how many messages a group should have sent since last message
const calculateMissedMessages = (group) => {
  const today = getTodayDate();
  // Only consider today's tracker - ignore old trackers from previous days
  const todayTracker = group.dailyTracker?.find(t => t.date === today);
  
  if (!todayTracker || !todayTracker.lastSentAt) {
    return 0;
  }
  
  const timeSinceLastSend = Date.now() - new Date(todayTracker.lastSentAt).getTime();
  const requiredInterval = calculateMessageInterval(group);
  
  if (requiredInterval === null) {
    return 0;
  }
  
  const missedIntervals = Math.floor(timeSinceLastSend / requiredInterval);
  const messagesSent = todayTracker.messageCount || 0;
  const maxPossibleMessages = Math.min(missedIntervals, group.msgPerDay - messagesSent);
  
  return Math.max(0, maxPossibleMessages);
};

// Handle catch-up for groups that missed multiple messages
const handleCatchUp = async (group, account, localMessageId) => {
  const missedMessages = calculateMissedMessages(group);
  
  if (missedMessages <= 1) {
    return { shouldSend: true, newLocalMessageId: localMessageId + 1 };
  }
  
  console.log(`  🔄 Catch-up: ${group.name} (${missedMessages} messages behind)`);
  
  return { shouldSend: true, newLocalMessageId: localMessageId + 1, catchUpMode: true };
};

// Create Telegram client with fingerprinting
function createClient(session) {
  const apiId = parseInt(process.env.API_ID);
  const apiHash = process.env.API_HASH;
  const stringSession = new StringSession(session);
  
  const client = new TelegramClient(stringSession, apiId, apiHash, {
    connectionRetries: 5,
    deviceModel: getRandomDeviceModel(),
    systemVersion: getRandomSystemVersion(),
    appVersion: getRandomAppVersion(),
    langCode: getRandomLangCode(),
    systemLangCode: getRandomLangCode(),
    useIPv6: Math.random() < 0.3,
    userAgent: getUserAgent(),
  });
  
  client.setLogLevel('none');
  
  return client;
}

// Handle account errors
const handleAccountError = async (error, accountNumber) => {
  const msg = error?.errorMessage?.toUpperCase() || error?.message?.toUpperCase() || '';
  const code = error?.code;

  if (msg.includes('CHAT_WRITE_FORBIDDEN') || 
      msg.includes('CHAT_ADMIN_REQUIRED') ||
      msg.includes('USER_BANNED_IN_CHANNEL') ||
      msg.includes('CHAT_SEND_') ||
      msg.includes('SLOWMODE_WAIT_') ||
      msg.includes('CHANNEL_INVALID') ||
      msg.includes('CHANNEL_PRIVATE') ||
      msg.includes('MSG_ID_INVALID') ||
      msg.includes('PEER_ID_INVALID')) {
    return { isGroupError: true };
  }

  if (code === 420 || msg.includes('FLOOD_WAIT')) {
    return { isFloodWait: true };
  }

  if (code === 401 && (msg.includes('AUTH_KEY_UNREGISTERED') || msg.includes('SESSION_REVOKED'))) {
    console.error(`❌ [${accountNumber}] Session revoked - logging out`);
    
    const alertMsg = `🚫 Account logged out:\n${accountNumber}\n\nAccount removed from database.`;
    
    try {
      if (global.bot && global.adminChatId) {
        await global.bot.telegram.sendMessage(global.adminChatId, alertMsg);
      }
      await Account.findOneAndDelete({ number: accountNumber });
    } catch (cleanupError) {
      // Silently handle cleanup errors
    }
    
    return { isCritical: true };
  }

  if (code === 400 && msg.includes('AUTH_BYTES_INVALID')) {
    console.error(`❌ [${accountNumber}] Corrupted session - logging out`);
    
    const alertMsg = `🚫 Corrupted session:\n${accountNumber}\n\nAccount removed from database.`;
    
    try {
      if (global.bot && global.adminChatId) {
        await global.bot.telegram.sendMessage(global.adminChatId, alertMsg);
      }
      await Account.findOneAndDelete({ number: accountNumber });
    } catch (cleanupError) {
      // Silently handle cleanup errors
    }
    
    return { isCritical: true };
  }

  if (msg.includes('USER_DEACTIVATED')) {
    console.error(`❌ [${accountNumber}] Account deactivated - logging out`);
    
    const alertMsg = `🚫 Account deactivated:\n${accountNumber}\n\nAccount removed from database.`;
    
    try {
      if (global.bot && global.adminChatId) {
        await global.bot.telegram.sendMessage(global.adminChatId, alertMsg);
      }
      await Account.findOneAndDelete({ number: accountNumber });
    } catch (cleanupError) {
      // Silently handle cleanup errors
    }
    
    return { isCritical: true };
  }

  return { isOtherError: true };
};

// Clean up old daily tracker entries (keep only today's and yesterday's for safety)
const cleanupOldTrackers = async (accountId, groupId) => {
  const today = getTodayDate();
  const yesterday = new Date();
  yesterday.setDate(yesterday.getDate() - 1);
  const yesterdayStr = yesterday.toISOString().split('T')[0];
  
  try {
    const account = await Account.findById(accountId);
    if (!account) return false;

    const group = account.groups.find(g => g.id === groupId);
    if (!group) return false;

    if (!group.dailyTracker || group.dailyTracker.length === 0) {
      return true;
    }

    // Keep only today's and yesterday's trackers
    group.dailyTracker = group.dailyTracker.filter(
      t => t.date === today || t.date === yesterdayStr
    );

    await account.save();
    return true;
  } catch (error) {
    console.error(`Error cleaning up trackers:`, error);
    return false;
  }
};

// Update daily tracker for a group
const updateDailyTracker = async (accountId, groupId) => {
  const today = getTodayDate();
  
  try {
    const account = await Account.findById(accountId);
    if (!account) return false;

    const group = account.groups.find(g => g.id === groupId);
    if (!group) return false;

    // Clean up old trackers first
    await cleanupOldTrackers(accountId, groupId);

    if (!group.dailyTracker) {
      group.dailyTracker = [];
    }

    let todayTracker = group.dailyTracker.find(t => t.date === today);

    if (!todayTracker) {
      group.dailyTracker.push({ 
        date: today, 
        messageCount: 0,
        lastSentAt: null
      });
    }

    await account.save();
    return true;
  } catch (error) {
    console.error(`Error updating daily tracker:`, error);
    return false;
  }
};

// Update daily tracker after successful message send
const incrementDailyTracker = async (accountId, groupId) => {
  const today = getTodayDate();
  
  try {
    const account = await Account.findById(accountId);
    if (!account) {
      console.error(`Account ${accountId} not found`);
      return false;
    }

    const group = account.groups.find(g => g.id === groupId);
    if (!group) {
      console.error(`Group ${groupId} not found`);
      return false;
    }

    if (!group.dailyTracker) {
      group.dailyTracker = [];
    }

    let todayTracker = group.dailyTracker.find(t => t.date === today);

    if (!todayTracker) {
      todayTracker = { 
        date: today, 
        messageCount: 0,
        lastSentAt: null
      };
      group.dailyTracker.push(todayTracker);
    }

    todayTracker.messageCount += 1;
    todayTracker.lastSentAt = new Date().toISOString();

    await account.save();
    
    return true;
  } catch (error) {
    console.error(`Error updating tracker:`, error);
    return false;
  }
};

// Check if group has reached daily limit
const hasReachedDailyLimit = (group) => {
  const today = getTodayDate();
  // Only check today's tracker - ignore old trackers from previous days
  const todayTracker = group.dailyTracker?.find(t => t.date === today);
  
  if (!todayTracker) return false;
  
  return todayTracker.messageCount >= group.msgPerDay;
};

// Get next message using local message ID counter
const getNextMessageWithLocalId = (localMessageId) => {
  const totalMessages = Object.keys(devMessages).length;
  const nextId = localMessageId >= totalMessages - 1 ? 0 : localMessageId + 1;
  
  return {
    id: nextId,
    text: devMessages[nextId]
  };
};

// Get random delay between messages
const getRandomDelay = () => {
  return Math.floor(Math.random() * 6000) + 2000;
};

// Calculate the next earliest time when any group will be ready
const getNextEarliestReadyTime = async () => {
  try {
    const accounts = await Account.find({ admin: false });
    let earliestReadyTime = null;
    let earliestGroupInfo = null;
    const today = getTodayDate();
    
    // FIRST: Check if any group hasn't sent today (should send immediately)
    for (const account of accounts) {
      for (const group of account.groups) {
        if (hasReachedDailyLimit(group)) {
          continue;
        }
        
        const todayTracker = group.dailyTracker?.find(t => t.date === today);
        
        // If no tracker for today OR no lastSentAt, group is ready NOW
        if (!todayTracker || !todayTracker.lastSentAt) {
          console.log(`  🔍 Found group ready to send: ${account.number}:${group.name} (no messages sent today)`);
          return { readyTime: 0, groupInfo: `${account.number}:${group.name}` };
        }
      }
    }
    
    // SECOND: If all groups have sent today, find the next one ready
    for (const account of accounts) {
      for (const group of account.groups) {
        if (hasReachedDailyLimit(group)) {
          continue;
        }
        
        const requiredInterval = calculateMessageInterval(group);
        if (requiredInterval === null) {
          continue;
        }
        
        const todayTracker = group.dailyTracker?.find(t => t.date === today);
        
        if (!todayTracker || !todayTracker.lastSentAt) {
          // This shouldn't happen since we checked above, but just in case
          return { readyTime: 0, groupInfo: `${account.number}:${group.name}` };
        }
        
        const timeSinceLastSend = Date.now() - new Date(todayTracker.lastSentAt).getTime();
        const timeUntilReady = requiredInterval - timeSinceLastSend;
        
        if (timeUntilReady <= 0) {
          return { readyTime: 0, groupInfo: `${account.number}:${group.name}` };
        }
        
        if (earliestReadyTime === null || timeUntilReady < earliestReadyTime) {
          earliestReadyTime = timeUntilReady;
          earliestGroupInfo = `${account.number}:${group.name}`;
        }
      }
    }
    
    // If no groups found, all are done for today
    if (earliestReadyTime === null) {
      return { readyTime: 10 * 60 * 1000, groupInfo: null };
    }
    
    return {
      readyTime: earliestReadyTime,
      groupInfo: earliestGroupInfo
    };
  } catch (error) {
    console.error('Error calculating next ready time:', error);
    return { readyTime: 10 * 60 * 1000, groupInfo: 'Error occurred' };
  }
};

// Check if group has sufficient activity
const checkGroupActivity = async (client, group, accountNumber, entityCache) => {
  try {
    const me = await client.getMe();
    const myUserId = me.id.toString();

    try {
      const entity = await resolveGroupEntity(client, group, entityCache);
      const messages = await client.getMessages(entity, { limit: 5 });
      
      if (!messages || messages.length === 0) {
        return { hasActivity: true };
      }

      const hasOurMessage = messages.some(msg => {
        const senderId = msg.senderId?.toString() || msg.fromId?.userId?.toString();
        return senderId === myUserId;
      });

      if (hasOurMessage) {
        return { hasActivity: false, reason: 'Message still in recent 5' };
      }

      return { hasActivity: true };
    } catch (resolveError) {
      console.warn(`  ⚠️ [${accountNumber}] Could not check activity for ${group.name}: ${resolveError.message}`);
      return { hasActivity: true };
    }

    return { hasActivity: true };

  } catch (error) {
    console.warn(`  ⚠️ [${accountNumber}] Activity check failed for ${group.name}: ${error.message}`);
    return { hasActivity: true };
  }
};

// Send message to a group
const sendMessageToGroup = async (client, group, message, accountNumber, entityCache) => {
  try {
    const entity = await resolveGroupEntity(client, group, entityCache);
    await client.sendMessage(entity, { message: message });
    return { success: true };
    
  } catch (error) {
    console.error(`  ❌ [${accountNumber}] Failed to send to ${group.name}: ${error.message}`);
    const errorResult = await handleAccountError(error, accountNumber);
    
    if (errorResult.isGroupError) {
      return { success: false, skipGroup: true };
    }
    
    if (errorResult.isCritical) {
      return { success: false, critical: true };
    }
    
    if (errorResult.isFloodWait) {
      return { success: false, floodWait: true };
    }
    
    if (errorResult.isOtherError) {
      return { success: false, otherError: true };
    }
    
    return { success: false, otherError: true };
  }
};

// Process a single account
const processAccount = async (account) => {
  let client = null;
  
  try {
    client = createClient(account.session);
    await client.connect();
    
    activeClients.set(account.number, client);
    
    // Build entity cache for efficient group resolution
    let entityCache = null;
    try {
      const dialogs = await client.getDialogs();
      entityCache = buildEntityCache(dialogs);
    } catch (error) {
      console.warn(`  ⚠️ [${account.number}] Could not build entity cache: ${error.message}`);
      entityCache = new Map();
    }
    
    let messagesSent = 0;
    let localMessageId = account.currentMessageId || 0;
    
    console.log(`  🔍 [${account.number}] Processing ${account.groups.length} groups...`);
    
    for (const group of account.groups) {
      if (!preachingActive || preachingController?.signal.aborted) {
        break;
      }
      
      if (hasReachedDailyLimit(group)) {
        console.log(`  ⏭️  [${account.number}] Skipping ${group.name} - daily limit reached`);
        continue;
      }
      
      const today = getTodayDate();
      const todayTracker = group.dailyTracker?.find(t => t.date === today);
      
      // If no tracker exists for today, send immediately (first message of the day)
      if (!todayTracker || !todayTracker.lastSentAt) {
        console.log(`  ✅ [${account.number}] ${group.name} is ready to send (first message today)`);
      } else {
        // Check interval only if we've already sent messages today
        const requiredInterval = calculateMessageInterval(group);
        
        if (requiredInterval === null) {
          console.log(`  ⏭️  [${account.number}] Skipping ${group.name} - interval calculation returned null`);
          continue;
        }
        
        const timeSinceLastSend = Date.now() - new Date(todayTracker.lastSentAt).getTime();
        
        if (timeSinceLastSend < requiredInterval) {
          const waitMinutes = Math.ceil((requiredInterval - timeSinceLastSend) / (1000 * 60));
          console.log(`  ⏭️  [${account.number}] Skipping ${group.name} - need to wait ${waitMinutes} more minutes`);
          continue;
        }
        
        console.log(`  ✅ [${account.number}] ${group.name} is ready to send (interval: ${Math.ceil(requiredInterval / (1000 * 60))} min)`);
      }
      
      const catchUpResult = await handleCatchUp(group, account, localMessageId);
      if (!catchUpResult.shouldSend) {
        console.log(`  ⏭️  [${account.number}] Skipping ${group.name} - catchUp returned shouldSend: false`);
        continue;
      }
      
      await updateDailyTracker(account._id, group.id);
      
      const nextMessage = getNextMessageWithLocalId(localMessageId);
      
      console.log(`  📤 ${group.name} (${todayTracker ? todayTracker.messageCount + 1 : 1}/${group.msgPerDay})`);
      
      const activityCheck = await checkGroupActivity(client, group, account.number, entityCache);
      
      if (!activityCheck.hasActivity) {
        console.log(`  ⏭️  Skipping ${group.name} - ${activityCheck.reason}`);
        
        localMessageId = catchUpResult.newLocalMessageId;
        
        await Account.updateOne(
          { _id: account._id },
          { $inc: { currentMessageId: 1 } }
        );
        
        await incrementDailyTracker(account._id, group.id);
        
        console.log(`  ✅ Marked as sent (skipped due to low activity)`);
        
        const delay = getRandomDelay();
        await sleep(delay);
        continue;
      }
      
      console.log(`  🚀 Attempting to send message to ${group.name}...`);
      const result = await sendMessageToGroup(client, group, nextMessage.text, account.number, entityCache);
      
      if (result?.success) {
        localMessageId = catchUpResult.newLocalMessageId;
        
        await Account.updateOne(
          { _id: account._id },
          { $inc: { currentMessageId: 1 } }
        );
        
        const trackerUpdateSuccess = await incrementDailyTracker(account._id, group.id);
        
        if (!trackerUpdateSuccess) {
          console.error(`  ❌ Failed to update tracker for ${group.name}`);
        }
        
        messagesSent++;
        console.log(`  ✅ Successfully sent message to ${group.name}`);
        
        if (catchUpResult.catchUpMode) {
          const catchUpDelay = 30000 + Math.random() * 30000;
          await sleep(catchUpDelay);
        } else {
          const delay = getRandomDelay();
          await sleep(delay);
        }
      } else if (result?.critical) {
        console.log(`  🚨 Critical error - stopping account`);
        break;
      } else if (result?.skipGroup) {
        console.log(`  ⏭️  Skipping ${group.name} due to group error`);
        continue;
      } else if (result?.floodWait) {
        console.log(`  ⏸️  Flood wait for ${group.name} - will retry later`);
        await sleep(2000);
      } else if (result?.otherError) {
        console.log(`  ⚠️  Other error for ${group.name} - marking as sent and continuing`);
        localMessageId = catchUpResult.newLocalMessageId;
        
        await Account.updateOne(
          { _id: account._id },
          { $inc: { currentMessageId: 1 } }
        );
        
        await incrementDailyTracker(account._id, group.id);
        
        const delay = getRandomDelay();
        await sleep(delay);
      } else {
        console.error(`  ❌ Unknown error sending to ${group.name} - result:`, result);
        localMessageId = catchUpResult.newLocalMessageId;
        await Account.updateOne(
          { _id: account._id },
          { $inc: { currentMessageId: 1 } }
        );
        await incrementDailyTracker(account._id, group.id);
        await sleep(2000);
      }
    }
    
    if (messagesSent > 0) {
      console.log(`  ✅ ${account.number}: ${messagesSent} messages sent`);
    } else {
      // Check if there are groups that should have sent but didn't
      const today = getTodayDate();
      let groupsNeedingMessages = 0;
      for (const group of account.groups) {
        if (hasReachedDailyLimit(group)) continue;
        const tracker = group.dailyTracker?.find(t => t.date === today);
        if (!tracker || !tracker.lastSentAt || tracker.messageCount === 0) {
          groupsNeedingMessages++;
        }
      }
      
      if (groupsNeedingMessages > 0) {
        console.log(`  ⚠️  ${account.number}: No messages sent but ${groupsNeedingMessages} groups need messages today!`);
      } else {
        console.log(`  ℹ️  ${account.number}: No messages sent this cycle`);
      }
    }
    
  } catch (error) {
    console.error(`Error processing ${account.number}:`, error.message);
    await handleAccountError(error, account.number);
  } finally {
    if (client && client.connected) {
      try {
        await client.disconnect();
      } catch (disconnectError) {
        // Silently handle disconnect errors
      }
    }
    activeClients.delete(account.number);
  }
};

/**
 * Start the preaching process
 */
export async function startPreaching(ctx) {
  if (preachingActive) {
    console.log('⚠️  Preaching is already active');
    if (ctx) {
      await ctx.reply('✅ Messages are already being sent!\n\nTo stop, use /stoppreaching');
    }
    return;
  }

  preachingActive = true;
  preachingController = new AbortController();

  if (ctx) {
    await ctx.reply('✅ Message sending started!\n\n📢 Dev messages will be sent to all groups with smart timing.\n\nTo stop, use /stoppreaching');
  }

  console.log('🚀 Starting preaching system...');

  (async () => {
    while (preachingActive && !preachingController.signal.aborted) {
      try {
        const accounts = await Account.find({ admin: false });
        
        if (accounts.length === 0) {
          console.log('No accounts found. Waiting...');
          await sleep(30000);
          continue;
        }

        console.log(`\n🔄 Processing ${accounts.length} accounts...`);

        for (const account of accounts) {
          if (!preachingActive || preachingController.signal.aborted) {
            break;
          }
          
          await processAccount(account);
          
          if (account !== accounts[accounts.length - 1]) {
            const accountDelay = 5000 + Math.random() * 5000;
            await sleep(accountDelay);
          }
        }

        console.log('✅ Cycle complete\n');

        // Check if there are any groups that haven't sent today
        const today = getTodayDate();
        let hasGroupsNeedingMessages = false;
        const allAccounts = await Account.find({ admin: false });
        for (const acc of allAccounts) {
          for (const grp of acc.groups) {
            if (hasReachedDailyLimit(grp)) {
              continue;
            }
            const tracker = grp.dailyTracker?.find(t => t.date === today);
            if (!tracker || !tracker.lastSentAt || tracker.messageCount === 0) {
              hasGroupsNeedingMessages = true;
              break;
            }
          }
          if (hasGroupsNeedingMessages) break;
        }

        while (preachingActive && !preachingController.signal.aborted) {
          const nextReady = await getNextEarliestReadyTime();
          
          // If groups need messages but we got "all done", something is wrong - retry immediately
          if (nextReady.readyTime === 10 * 60 * 1000 && nextReady.groupInfo === null) {
            if (hasGroupsNeedingMessages) {
              console.log('⚠️  Groups need messages but got "all done" - retrying immediately...\n');
              await sleep(5000); // Short delay before retry
              break; // Break to start new cycle
            }
            console.log('📅 All groups done for today. Waiting until tomorrow...');
            const now = new Date();
            const tomorrow = new Date(now);
            tomorrow.setDate(tomorrow.getDate() + 1);
            tomorrow.setHours(1, 0, 0, 0);
            const waitUntilTomorrow = tomorrow.getTime() - now.getTime();
            await sleep(Math.min(waitUntilTomorrow, 24 * 60 * 60 * 1000));
            continue;
          }
          
          if (nextReady.readyTime <= 30000) {
            if (nextReady.readyTime > 0) {
              await sleep(nextReady.readyTime);
            }
            console.log('⚡ Group ready - starting new cycle\n');
            break;
          }
          
          const waitTimeMinutes = Math.ceil(nextReady.readyTime / (1000 * 60));
          const waitTimeHours = (waitTimeMinutes / 60).toFixed(1);
          
          console.log(`💤 Next group ready in ${waitTimeHours}h (${waitTimeMinutes}min)`);
          
          const checkInterval = 5 * 60 * 1000;
          const waitTime = Math.min(nextReady.readyTime, checkInterval);
          
          const actualWaitMinutes = Math.ceil(waitTime / (1000 * 60));
          console.log(`⏰ Sleeping ${actualWaitMinutes} min...\n`);
          
          await sleep(waitTime);
        }
        
      } catch (error) {
        console.error('❌ Error in preaching loop:', error.message);
        
        if (global.bot && global.adminChatId) {
          try {
            await global.bot.telegram.sendMessage(
              global.adminChatId,
              `🚨 CRITICAL ERROR:\n\n${error.message}\n\nPlease check logs.`
            );
          } catch (notifyError) {
            // Silently handle notification errors
          }
        }
        
        await sleep(30000);
      }
    }
    
    console.log('🛑 Preaching system stopped');
  })();
}

/**
 * Stop the preaching process
 */
export async function stopPreaching(ctx) {
  if (!preachingActive) {
    console.log('ℹ️  Preaching is not active');
    if (ctx) {
      await ctx.reply('ℹ️  Messages are not currently being sent.\n\nTo start, use /startpreaching');
    }
    return;
  }

  console.log('🛑 Stopping preaching system...');

  preachingActive = false;

  if (preachingController) {
    preachingController.abort();
  }

  const disconnectPromises = Array.from(activeClients.values()).map(async (client) => {
    try {
      if (client && client.connected) {
        await client.disconnect();
      }
    } catch (error) {
      console.error('Error disconnecting client:', error);
    }
  });

  await Promise.allSettled(disconnectPromises);
  activeClients.clear();

  console.log('✅ All clients disconnected. Preaching system stopped.');

  if (ctx) {
    await ctx.reply('✅ Message sending stopped!\n\n📢 All accounts have been disconnected.\n\nTo start again, use /startpreaching');
  }
}