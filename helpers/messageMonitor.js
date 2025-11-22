import { TelegramClient } from 'telegram';
import { StringSession } from 'telegram/sessions/index.js';
import { NewMessage } from 'telegram/events/index.js';
import { Account, Customer } from '../models/db.js';

// Global storage for active monitoring clients
let monitoringClients = new Map();
let reconnectIntervals = new Map();

// Create Telegram client for monitoring
function createMonitoringClient(session) {
  const apiId = parseInt(process.env.API_ID);
  const apiHash = process.env.API_HASH;
  const stringSession = new StringSession(session);

  const client = new TelegramClient(stringSession, apiId, apiHash, {
    connectionRetries: 5,
    timeout: 30000,
    requestRetries: 3,
    autoReconnect: true,
  });

  // Suppress logs
  client.setLogLevel('none');

  return client;
}

// Send notification to admin
async function notifyAdmin(message) {
  try {
    if (global.bot && global.adminChatId) {
      await global.bot.telegram.sendMessage(global.adminChatId, message);
    }
  } catch (error) {
    console.error('❌ Error sending admin notification:', error);
  }
}

// Reconnect monitoring for a specific account
async function reconnectMonitoring(account, accountUsername) {
  try {
    console.log(`🔄 Reconnecting monitoring for ${accountUsername}...`);
    
    // Stop existing monitoring for this account
    const existing = monitoringClients.get(account.number);
    if (existing && existing.client) {
      try {
        if (existing.client.connected) {
          await existing.client.disconnect();
        }
      } catch (e) {
        // Ignore disconnect errors
      }
    }

    // Clear any existing reconnect interval
    if (reconnectIntervals.has(account.number)) {
      clearInterval(reconnectIntervals.get(account.number));
      reconnectIntervals.delete(account.number);
    }

    // Wait a bit before reconnecting
    await new Promise(resolve => setTimeout(resolve, 5000));

    // Start monitoring again
    await startMonitoringAccount(account);

  } catch (error) {
    console.error(`❌ Error reconnecting ${accountUsername}:`, error);
  }
}

// Start monitoring for a single account
async function startMonitoringAccount(account) {
  try {
    const client = createMonitoringClient(account.session);
    await client.connect();

    // Get account username
    const me = await client.getMe();
    const accountUsername = me.username ? `@${me.username}` : account.number;

    monitoringClients.set(account.number, { client, username: accountUsername });

    // Set up disconnect handler
    client.addEventHandler(async (update) => {
      if (update.className === 'UpdateConnectionState') {
        console.log(`⚠️ Connection state changed for ${accountUsername}`);
        
        // If disconnected, try to reconnect after delay
        if (!client.connected) {
          console.log(`❌ ${accountUsername} disconnected - scheduling reconnect`);
          setTimeout(() => {
            reconnectMonitoring(account, accountUsername);
          }, 10000);
        }
      }
    });

    client.addEventHandler(
      async (event) => {
        try {
          const message = event.message;

          // Skip our own messages
          if (message.out) return;

          const sender = await message.getSender();
          const username = sender.username ? `@${sender.username}` : 'No username';
          const userId = sender.id.toString();
          const content = message.text || '[Media message]';

          // Handle DMs
          if (event.isPrivate) {
            // Check if customer already exists
            const existingCustomer = await Customer.findOne({ userId: userId });

            if (!existingCustomer) {
              // First message - save, notify, and reply
              const customer = new Customer({
                username: username,
                userId: userId,
                textedAt: new Date(),
                type: 'dm',
                content: content,
                senderAccount: account.number,
              });

              await customer.save();

              // Send notification to admin
              const notificationMessage = `🔔 New DM to ${accountUsername}

From: ${username} | ID: ${userId}

Message:
${content}`;

              await notifyAdmin(notificationMessage);

              // Auto-reply to the sender
              try {
                const replyMessage = `Hi, i will reply you from my main account ${global.adminUsername}. Please hold on`;
                await client.sendMessage(sender, { message: replyMessage });
              } catch (replyError) {
                console.error('Error sending auto-reply:', replyError);
              }
            }
            // If customer exists, ignore subsequent messages
            return;
          }
          // Handle group messages - check if it's a reply to our message
          else if (event.isGroup && message.replyTo && message.replyTo.replyToMsgId) {
            // Get the message being replied to
            const repliedToMsg = await client.getMessages(message.peerId, {
              ids: [message.replyTo.replyToMsgId]
            });

            // Only notify if the reply is to our message
            if (repliedToMsg && repliedToMsg[0] && repliedToMsg[0].out) {
              const chat = await message.getChat();
              const groupName = chat.title || 'Unknown Group';
              const groupId = chat.id.toString();

              // Create clickable link to the actual reply message
              let messageLink;
              if (chat.username) {
                messageLink = `https://t.me/${chat.username}/${message.id}`;
              } else {
                // For private groups, use the group ID format
                const cleanGroupId = groupId.replace('-100', '');
                messageLink = `https://t.me/c/${cleanGroupId}/${message.id}`;
              }

              const notificationMessage = `🔔 New Reply to ${accountUsername}

From: ${username} | ID: ${userId}

Group: <a href="${messageLink}">${groupName}</a>

Message:
${content}`;

              await global.bot.telegram.sendMessage(global.adminChatId, notificationMessage, {
                parse_mode: 'HTML',
                disable_web_page_preview: true,
              });
              return;
            }
          }
        } catch (error) {
          console.error(`Error processing message for ${accountUsername}:`, error);
          // Don't let individual message errors crash the entire monitoring
        }
      },
      new NewMessage({ incoming: true })
    );

    // Set up periodic health check
    const healthCheckInterval = setInterval(async () => {
      try {
        if (!client.connected) {
          console.log(`⚠️ Health check failed for ${accountUsername} - reconnecting`);
          clearInterval(healthCheckInterval);
          reconnectMonitoring(account, accountUsername);
        }
      } catch (error) {
        console.error(`Health check error for ${accountUsername}:`, error);
        clearInterval(healthCheckInterval);
        reconnectMonitoring(account, accountUsername);
      }
    }, 5 * 60 * 1000); // Check every 5 minutes

    // Store health check interval
    reconnectIntervals.set(account.number, healthCheckInterval);

    console.log(`✅ Monitoring started for account: ${accountUsername}`);

  } catch (error) {
    const errorMsg = error?.errorMessage || error?.message || '';
    const errorCode = error?.code;
    
    // Handle AUTH_KEY_DUPLICATED - session is being used elsewhere
    if (errorCode === 406 || errorMsg.includes('AUTH_KEY_DUPLICATED')) {
      console.warn(`⚠️  Account ${account.number}: Session is being used by another client (preaching system). Monitoring will be skipped for this account.`);
      console.warn(`   This is normal when the preaching system is active. Monitoring will resume when preaching stops.`);
      return; // Don't retry - this account is being used for preaching
    }
    
    console.error(`❌ Error starting monitoring for account ${account.number}:`, error);
    
    // Retry after delay if initial connection fails (but not for AUTH_KEY_DUPLICATED)
    setTimeout(() => {
      console.log(`🔄 Retrying monitoring for ${account.number}...`);
      startMonitoringAccount(account);
    }, 30000);
  }
}

// Start monitoring for all accounts
export async function startMessageMonitoring() {
  try {
    const accounts = await Account.find({ admin: false });

    if (accounts.length === 0) {
      console.log('ℹ️ No non-admin accounts found for monitoring');
      return;
    }

    for (const account of accounts) {
      await startMonitoringAccount(account);
      await new Promise(resolve => setTimeout(resolve, 2000));
    }

    console.log(`✅ Message monitoring started for ${accounts.length} accounts`);

  } catch (error) {
    console.error('❌ Error starting message monitoring:', error);
  }
}

// Stop monitoring for all accounts
export async function stopMessageMonitoring() {
  try {
    // Clear all health check intervals
    for (const interval of reconnectIntervals.values()) {
      clearInterval(interval);
    }
    reconnectIntervals.clear();

    const disconnectPromises = Array.from(monitoringClients.values()).map(async (clientData) => {
      try {
        const client = clientData.client || clientData;
        if (client && client.connected) {
          await client.disconnect();
        }
      } catch (error) {
        // Silently handle disconnect errors
      }
    });

    await Promise.allSettled(disconnectPromises);
    monitoringClients.clear();

    console.log('✅ Message monitoring stopped');

  } catch (error) {
    console.error('❌ Error stopping message monitoring:', error);
  }
}

// Restart monitoring when new accounts are added
export async function restartMonitoring() {
  await stopMessageMonitoring();
  await new Promise(resolve => setTimeout(resolve, 3000));
  await startMessageMonitoring();
}