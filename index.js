import {
  handleStart,
  handleAllAccounts,
  handleAddAccount,
  handleRefreshGroups,
  handleSetReportChannel,
  handleRestartMonitoring,
  getUserSession,
  handlePhoneNumber,
  handleVerificationCode,
  handlePassword,
  handleChannelUsername,
} from "./helpers/botHandlers.js";
import dotenv from "dotenv";
import { Telegraf } from "telegraf";
import { connectDB } from "./models/db.js";
import { startPreaching, stopPreaching } from "./helpers/preaching.js";
import { startMessageMonitoring, stopMessageMonitoring } from "./helpers/messageMonitor.js";
import launchBot from "./helpers/launchbot.js";
import express from "express";
dotenv.config();

const app = express();
const bot = new Telegraf(process.env.BOT_TOKEN);
global.bot = bot;
global.adminChatId = 1632962204; // Admin chat ID for notifications
global.adminUsername = '@endurenow';

app.get("/ping", (req, res) => {
  res.send("Pong");
});

const PORT = process.env.PORT || 3000
app.listen(PORT, () => {
  console.log("Server is running on port ",PORT);
});

bot.start(handleStart)
bot.action("all_accounts", handleAllAccounts);
bot.action("add_account", handleAddAccount);
bot.action("refresh_groups", handleRefreshGroups);
bot.action("set_report_channel", handleSetReportChannel);
bot.action("start_preaching", startPreaching)
bot.action("stop_preaching", stopPreaching)
bot.action("restart_monitoring", handleRestartMonitoring)

// Back button navigation
bot.action("back_to_main", async (ctx) => {
  await handleStart(ctx);
});

// Callback query handlers
bot.action("all_accounts", handleAllAccounts);
bot.action(/^back_to_/, async (ctx) => {
  // Handle back button navigation
  const targetMenu = ctx.match[0].split("back_to_")[1];

  if (targetMenu === "main") {
    await handleStart(ctx);
  } else if (targetMenu === "accounts") {
    await handleAllAccounts(ctx);
  }
});

bot.action("add_account", handleAddAccount);
bot.action("refresh_groups", handleRefreshGroups);
bot.action("set_report_channel", handleSetReportChannel);

// Text message handler for multi-step conversations
bot.on("text", async (ctx) => {
  const userId = ctx.from.id;
  const session = getUserSession(userId);

  if (!session) return;

  if (session.step === "awaiting_number") {
    await handlePhoneNumber(ctx, session);
  } else if (session.step === "awaiting_code") {
    await handleVerificationCode(ctx, session);
  } else if (session.step === "awaiting_password") {
    await handlePassword(ctx, session);
  } else if (session.step === "awaiting_channel") {
    await handleChannelUsername(ctx, session);
  }
});

bot.telegram.setMyCommands([{command:"/start", description:"Start the bot"}])
// ============================================
// Initialize Bot
// ============================================
async function main() {
  try {
    // Connect to database
    await connectDB();

    // Launch bot
    launchBot(bot);
    
    // Start message monitoring for DMs and replies
    await startMessageMonitoring();
    
    // Start preaching functionality
    startPreaching();

    // Graceful shutdown
    process.once("SIGINT", async () => {
      console.log("\n⏳ Shutting down bot...");
      await stopPreaching();
      await stopMessageMonitoring();
      bot.stop("SIGINT");
      process.exit(0);
    });

    process.once("SIGTERM", async () => {
      console.log("\n⏳ Shutting down bot...");
      await stopPreaching();
      await stopMessageMonitoring();
      bot.stop("SIGTERM");
      process.exit(0);
    });
  } catch (error) {
    console.error("❌ Failed to start bot:", error);
    process.exit(1);
  }
}

main();

// Silence telegram timeout errors only
process.on('unhandledRejection', (reason) => {
  if (reason && (reason.message === 'TIMEOUT' || reason.toString().includes('TIMEOUT'))) {
    return; // Silent
  }
  console.error('Unhandled Rejection:', reason);
});

process.on('uncaughtException', (error) => {
  if (error && (error.message === 'TIMEOUT' || error.toString().includes('TIMEOUT'))) {
    return; // Silent
  }
  console.error('Uncaught Exception:', error);
});