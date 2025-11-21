const launchBot = (bot) => {
  bot.telegram
    .getMe()
    .then((botInfo) => {
      console.log(`Bot ${botInfo.username} is connected and running.`);
      bot.launch({
        allowedUpdates: [
          "message",
          "edited_message",
          "channel_post",
          "edited_channel_post",
          "inline_query",
          "chosen_inline_result",
          "callback_query",
          "shipping_query",
          "pre_checkout_query",
          "poll",
          "poll_answer",
          "my_chat_member",
          "chat_member",
          "chat_join_request",
        ],
      });
    })
    .catch((err) => {
      console.error("Error connecting bot:", err);
      console.log("Retrying bot connection...");
      setTimeout(launchBot, 2000);
    });
};

export default launchBot;
