const launchBot = (bot) => {
  bot.telegram
    .getMe()
    .then((botInfo) => {
      console.log(`Bot ${botInfo.username} is connected and running.`);
      bot.launch()
    })
    .catch((err) => {
      console.error("Error connecting bot:", err);
      console.log("Retrying bot connection...");
      setTimeout(launchBot, 2000);
    });
};

export default launchBot;
