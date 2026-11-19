import { loadConfig } from "./config";
import { startServer } from "./server";

const config = loadConfig(process.env);
const started = await startServer(config);
console.log(
  JSON.stringify({
    type: "intelligence-lite-listening",
    url: started.url,
    wsUrl: started.wsUrl,
  }),
);

const shutdown = () => {
  void started.close().then(() => process.exit(0));
};
process.on("SIGINT", shutdown);
process.on("SIGTERM", shutdown);
