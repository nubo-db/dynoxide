import { start } from "dynoxide";

export async function check(): Promise<void> {
  const server = await start();
  const status = await server.stop();
  void [server.endpoint, status.code];
}
