import { start, type DynoxideServer, type ExitStatus } from "dynoxide";

export async function check(): Promise<void> {
  await using server = await start({ dbPath: "data.db", readyTimeoutMs: 5000 });
  const endpoint: string = server.endpoint;
  const port: number = server.port;
  const source: "option" | "env" | "package" = server.binarySource;
  const { accessKeyId, secretAccessKey } = server.credentials;
  const kept: DynoxideServer = server;
  void [endpoint, port, source, accessKeyId, secretAccessKey, kept.region];
}

export async function stopping(): Promise<ExitStatus> {
  const server = await start();
  return server.stop();
}
