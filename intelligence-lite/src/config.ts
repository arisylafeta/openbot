/**
 * Process configuration. The key is the same bearer OpenBot sends as
 * `CPK_INTELLIGENCE_API_KEY` or `INTELLIGENCE_API_KEY`.
 */
export type Config = {
  databaseUrl: string;
  apiKey: string;
  schema: string;
  host: string;
  port: number;
};

const SCHEMA = /^[a-z_][a-z0-9_]*$/;

export function loadConfig(
  environment: Record<string, string | undefined>,
): Config {
  const databaseUrl = environment.DATABASE_URL?.trim();
  if (!databaseUrl) {
    throw new Error("DATABASE_URL is required.");
  }
  const apiKey =
    environment.CPK_INTELLIGENCE_API_KEY?.trim() ||
    environment.INTELLIGENCE_API_KEY?.trim();
  if (!apiKey) {
    throw new Error(
      "CPK_INTELLIGENCE_API_KEY or INTELLIGENCE_API_KEY is required.",
    );
  }
  const schema = environment.INTELLIGENCE_SCHEMA?.trim() || "intelligence_lite";
  if (!SCHEMA.test(schema) || schema.length > 63) {
    throw new Error(
      "INTELLIGENCE_SCHEMA must be a lowercase identifier of at most 63 characters.",
    );
  }
  const port = Number(environment.PORT ?? "7100");
  if (!Number.isInteger(port) || port < 0 || port > 65535) {
    throw new Error("PORT must be an integer from 0 to 65535.");
  }
  return {
    databaseUrl,
    apiKey,
    schema,
    host: environment.HOST?.trim() || "0.0.0.0",
    port,
  };
}

export function quoteIdent(value: string): string {
  if (!SCHEMA.test(value)) {
    throw new Error(`Refusing identifier ${value}`);
  }
  return `"${value}"`;
}
