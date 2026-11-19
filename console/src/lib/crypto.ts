import crypto from "node:crypto";

const ALGORITHM = "aes-256-gcm";

function getEncryptionKey(): Buffer {
  const raw = process.env.AIR_CONSOLE_ENCRYPTION_KEY || process.env.BETTER_AUTH_SECRET || "air-dev-fallback-secret-key-32-bytes!";
  return crypto.createHash("sha256").update(raw).digest();
}

export function sha256Hex(value: string): string {
  return crypto.createHash("sha256").update(value).digest("hex");
}

export function generateId(prefix: string): string {
  return `${prefix}_${crypto.randomBytes(10).toString("hex")}`;
}

export function generateApiKeyToken(): { token: string; prefix: string; hash: string } {
  const random = crypto.randomBytes(24).toString("base64url");
  const token = `air_key_${random}`;
  const prefix = token.slice(0, 14);
  const hash = sha256Hex(token);
  return { token, prefix, hash };
}

export function encryptSecret(plaintext: string): string {
  if (!plaintext) return "";
  const key = getEncryptionKey();
  const iv = crypto.randomBytes(12);
  const cipher = crypto.createCipheriv(ALGORITHM, key, iv);
  const encrypted = Buffer.concat([cipher.update(plaintext, "utf8"), cipher.final()]);
  const tag = cipher.getAuthTag();
  return `${iv.toString("base64")}:${tag.toString("base64")}:${encrypted.toString("base64")}`;
}

export function decryptSecret(ciphertext: string): string {
  if (!ciphertext) return "";
  const parts = ciphertext.split(":");
  if (parts.length !== 3) return "";
  const [ivB64, tagB64, dataB64] = parts;
  const key = getEncryptionKey();
  const iv = Buffer.from(ivB64, "base64");
  const tag = Buffer.from(tagB64, "base64");
  const data = Buffer.from(dataB64, "base64");
  const decipher = crypto.createDecipheriv(ALGORITHM, key, iv);
  decipher.setAuthTag(tag);
  return Buffer.concat([decipher.update(data), decipher.final()]).toString("utf8");
}
