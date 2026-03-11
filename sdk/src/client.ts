import { Socket } from "net";
import type { LuxConfig, RespValue } from "./types";
import { LuxConnectionError, LuxError } from "./errors";

export class Lux {
  private socket: Socket | null = null;
  private host: string;
  private port: number;
  private connected = false;
  private buffer = "";
  private queue: { resolve: (v: RespValue) => void; reject: (e: Error) => void }[] = [];

  constructor(config: LuxConfig) {
    this.host = config.host;
    this.port = config.port || 6379;
  }

  async connect(): Promise<void> {
    if (this.connected) return;

    return new Promise((resolve, reject) => {
      this.socket = new Socket();

      this.socket.on("connect", () => {
        this.connected = true;
        resolve();
      });

      this.socket.on("data", (data) => {
        this.buffer += data.toString();
        this.processBuffer();
      });

      this.socket.on("error", (err) => {
        if (!this.connected) {
          reject(new LuxConnectionError(err.message));
        }
        for (const entry of this.queue) {
          entry.reject(new LuxError(err.message));
        }
        this.queue = [];
      });

      this.socket.on("close", () => {
        this.connected = false;
      });

      this.socket.setNoDelay(true);
      this.socket.connect(this.port, this.host);
    });
  }

  disconnect(): void {
    if (this.socket) {
      this.socket.destroy();
      this.socket = null;
      this.connected = false;
    }
  }

  async command(...args: (string | number)[]): Promise<RespValue> {
    if (!this.socket || !this.connected) {
      throw new LuxConnectionError("not connected");
    }

    const encoded = this.encode(args);
    return new Promise((resolve, reject) => {
      this.queue.push({ resolve, reject });
      this.socket!.write(encoded);
    });
  }

  async pipeline(commands: (string | number)[][]): Promise<RespValue[]> {
    if (!this.socket || !this.connected) {
      throw new LuxConnectionError("not connected");
    }

    let encoded = "";
    for (const args of commands) {
      encoded += this.encode(args);
    }

    const promises: Promise<RespValue>[] = [];
    for (let i = 0; i < commands.length; i++) {
      promises.push(
        new Promise((resolve, reject) => {
          this.queue.push({ resolve, reject });
        })
      );
    }

    this.socket.write(encoded);
    return Promise.all(promises);
  }

  async set(key: string, value: string, ttl?: number): Promise<string> {
    const args: (string | number)[] = ["SET", key, value];
    if (ttl !== undefined) args.push("EX", ttl);
    return (await this.command(...args)) as string;
  }

  async get(key: string): Promise<string | null> {
    return (await this.command("GET", key)) as string | null;
  }

  async del(...keys: string[]): Promise<number> {
    return (await this.command("DEL", ...keys)) as number;
  }

  async exists(...keys: string[]): Promise<number> {
    return (await this.command("EXISTS", ...keys)) as number;
  }

  async incr(key: string): Promise<number> {
    return (await this.command("INCR", key)) as number;
  }

  async decr(key: string): Promise<number> {
    return (await this.command("DECR", key)) as number;
  }

  async incrby(key: string, delta: number): Promise<number> {
    return (await this.command("INCRBY", key, delta)) as number;
  }

  async expire(key: string, seconds: number): Promise<number> {
    return (await this.command("EXPIRE", key, seconds)) as number;
  }

  async ttl(key: string): Promise<number> {
    return (await this.command("TTL", key)) as number;
  }

  async keys(pattern: string): Promise<string[]> {
    return (await this.command("KEYS", pattern)) as string[];
  }

  async mset(...pairs: string[]): Promise<string> {
    return (await this.command("MSET", ...pairs)) as string;
  }

  async mget(...keys: string[]): Promise<(string | null)[]> {
    return (await this.command("MGET", ...keys)) as (string | null)[];
  }

  async lpush(key: string, ...values: string[]): Promise<number> {
    return (await this.command("LPUSH", key, ...values)) as number;
  }

  async rpush(key: string, ...values: string[]): Promise<number> {
    return (await this.command("RPUSH", key, ...values)) as number;
  }

  async lpop(key: string): Promise<string | null> {
    return (await this.command("LPOP", key)) as string | null;
  }

  async rpop(key: string): Promise<string | null> {
    return (await this.command("RPOP", key)) as string | null;
  }

  async llen(key: string): Promise<number> {
    return (await this.command("LLEN", key)) as number;
  }

  async lrange(key: string, start: number, stop: number): Promise<string[]> {
    return (await this.command("LRANGE", key, start, stop)) as string[];
  }

  async hset(key: string, ...fieldValues: string[]): Promise<number> {
    return (await this.command("HSET", key, ...fieldValues)) as number;
  }

  async hget(key: string, field: string): Promise<string | null> {
    return (await this.command("HGET", key, field)) as string | null;
  }

  async hdel(key: string, ...fields: string[]): Promise<number> {
    return (await this.command("HDEL", key, ...fields)) as number;
  }

  async hgetall(key: string): Promise<Record<string, string>> {
    const arr = (await this.command("HGETALL", key)) as string[];
    const result: Record<string, string> = {};
    for (let i = 0; i < arr.length; i += 2) {
      result[arr[i]] = arr[i + 1];
    }
    return result;
  }

  async sadd(key: string, ...members: string[]): Promise<number> {
    return (await this.command("SADD", key, ...members)) as number;
  }

  async srem(key: string, ...members: string[]): Promise<number> {
    return (await this.command("SREM", key, ...members)) as number;
  }

  async smembers(key: string): Promise<string[]> {
    return (await this.command("SMEMBERS", key)) as string[];
  }

  async sismember(key: string, member: string): Promise<number> {
    return (await this.command("SISMEMBER", key, member)) as number;
  }

  async publish(channel: string, message: string): Promise<number> {
    return (await this.command("PUBLISH", channel, message)) as number;
  }

  async dbsize(): Promise<number> {
    return (await this.command("DBSIZE")) as number;
  }

  async flushdb(): Promise<string> {
    return (await this.command("FLUSHDB")) as string;
  }

  async ping(): Promise<string> {
    return (await this.command("PING")) as string;
  }

  private encode(args: (string | number)[]): string {
    let out = `*${args.length}\r\n`;
    for (const arg of args) {
      const s = String(arg);
      out += `$${Buffer.byteLength(s)}\r\n${s}\r\n`;
    }
    return out;
  }

  private processBuffer(): void {
    while (this.queue.length > 0 && this.buffer.length > 0) {
      const result = this.parseOne(0);
      if (result === null) break;

      const [value, consumed] = result;
      this.buffer = this.buffer.slice(consumed);
      const entry = this.queue.shift()!;

      if (typeof value === "string" && value.startsWith("ERR")) {
        entry.reject(new LuxError(value));
      } else {
        entry.resolve(value);
      }
    }
  }

  private parseOne(pos: number): [RespValue, number] | null {
    if (pos >= this.buffer.length) return null;

    const nl = this.buffer.indexOf("\r\n", pos);
    if (nl === -1) return null;

    const type = this.buffer[pos];
    const line = this.buffer.slice(pos + 1, nl);
    const after = nl + 2;

    switch (type) {
      case "+":
        return [line, after];
      case "-":
        return [line, after];
      case ":":
        return [parseInt(line, 10), after];
      case "$": {
        const len = parseInt(line, 10);
        if (len === -1) return [null, after];
        const end = after + len + 2;
        if (end > this.buffer.length) return null;
        return [this.buffer.slice(after, after + len), end];
      }
      case "*": {
        const count = parseInt(line, 10);
        if (count === -1) return [null, after];
        if (count === 0) return [[], after];
        const arr: RespValue[] = [];
        let cursor = after;
        for (let i = 0; i < count; i++) {
          const elem = this.parseOne(cursor);
          if (elem === null) return null;
          arr.push(elem[0]);
          cursor = elem[1];
        }
        return [arr, cursor];
      }
      default:
        return null;
    }
  }
}
