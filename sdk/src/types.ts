export interface LuxConfig {
  host: string;
  port?: number;
  maxRetries?: number;
  retryDelay?: number;
}

export type RespValue = string | number | null | RespValue[];
