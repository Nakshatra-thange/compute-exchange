

import Redis from "ioredis";
import { OrderBook, Order, Match,BookEntry ,Side } from "./matching-engine";

export const marketKey = (gpuType: string, termMonths: number) => `${gpuType}:${termMonths}`;

function orderKey(orderId: string) {
  return `order:${orderId}`;
}
function restingKey(market: string) {
  return `resting:${market}`; // sorted set: score = timestamp, member = orderId
}
function matchChannel(market: string) {
  return `matches:${market}`; // pub/sub channel, live fan-out
}
function matchStreamKey(market: string) {
  return `matches:stream:${market}`; // durable history, for dashboard replay on load
}

export class RedisOrderBookStore {
  private constructor(
    private redis: Redis,
    private pub: Redis,
    private book: OrderBook,
    private market: string
  ) {}

  /** Load (or create fresh) a market, restoring any resting orders from Redis. */
  static async load(
    redis: Redis,
    pub: Redis,
    gpuType: string,
    termMonths: number
  ): Promise<RedisOrderBookStore> {
    const market = marketKey(gpuType, termMonths);
    const book = new OrderBook(gpuType, termMonths);

    const orderIds = await redis.zrange(restingKey(market), 0, -1); // ascending by timestamp
    for (const id of orderIds) {
      const data = await redis.hgetall(orderKey(id));
      if (!data.id) continue; // stale/missing hash, skip
      const entry: BookEntry = {
        id: data.id,
        side: data.side as Side,
        gpuType: data.gpuType,
        termMonths: Number(data.termMonths),
        quantity: Number(data.quantity),
        remaining: Number(data.remaining),
        price: Number(data.price),
        partyId: data.partyId,
        timestamp: Number(data.timestamp),
      };
      book.restoreEntry(entry);
    }

    return new RedisOrderBookStore(redis, pub, book, market);
  }

  /** Submit a new order: runs the match, then persists every touched order and
   *  broadcasts any resulting matches. */
  async submit(order: Order): Promise<Match[]> {
    const matches = this.book.submit(order);

    await this.persistOrderState(order.id, order);
    for (const m of matches) {
      await this.persistOrderState(m.bidId);
      await this.persistOrderState(m.askId);
      await this.recordMatch(m);
    }

    return matches;
  }

  async cancel(orderId: string): Promise<boolean> {
    const removed = this.book.cancel(orderId);
    if (removed) {
      await this.redis
        .multi()
        .zrem(restingKey(this.market), orderId)
        .del(orderKey(orderId))
        .exec();
    }
    return removed;
  }

  depth(levels = 10) {
    return this.book.depth(levels);
  }

  /** Replay durable match history for this market, e.g. for the dashboard on page load. */
  async matchHistory(limit = 50): Promise<Match[]> {
    const raw = await this.redis.xrevrange(matchStreamKey(this.market), "+", "-", "COUNT", limit);
    return raw.map(([, fields]) => this.fieldsToMatch(fields)).reverse();
  }

  // ---- internals ----

  private async persistOrderState(orderId: string, originalOrder?: Order) {
    const remaining = this.book.getRemaining(orderId);

    if (remaining === undefined) {
      // fully filled — remove from resting set, but keep the hash briefly for audit/debug
      await this.redis.zrem(restingKey(this.market), orderId);
      return;
    }

    // still resting (new order that didn't fully match, or a partially-filled resting order)
    const base = originalOrder ?? (await this.hydratePartialFieldsFallback(orderId));
    await this.redis
      .multi()
      .hset(orderKey(orderId), {
        id: orderId,
        side: base.side,
        gpuType: base.gpuType,
        termMonths: String(base.termMonths),
        quantity: String(base.quantity),
        remaining: String(remaining),
        price: String(base.price),
        partyId: base.partyId,
        timestamp: String(base.timestamp),
      })
      .zadd(restingKey(this.market), base.timestamp, orderId)
      .exec();
  }

  // Only hit when we're updating remaining qty on an order we didn't just submit
  // (i.e. the resting counterpart in a match). Reads its own prior hash as the base.
  private async hydratePartialFieldsFallback(orderId: string): Promise<Order> {
    const data = await this.redis.hgetall(orderKey(orderId));
    return {
      id: data.id,
      side: data.side as Side,
      gpuType: data.gpuType,
      termMonths: Number(data.termMonths),
      quantity: Number(data.quantity),
      price: Number(data.price),
      partyId: data.partyId,
      timestamp: Number(data.timestamp),
    };
  }

  private async recordMatch(m: Match) {
    const fields = {
      id: m.id,
      bidId: m.bidId,
      askId: m.askId,
      gpuType: m.gpuType,
      termMonths: String(m.termMonths),
      quantity: String(m.quantity),
      price: String(m.price),
      buyerPartyId: m.buyerPartyId,
      providerPartyId: m.providerPartyId,
      timestamp: String(m.timestamp),
    };
    await this.redis.xadd(matchStreamKey(this.market), "*", ...Object.entries(fields).flat());
    await this.pub.publish(matchChannel(this.market), JSON.stringify(m));
  }

  private fieldsToMatch(fields: string[]): Match {
    const obj: Record<string, string> = {};
    for (let i = 0; i < fields.length; i += 2) obj[fields[i]] = fields[i + 1];
    return {
      id: obj.id,
      bidId: obj.bidId,
      askId: obj.askId,
      gpuType: obj.gpuType,
      termMonths: Number(obj.termMonths),
      quantity: Number(obj.quantity),
      price: Number(obj.price),
      buyerPartyId: obj.buyerPartyId,
      providerPartyId: obj.providerPartyId,
      timestamp: Number(obj.timestamp),
    };
  }
}

/** Holds one RedisOrderBookStore per active (gpuType, termMonths) market. */
export class MarketRegistry {
  private markets = new Map<string, RedisOrderBookStore>();

  constructor(private redis: Redis, private pub: Redis) {}

  async getOrCreate(gpuType: string, termMonths: number): Promise<RedisOrderBookStore> {
    const key = marketKey(gpuType, termMonths);
    let store = this.markets.get(key);
    if (!store) {
      store = await RedisOrderBookStore.load(this.redis, this.pub, gpuType, termMonths);
      this.markets.set(key, store);
    }
    return store;
  }
}