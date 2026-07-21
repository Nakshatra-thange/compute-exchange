

import express from "express";
import cors from "cors";
import { WebSocketServer, WebSocket } from "ws";
import Redis from "ioredis";
import { createServer } from "http";
import { MarketRegistry, marketKey } from "./redis-store";
import { Order, Side } from "./matching-engine";

const PORT = Number(process.env.PORT ?? 4000);

const redis = new Redis(); 
const pub = new Redis(); 
const sub = new Redis(); 

const registry = new MarketRegistry(redis, pub);


const orderLocation = new Map<string, { gpuType: string; termMonths: number }>();

function depthChannel(market: string) {
  return `depth:${market}`;
}



const app = express();
app.use(cors());
app.use(express.json());

app.post("/orders", async (req, res) => {
  try {
    const { side, gpuType, termMonths, quantity, price, partyId } = req.body as Partial<Order>;

    if (!side || !gpuType || termMonths == null || !quantity || price == null || !partyId) {
      return res.status(400).json({ error: "missing required fields" });
    }
    if (side !== "bid" && side !== "ask") {
      return res.status(400).json({ error: "side must be 'bid' or 'ask'" });
    }
    if (quantity <= 0 || price <= 0) {
      return res.status(400).json({ error: "quantity and price must be positive" });
    }

    const order: Order = {
      id: `ord_${Date.now()}_${Math.random().toString(36).slice(2, 8)}`,
      side: side as Side,
      gpuType,
      termMonths: Number(termMonths),
      quantity: Number(quantity),
      price: Number(price),
      partyId,
      timestamp: Date.now(),
    };

    const store = await registry.getOrCreate(order.gpuType, order.termMonths);
    const matches = await store.submit(order);

    orderLocation.set(order.id, { gpuType: order.gpuType, termMonths: order.termMonths });

    // publish fresh depth snapshot for anyone watching this market
    await pub.publish(
      depthChannel(marketKey(order.gpuType, order.termMonths)),
      JSON.stringify(store.depth())
    );

    res.status(201).json({ order, matches });
  } catch (err) {
    console.error(err);
    res.status(500).json({ error: "internal error" });
  }
});

app.delete("/orders/:id", async (req, res) => {
  const { id } = req.params;
  const loc = orderLocation.get(id);
  if (!loc) return res.status(404).json({ error: "unknown order id" });

  const store = await registry.getOrCreate(loc.gpuType, loc.termMonths);
  const removed = await store.cancel(id);
  if (removed) {
    orderLocation.delete(id);
    await pub.publish(depthChannel(marketKey(loc.gpuType, loc.termMonths)), JSON.stringify(store.depth()));
  }
  res.json({ removed });
});

app.get("/markets/:gpuType/:termMonths/depth", async (req, res) => {
  const { gpuType, termMonths } = req.params;
  const store = await registry.getOrCreate(gpuType, Number(termMonths));
  res.json(store.depth());
});

app.get("/markets/:gpuType/:termMonths/history", async (req, res) => {
  const { gpuType, termMonths } = req.params;
  const store = await registry.getOrCreate(gpuType, Number(termMonths));
  res.json(await store.matchHistory());
});

// ---------- WebSocket layer ----------

const httpServer = createServer(app);
const wss = new WebSocketServer({ server: httpServer });

// socket -> set of market keys it's subscribed to
const subscriptions = new Map<WebSocket, Set<string>>();

wss.on("connection", (ws) => {
  subscriptions.set(ws, new Set());

  ws.on("message", async (raw) => {
    try {
      const msg = JSON.parse(raw.toString());
      if (msg.type === "subscribe" && msg.gpuType && msg.termMonths != null) {
        const market = marketKey(msg.gpuType, Number(msg.termMonths));
        subscriptions.get(ws)!.add(market);

        // send an immediate snapshot so the client doesn't wait for the next change
        const store = await registry.getOrCreate(msg.gpuType, Number(msg.termMonths));
        ws.send(JSON.stringify({ type: "depth", market, data: store.depth() }));
      }
      if (msg.type === "unsubscribe" && msg.gpuType && msg.termMonths != null) {
        subscriptions.get(ws)!.delete(marketKey(msg.gpuType, Number(msg.termMonths)));
      }
    } catch {
      ws.send(JSON.stringify({ type: "error", message: "invalid message" }));
    }
  });

  ws.on("close", () => subscriptions.delete(ws));
});

function broadcast(market: string, payload: unknown) {
  for (const [ws, markets] of subscriptions) {
    if (markets.has(market) && ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(payload));
    }
  }
}

sub.psubscribe("depth:*", "matches:*");
sub.on("pmessage", (_pattern, channel, message) => {
  if (channel.startsWith("depth:")) {
    const market = channel.slice("depth:".length);
    broadcast(market, { type: "depth", market, data: JSON.parse(message) });
  } else if (channel.startsWith("matches:")) {
    const market = channel.slice("matches:".length);
    broadcast(market, { type: "match", market, data: JSON.parse(message) });
  }
});

httpServer.listen(PORT, () => {
  console.log(`Matching engine API + WS listening on :${PORT}`);
});