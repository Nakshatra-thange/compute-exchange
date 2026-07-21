import Redis from "ioredis";
import { MarketRegistry } from "./redis-store";
import { Order } from "./matching-engine";

async function main() {
  const redis = new Redis(); // localhost:6379 by default
  const pub = new Redis();
  const registry = new MarketRegistry(redis, pub);

  const store = await registry.getOrCreate("H100", 6);

  const ask: Order = {
    id: "ask1", side: "ask", gpuType: "H100", termMonths: 6,
    quantity: 100, price: 2.1, partyId: "provider_A", timestamp: Date.now(),
  };
  console.log("ask matches:", await store.submit(ask));

  const bid: Order = {
    id: "bid1", side: "bid", gpuType: "H100", termMonths: 6,
    quantity: 40, price: 2.3, partyId: "buyer_X", timestamp: Date.now(),
  };
  console.log("bid matches:", await store.submit(bid));

  console.log("depth:", store.depth());
  console.log("history:", await store.matchHistory());

  process.exit(0);
}

main();