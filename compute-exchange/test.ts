// test.ts
import { OrderBook, Order } from "./matching-engine";

const book = new OrderBook("H100", 6);

const ask: Order = {
  id: "ask1", side: "ask", gpuType: "H100", termMonths: 6,
  quantity: 100, price: 2.1, partyId: "provider_A", timestamp: Date.now(),
};
console.log("submit ask:", book.submit(ask)); // [] — no bids yet, rests on book

const bid: Order = {
  id: "bid1", side: "bid", gpuType: "H100", termMonths: 6,
  quantity: 40, price: 2.3, partyId: "buyer_X", timestamp: Date.now(),
};
console.log("submit bid:", book.submit(bid)); // 1 match, qty 40, price 2.1

console.log("book depth:", book.depth());
// ask1 should show remaining: 60, no bids resting