
export type Side = "bid" | "ask";

export interface Order {
  id: string;
  side: Side;
  gpuType: string;       
  termMonths: number;   
  quantity: number;      
  price: number;        
  partyId: string;      
  timestamp: number;     
}

export interface Match {
  id: string;
  bidId: string;
  askId: string;
  gpuType: string;
  termMonths: number;
  quantity: number;     
  price: number;         
  buyerPartyId: string;
  providerPartyId: string;
  timestamp: number;
}

interface BookEntry extends Order {
  remaining: number;
}


export class OrderBook {
  readonly gpuType: string;
  readonly termMonths: number;
  private bids: BookEntry[] = [];
  private asks: BookEntry[] = [];
  private matchCounter = 0;

  constructor(gpuType: string, termMonths: number) {
    this.gpuType = gpuType;
    this.termMonths = termMonths;
  }


  submit(order: Order): Match[] {
    if (order.gpuType !== this.gpuType || order.termMonths !== this.termMonths) {
      throw new Error("Order does not belong to this market");
    }
    const entry: BookEntry = { ...order, remaining: order.quantity };
    const matches: Match[] = [];

    const book = entry.side === "bid" ? this.asks : this.bids;

    while (entry.remaining > 0 && book.length > 0) {
      const top = book[0];
      const crosses =
        entry.side === "bid" ? entry.price >= top.price : entry.price <= top.price;
      if (!crosses) break;

      const fillQty = Math.min(entry.remaining, top.remaining);
      const bidEntry = entry.side === "bid" ? entry : top;
      const askEntry = entry.side === "bid" ? top : entry;

      matches.push({
        id: `m_${++this.matchCounter}_${Date.now()}`,
        bidId: bidEntry.id,
        askId: askEntry.id,
        gpuType: this.gpuType,
        termMonths: this.termMonths,
        quantity: fillQty,
        price: top.price, 
        buyerPartyId: bidEntry.partyId,
        providerPartyId: askEntry.partyId,
        timestamp: Date.now(),
      });

      entry.remaining -= fillQty;
      top.remaining -= fillQty;
      if (top.remaining === 0) book.shift();
    }

    if (entry.remaining > 0) {
      this.insertSorted(entry);
    }

    return matches;
  }

  private insertSorted(entry: BookEntry) {
    const book = entry.side === "bid" ? this.bids : this.asks;
    const betterThan = (a: BookEntry, b: BookEntry) =>
      entry.side === "bid"
        ? a.price > b.price || (a.price === b.price && a.timestamp < b.timestamp)
        : a.price < b.price || (a.price === b.price && a.timestamp < b.timestamp);

    let i = 0;
    while (i < book.length && betterThan(book[i], entry)) i++;
    book.splice(i, 0, entry);
  }

  cancel(orderId: string): boolean {
    for (const book of [this.bids, this.asks]) {
      const idx = book.findIndex((o) => o.id === orderId);
      if (idx !== -1) {
        book.splice(idx, 1);
        return true;
      }
    }
    return false;
  }
  /** Look up remaining quantity for an order still resting on the book. Undefined = not resting (fully filled, cancelled, or never rested). */
  getRemaining(orderId: string): number | undefined {
    const found = [...this.bids, ...this.asks].find((o) => o.id === orderId);
    return found?.remaining;
  }

  /** Rebuild a book on startup from persisted resting orders. Trusts that these entries
   *  never crossed each other (they didn't match before the process stopped), so it
   *  inserts directly without running them through submit(). Feed entries in timestamp order. */
  restoreEntry(entry: BookEntry): void {
    const book = entry.side === "bid" ? this.bids : this.asks;
    const betterThan = (a: BookEntry, b: BookEntry) =>
      entry.side === "bid"
        ? a.price > b.price || (a.price === b.price && a.timestamp < b.timestamp)
        : a.price < b.price || (a.price === b.price && a.timestamp < b.timestamp);
    let i = 0;
    while (i < book.length && betterThan(book[i], entry)) i++;
    book.splice(i, 0, entry);
  }

  depth(levels = 10) {
    return {
      gpuType: this.gpuType,
      termMonths: this.termMonths,
      bids: this.bids.slice(0, levels).map((o) => ({
        price: o.price,
        remaining: o.remaining,
        partyId: o.partyId,
      })),
      asks: this.asks.slice(0, levels).map((o) => ({
        price: o.price,
        remaining: o.remaining,
        partyId: o.partyId,
      })),
    };
  }
}