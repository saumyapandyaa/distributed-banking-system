// src/pages/TellerDashboard.jsx
import React, { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import {
  getUserAccounts,
  getHistory,
  getCards,
  createCard,
  deleteCard,
} from "../api/tellerApi";
import StatsChart from "../components/StatsChart";
import {
  Card,
  CardContent,
  CardDescription,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Badge } from "../components/ui/badge";
import { ScrollArea } from "../components/ui/scroll-area";

export default function TellerDashboard() {
  const { userId } = useParams();
  const navigate = useNavigate();

  const [accounts, setAccounts] = useState([]);
  const [selectedType, setSelectedType] = useState("checking");
  const [currentAccount, setCurrentAccount] = useState(null);
  const [transactions, setTransactions] = useState([]);

  const [cards, setCards] = useState([]);
  const [selectedCardId, setSelectedCardId] = useState(null);
  const [isCardLoading, setIsCardLoading] = useState(false);

  /* ------------------------------------------
   * Load accounts
   * ----------------------------------------- */
  useEffect(() => {
    async function loadAccounts() {
      try {
        const res = await getUserAccounts(userId);
        const data = res.data || [];
        setAccounts(data);

        const checking = data.find((a) => a.account_type === "checking");
        const savings = data.find((a) => a.account_type === "savings");

        const acc = checking || savings;
        if (acc) {
          setSelectedType(acc.account_type);
          setCurrentAccount(acc);
          loadHistory(acc.account_number);
        }
      } catch (err) {
        console.error("Failed to load accounts:", err);
      }
    }
    loadAccounts();
  }, [userId]);

  /* ------------------------------------------
   * Load history
   * ----------------------------------------- */
  async function loadHistory(accNo) {
    try {
      const res = await getHistory(accNo);
      setTransactions(res.data || []);
    } catch (err) {
      console.error("Failed to load history:", err);
    }
  }

  useEffect(() => {
    if (!accounts.length) return;
    const acc = accounts.find((a) => a.account_type === selectedType);
    if (acc) {
      setCurrentAccount(acc);
      loadHistory(acc.account_number);
    }
  }, [selectedType, accounts]);

  /* ------------------------------------------
   * Load cards
   * ----------------------------------------- */
  async function loadCards() {
    try {
      setIsCardLoading(true);
      const res = await getCards(userId);
      const list = res.data || [];
      setCards(list);

      // Debit card always default
      const defaultCard =
        list.find((c) => c.card_type === "debit") || list[0] || null;

      setSelectedCardId(defaultCard?.id || null);
    } catch (err) {
      console.error("Failed to load cards:", err);
    } finally {
      setIsCardLoading(false);
    }
  }

  useEffect(() => {
    loadCards();
  }, [userId]);

  /* ------------------------------------------
   * Add card
   * ----------------------------------------- */
  async function handleAddCard(type) {
    try {
      setIsCardLoading(true);
      await createCard(userId, type);
      await loadCards();
    } catch {
      alert("Error creating card.");
    } finally {
      setIsCardLoading(false);
    }
  }

  /* ------------------------------------------
   * Delete non-debit card
   * ----------------------------------------- */
  async function handleDeleteSelectedCard(card) {
    if (!card) return;

    if (card.card_type === "debit") {
      alert("Cannot delete default debit card.");
      return;
    }

    if (!window.confirm(`Delete card ending ${card.card_number.slice(-4)}?`))
      return;

    try {
      setIsCardLoading(true);
      await deleteCard(card.card_number);
      await loadCards();
    } catch {
      alert("Error deleting card.");
    } finally {
      setIsCardLoading(false);
    }
  }

  /* ------------------------------------------
   * Card ordering
   * ----------------------------------------- */
  const orderedCards = useMemo(() => {
    if (!cards.length) return [];
    const front = cards.find((c) => c.id === selectedCardId);
    const rest = cards.filter((c) => c.id !== selectedCardId);
    return front ? [front, ...rest] : cards;
  }, [cards, selectedCardId]);

  const selectedCard = orderedCards[0] || null;

  /* ------------------------------------------
   * Card helpers
   * ----------------------------------------- */
  function cardBg(type) {
    switch ((type || "").toLowerCase()) {
      case "debit":
        return "bg-[#0B1120]";
      case "credit":
        return "bg-[#1D4ED8]";
      case "prepaid":
        return "bg-[#15803D]";
      default:
        return "bg-slate-700";
    }
  }

  function cardLabel(type) {
    switch ((type || "").toLowerCase()) {
      case "debit":
        return "DEBIT CARD";
      case "credit":
        return "CREDIT CARD";
      case "prepaid":
        return "PREPAID CARD";
      default:
        return "CARD";
    }
  }

  /* ------------------------------------------
   * Wallet UI (3-card stack, fully visible)
   * ----------------------------------------- */
  const renderWallet = () => (
    <Card className="glass-panel h-full">
      <CardHeader className="flex flex-row items-center justify-between space-y-0">
        <div>
          <CardTitle className="text-lg">My wallet</CardTitle>
          <CardDescription>Issued payment cards</CardDescription>
        </div>
        <div className="flex gap-2">
          <Button
            size="sm"
            variant="outline"
            disabled={isCardLoading}
            onClick={() => handleAddCard("credit")}
          >
            + Credit
          </Button>
          <Button
            size="sm"
            variant="outline"
            disabled={isCardLoading}
            onClick={() => handleAddCard("prepaid")}
          >
            + Prepaid
          </Button>
        </div>
      </CardHeader>
      <CardContent>
        {orderedCards.length ? (
          <div className="relative mx-auto mt-4 h-[330px] w-full max-w-[340px]">
            {orderedCards.slice(1).map((card, i) => (
              <div
                key={card.id}
                className={`absolute flex h-[170px] w-[300px] cursor-pointer flex-col justify-between rounded-3xl p-5 text-white shadow-xl ${cardBg(
                  card.card_type
                )}`}
                style={{
                  top: 40 + i * 30,
                  left: 30 + i * 20,
                  zIndex: 10 - i,
                  opacity: 0.7,
                }}
                onClick={() => setSelectedCardId(card.id)}
              >
                <div className="text-xs font-semibold uppercase tracking-[0.35em]">
                  {cardLabel(card.card_type)}
                </div>
                <div className="text-xs">EXP {card.expiry}</div>
              </div>
            ))}

            {selectedCard && (
              <div
                className={`absolute flex h-[190px] w-[320px] cursor-pointer flex-col justify-between rounded-3xl p-6 text-white shadow-2xl ${cardBg(
                  selectedCard.card_type
                )}`}
                style={{ top: 0, left: 0, zIndex: 20 }}
                onClick={() => setSelectedCardId(selectedCard.id)}
              >
                <div className="flex items-center justify-between text-xs uppercase tracking-[0.35em]">
                  <span>{cardLabel(selectedCard.card_type)}</span>
                  {selectedCard.card_type === "debit" && (
                    <Badge
                      variant="outline"
                      className="border-white/70 bg-white/10 text-[10px] text-white"
                    >
                      Default
                    </Badge>
                  )}
                </div>
                <div className="text-lg tracking-[0.35em]">
                  {selectedCard.card_number}
                </div>

                <div className="flex justify-between text-xs">
                  <div>
                    <p className="text-white/70">EXP</p>
                    <p>{selectedCard.expiry}</p>
                  </div>
                  <div>
                    <p className="text-white/70">CVV</p>
                    <p>{selectedCard.cvv}</p>
                  </div>
                </div>

                {selectedCard.card_type !== "debit" && (
                  <Button
                    size="sm"
                    variant="outline"
                    className="mt-3 border-white/60 text-xs text-white hover:bg-white/10"
                    onClick={(e) => {
                      e.stopPropagation();
                      handleDeleteSelectedCard(selectedCard);
                    }}
                    disabled={isCardLoading}
                  >
                    Delete card
                  </Button>
                )}
              </div>
            )}
          </div>
        ) : (
          <div className="rounded-2xl border border-dashed border-slate-300 px-4 py-8 text-center text-sm text-muted-foreground">
            {isCardLoading ? "Loading cards..." : "No cards issued yet"}
          </div>
        )}
      </CardContent>
    </Card>
  );

  /* ------------------------------------------
   * MAIN LAYOUT
   * ----------------------------------------- */
  return (
    <div className="min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-8">
      <div className="mx-auto grid min-h-[calc(100vh-4rem)] max-w-7xl gap-6 lg:grid-cols-[280px_1fr]">
        <aside className="space-y-6">
          <Card className="glass-panel">
            <CardHeader className="pb-0">
              <CardTitle className="text-xl">User overview</CardTitle>
              <CardDescription>Act on behalf of the customer</CardDescription>
            </CardHeader>
            <CardContent className="space-y-4 pt-4">
              <div className="flex items-center gap-3 rounded-2xl border border-dashed border-slate-200/80 p-4">
                <div className="flex h-12 w-12 items-center justify-center rounded-full bg-primary/10 text-lg font-semibold text-primary">
                  {String(userId).slice(-2)}
                </div>
                <div>
                  <p className="text-sm font-semibold text-slate-900">
                    User #{userId}
                  </p>
                  <p className="text-xs text-muted-foreground">
                    {currentAccount
                      ? `${selectedType} · ${currentAccount.account_number}`
                      : "No active accounts"}
                  </p>
                </div>
              </div>

              <div className="space-y-2">
                <Button
                  className="w-full justify-start"
                  variant="secondary"
                  onClick={() => navigate(`/users/${userId}/deposit`)}
                >
                  Deposit funds
                </Button>
                <Button
                  className="w-full justify-start"
                  variant="secondary"
                  onClick={() => navigate(`/users/${userId}/withdraw`)}
                >
                  Withdraw cash
                </Button>
                <Button
                  className="w-full justify-start"
                  variant="secondary"
                  onClick={() => navigate(`/users/${userId}/transfer`)}
                >
                  Transfer money
                </Button>
              </div>
            </CardContent>
          </Card>

          <Card className="glass-panel">
            <CardHeader>
              <CardTitle className="text-base">Session</CardTitle>
              <CardDescription>Save progress & revisit the ledger</CardDescription>
            </CardHeader>
            <CardContent className="space-y-3">
              <Button
                variant="outline"
                className="w-full"
                onClick={() => navigate("/ledger")}
              >
                ← Back to Ledger
              </Button>
            </CardContent>
          </Card>
        </aside>

        <section className="space-y-6">
          <div className="flex flex-col gap-4 lg:flex-row lg:items-center lg:justify-between">
            <div>
              <p className="text-sm uppercase tracking-[0.35em] text-muted-foreground">
                Teller dashboard
              </p>
              <h1 className="text-3xl font-semibold text-slate-900">
                Customer snapshot
              </h1>
            </div>
            <div className="flex flex-wrap gap-2">
              <Button
                variant={selectedType === "checking" ? "default" : "outline"}
                onClick={() => setSelectedType("checking")}
              >
                Checking
              </Button>
              <Button
                variant={selectedType === "savings" ? "default" : "outline"}
                onClick={() => setSelectedType("savings")}
              >
                Savings
              </Button>
            </div>
          </div>

          {currentAccount && (
            <Card className="overflow-hidden border-none bg-gradient-to-r from-indigo-600 via-indigo-500 to-sky-500 text-white shadow-soft-card">
              <CardContent className="space-y-4 p-6">
                <div className="flex flex-col gap-2 sm:flex-row sm:items-center sm:justify-between">
                  <p className="text-sm uppercase tracking-[0.35em] text-white/70">
                    Total balance
                  </p>
                  <Badge className="bg-white/20 text-xs text-white">
                    Account · {currentAccount.account_number}
                  </Badge>
                </div>
                <p className="text-4xl font-semibold">${currentAccount.balance}</p>
                <p className="text-sm text-white/70">
                  Last refreshed · {new Date().toLocaleTimeString()}
                </p>
              </CardContent>
            </Card>
          )}

          <div className="grid gap-6 lg:grid-cols-3">
            <Card className="glass-panel lg:col-span-2">
              <CardHeader className="flex flex-row items-center justify-between">
                <CardTitle className="text-lg">Statistics</CardTitle>
                <CardDescription>Money in vs. money out</CardDescription>
              </CardHeader>
              <CardContent className="h-[320px]">
                <StatsChart
                  transactions={transactions}
                  currentBalance={currentAccount?.balance}
                />
              </CardContent>
            </Card>

            <Card className="glass-panel">
              <CardHeader>
                <CardTitle className="text-lg">Transactions</CardTitle>
                <CardDescription>Most recent activity</CardDescription>
              </CardHeader>
              <CardContent className="h-[320px]">
                <ScrollArea className="h-full pr-2">
                  <div className="space-y-3">
                    {transactions.length ? (
                      transactions.map((t, i) => (
                        <div
                          key={`${t.tx_type}-${i}`}
                          className="flex items-center justify-between rounded-xl border border-slate-100/80 px-3 py-2"
                        >
                          <div>
                            <p className="text-sm font-medium text-slate-900">
                              {t.tx_type}
                            </p>
                            <p className="text-xs text-muted-foreground">
                              {t.timestamp?.slice(0, 10) || "—"}
                            </p>
                          </div>
                          <p
                            className={`text-sm font-semibold ${
                              t.amount >= 0 ? "text-emerald-600" : "text-red-500"
                            }`}
                          >
                            {t.amount >= 0 ? "+" : ""}
                            {t.amount}
                          </p>
                        </div>
                      ))
                    ) : (
                      <p className="text-sm text-muted-foreground">
                        No activity recorded yet.
                      </p>
                    )}
                  </div>
                </ScrollArea>
              </CardContent>
            </Card>
          </div>

          <div className="grid gap-6 lg:grid-cols-3">
            <div className="lg:col-span-2">{renderWallet()}</div>

            <Card className="glass-panel">
              <CardHeader>
                <CardTitle className="text-lg">Accounts overview</CardTitle>
                <CardDescription>Balance by type</CardDescription>
              </CardHeader>
              <CardContent className="space-y-4">
                {accounts.length ? (
                  accounts.map((acc) => (
                    <div
                      key={acc.account_number}
                      className="rounded-2xl border border-slate-100/70 px-4 py-3"
                    >
                      <div className="flex items-center justify-between">
                        <div>
                          <p className="text-sm font-semibold capitalize">
                            {acc.account_type}
                          </p>
                          <p className="text-xs text-muted-foreground">
                            Acct · {acc.account_number}
                          </p>
                        </div>
                        <p className="text-sm font-semibold">${acc.balance}</p>
                      </div>
                    </div>
                  ))
                ) : (
                  <p className="text-sm text-muted-foreground">
                    No accounts found for this user.
                  </p>
                )}
              </CardContent>
            </Card>
          </div>
        </section>
      </div>
    </div>
  );
}
