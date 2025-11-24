// src/pages/Withdraw.jsx
import React, { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { getUserAccounts, withdraw } from "../api/tellerApi";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Badge } from "../components/ui/badge";
import { Label } from "../components/ui/label";
import { Input } from "../components/ui/input";

const accountCopy = {
  checking: {
    title: "Checking",
    description: "Real-time transactions",
  },
  savings: {
    title: "Savings",
    description: "Slower, higher balance",
  },
};

export default function Withdraw() {
  const { userId } = useParams();
  const navigate = useNavigate();

  const [accounts, setAccounts] = useState([]);
  const [accountType, setAccountType] = useState("checking");
  const [amount, setAmount] = useState("");

  useEffect(() => {
    async function load() {
      try {
        const res = await getUserAccounts(userId);
        setAccounts(res.data || []);
      } catch (err) {
        console.error(err);
      }
    }
    load();
  }, [userId]);

  const activeAccount = useMemo(
    () => accounts.find((acc) => acc.account_type === accountType),
    [accounts, accountType]
  );

  const handleWithdraw = async () => {
    if (!activeAccount) {
      alert("Account not found.");
      return;
    }

    try {
      await withdraw(activeAccount.account_number, Number(amount));
      alert("Withdrawal successful!");
      navigate(`/users/${userId}/dashboard`);
    } catch (error) {
      console.error(error);
      alert("Error processing withdrawal.");
    }
  };

  return (
    <div className="relative min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-16">
      <div className="pointer-events-none absolute inset-x-0 top-10 mx-auto h-64 max-w-xl rounded-3xl bg-rose-200/40 blur-3xl" />
      <div className="relative mx-auto w-full max-w-3xl">
        <Card className="glass-panel">
          <CardHeader className="pb-6">
            <Badge className="w-fit uppercase tracking-[0.35em]" variant="warning">
              Withdraw
            </Badge>
            <CardTitle className="text-3xl">Release customer funds</CardTitle>
            <p className="text-sm text-muted-foreground">
              User #{userId} · Choose the source account then confirm the debit.
            </p>
          </CardHeader>

          <CardContent className="space-y-8">
            <div>
              <span className="text-xs font-semibold uppercase tracking-[0.3em] text-muted-foreground">
                Account type
              </span>
              <div className="mt-3 grid gap-4 sm:grid-cols-2">
                {["checking", "savings"].map((type) => {
                  const info = accountCopy[type];
                  const account = accounts.find(
                    (acc) => acc.account_type === type
                  );
                  return (
                    <button
                      key={type}
                      type="button"
                      onClick={() => setAccountType(type)}
                      className={`rounded-2xl border px-4 py-4 text-left transition-all ${
                        accountType === type
                          ? "border-destructive/60 bg-destructive/5 shadow-soft-card"
                          : "border-dashed border-slate-200 hover:border-slate-300"
                      }`}
                    >
                      <p className="text-sm font-semibold text-slate-900">
                        {info.title}
                      </p>
                      <p className="text-xs text-muted-foreground">
                        {info.description}
                      </p>
                      <p className="mt-3 text-[13px] text-muted-foreground">
                        {account
                          ? `Balance: $${account.balance}`
                          : "No account provisioned"}
                      </p>
                    </button>
                  );
                })}
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="withdraw-amount">Amount</Label>
              <Input
                id="withdraw-amount"
                type="number"
                placeholder="0.00"
                value={amount}
                onChange={(e) => setAmount(e.target.value)}
              />
            </div>

            <div className="flex flex-col gap-3 sm:flex-row sm:justify-between">
              <Button
                variant="ghost"
                className="flex-1 text-muted-foreground"
                onClick={() => navigate(`/users/${userId}/dashboard`)}
              >
                Cancel
              </Button>
              <Button
                className="flex-1 border border-destructive bg-destructive text-white hover:bg-destructive/90"
                onClick={handleWithdraw}
              >
                Confirm withdrawal
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

