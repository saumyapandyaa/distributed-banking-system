import React, { useEffect, useMemo, useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import { getUserAccounts } from "../api/tellerApi";
import apiClient from "../api/apiClient";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Button } from "../components/ui/button";
import { Label } from "../components/ui/label";
import { Input } from "../components/ui/input";

const meta = {
  checking: {
    label: "Checking",
    blurb: "Daily spending · Instant clearing",
  },
  savings: {
    label: "Savings",
    blurb: "Conservative · Higher balance",
  },
};

export default function InternalTransfer() {
  const { userId } = useParams();
  const navigate = useNavigate();

  const [accounts, setAccounts] = useState([]);
  const [fromType, setFromType] = useState("checking");
  const [toType, setToType] = useState("savings");
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

  const accountByType = useMemo(() => {
    const map = {};
    accounts.forEach((acc) => {
      map[acc.account_type] = acc;
    });
    return map;
  }, [accounts]);

  const handleTransfer = async () => {
    if (fromType === toType) {
      alert("Cannot transfer to the same account type.");
      return;
    }

    try {
      await apiClient.post("/transactions/transfer-internal", {
        user_id: userId,
        from_account_type: fromType,
        to_account_type: toType,
        amount: Number(amount),
      });

      alert("Internal transfer successful!");
      navigate(`/users/${userId}/dashboard`);
    } catch (err) {
      console.error(err);
      alert("Internal transfer failed.");
    }
  };

  const renderAccountOption = (type, isActive, onSelect) => {
    const account = accountByType[type];
    return (
      <button
        type="button"
        onClick={() => onSelect(type)}
        className={`rounded-2xl border px-4 py-4 text-left transition-all ${
          isActive
            ? "border-primary bg-primary/5 shadow-soft-card"
            : "border-dashed border-slate-200 hover:border-slate-300"
        }`}
      >
        <p className="text-sm font-semibold text-slate-900">{meta[type].label}</p>
        <p className="text-xs text-muted-foreground">{meta[type].blurb}</p>
        <p className="mt-3 text-[13px] text-muted-foreground">
          {account
            ? `Acct •••${String(account.account_number).slice(-4)} · $${account.balance}`
            : "Not available"}
        </p>
      </button>
    );
  };

  return (
    <div className="relative min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-16">
      <div className="absolute inset-x-0 top-10 mx-auto h-64 max-w-4xl rounded-3xl bg-glass-gradient blur-3xl" />
      <div className="relative mx-auto w-full max-w-3xl">
        <Card className="glass-panel">
          <CardHeader>
            <CardTitle className="text-3xl">Internal transfer</CardTitle>
            <p className="text-sm text-muted-foreground">
              Move balances between accounts for user #{userId}.
            </p>
          </CardHeader>

          <CardContent className="space-y-8">
            <div className="space-y-3">
              <Label className="uppercase tracking-[0.35em] text-[11px] text-muted-foreground">
                From account
              </Label>
              <div className="grid gap-4 sm:grid-cols-2">
                {["checking", "savings"].map((type) =>
                  renderAccountOption(type, fromType === type, (selected) => {
                    setFromType(selected);
                    setToType(selected === "checking" ? "savings" : "checking");
                  })
                )}
              </div>
            </div>

            <div className="space-y-3">
              <Label className="uppercase tracking-[0.35em] text-[11px] text-muted-foreground">
                To account
              </Label>
              <div className="grid gap-4 sm:grid-cols-2">
                {["checking", "savings"].map((type) =>
                  renderAccountOption(type, toType === type, setToType)
                )}
              </div>
            </div>

            <div className="space-y-2">
              <Label htmlFor="internal-amount">Amount</Label>
              <Input
                id="internal-amount"
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
                onClick={() => navigate(`/users/${userId}/transfer`)}
              >
                Cancel
              </Button>
              <Button className="flex-1" onClick={handleTransfer}>
                Confirm transfer
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
