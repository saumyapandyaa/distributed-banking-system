import { useState } from "react";
import { createAccount } from "../api/tellerApi";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { Button } from "../components/ui/button";

export default function AccountCreation() {
  const [accNum, setAccNum] = useState("");
  const [initialBalance, setInitialBalance] = useState("");

  const handleCreate = async () => {
    try {
      await createAccount({
        account_number: accNum,
        initial_balance: Number(initialBalance),
      });
      alert("Account Created!");
    } catch (err) {
      console.error(err);
      alert("Error creating account");
    }
  };

  return (
    <div className="relative min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-16">
      <div className="absolute inset-x-0 top-12 mx-auto h-72 max-w-2xl rounded-3xl bg-glass-gradient blur-3xl" />
      <div className="relative mx-auto w-full max-w-xl">
        <Card className="glass-panel">
          <CardHeader>
            <CardTitle className="text-3xl">Create account</CardTitle>
            <p className="text-sm text-muted-foreground">
              Provision a new standalone account with an opening balance.
            </p>
          </CardHeader>

          <CardContent className="space-y-5">
            <div className="space-y-2">
              <Label htmlFor="account-number">Account number</Label>
              <Input
                id="account-number"
                placeholder="12-digit account number"
                value={accNum}
                onChange={(e) => setAccNum(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="initial-balance">Initial balance</Label>
              <Input
                id="initial-balance"
                type="number"
                placeholder="0.00"
                value={initialBalance}
                onChange={(e) => setInitialBalance(e.target.value)}
              />
            </div>

            <Button
              className="w-full bg-emerald-600 hover:bg-emerald-600/90"
              onClick={handleCreate}
            >
              Create account
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

