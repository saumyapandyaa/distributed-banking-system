import { useState } from "react";
import { transfer } from "../api/tellerApi";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { Button } from "../components/ui/button";

export default function Transfer() {
  const [fromAcc, setFromAcc] = useState("");
  const [toAcc, setToAcc] = useState("");
  const [amount, setAmount] = useState("");

  const handleTransfer = async () => {
    try {
      await transfer(fromAcc, toAcc, Number(amount));
      alert("Transfer successful!");
    } catch (err) {
      console.error(err);
      alert("Error processing transfer");
    }
  };

  return (
    <div className="relative min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-16">
      <div className="absolute inset-x-0 top-12 mx-auto h-72 max-w-2xl rounded-3xl bg-glass-gradient blur-3xl" />
      <div className="relative mx-auto w-full max-w-xl">
        <Card className="glass-panel">
          <CardHeader>
            <CardTitle className="text-3xl">Manual transfer</CardTitle>
            <p className="text-sm text-muted-foreground">
              Use when neither internal nor same-branch workflow applies.
            </p>
          </CardHeader>

          <CardContent className="space-y-5">
            <div className="space-y-2">
              <Label htmlFor="transfer-from">From account</Label>
              <Input
                id="transfer-from"
                placeholder="Account number"
                value={fromAcc}
                onChange={(e) => setFromAcc(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="transfer-to">To account</Label>
              <Input
                id="transfer-to"
                placeholder="Account number"
                value={toAcc}
                onChange={(e) => setToAcc(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="transfer-amount">Amount</Label>
              <Input
                id="transfer-amount"
                type="number"
                placeholder="0.00"
                value={amount}
                onChange={(e) => setAmount(e.target.value)}
              />
            </div>

            <Button className="w-full" onClick={handleTransfer}>
              Confirm transfer
            </Button>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}

