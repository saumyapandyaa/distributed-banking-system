import React, { useState } from "react";
import { useNavigate, useParams } from "react-router-dom";
import apiClient from "../api/apiClient";
import {
  Card,
  CardContent,
  CardHeader,
  CardTitle,
} from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { Button } from "../components/ui/button";

export default function SameBranchTransfer() {
  const { userId } = useParams();
  const navigate = useNavigate();

  const [fromAcc, setFromAcc] = useState("");
  const [toAcc, setToAcc] = useState("");
  const [amount, setAmount] = useState("");

  const handleTransfer = async () => {
    if (!fromAcc || !toAcc || !amount) {
      alert("Please fill all fields.");
      return;
    }

    try {
      await apiClient.post("/transactions/transfer-same-bank", {
        from_account: fromAcc,
        to_account: toAcc,
        amount: Number(amount),
      });

      alert("Same-bank transfer successful!");
      navigate(`/users/${userId}/dashboard`);
    } catch (err) {
      console.error(err);
      alert("Transfer failed.");
    }
  };

  return (
    <div className="relative min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-16">
      <div className="absolute inset-x-0 top-8 mx-auto h-72 max-w-3xl rounded-3xl bg-glass-gradient blur-3xl" />
      <div className="relative mx-auto w-full max-w-2xl">
        <Card className="glass-panel">
          <CardHeader>
            <CardTitle className="text-3xl">Same-bank transfer</CardTitle>
            <p className="text-sm text-muted-foreground">
              Move funds between customers without leaving the branch network.
            </p>
          </CardHeader>

          <CardContent className="space-y-6">
            <div className="space-y-2">
              <Label htmlFor="from-account">From account</Label>
              <Input
                id="from-account"
                placeholder="Source account number"
                value={fromAcc}
                onChange={(e) => setFromAcc(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="to-account">To account</Label>
              <Input
                id="to-account"
                placeholder="Destination account number"
                value={toAcc}
                onChange={(e) => setToAcc(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="branch-amount">Amount</Label>
              <Input
                id="branch-amount"
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
