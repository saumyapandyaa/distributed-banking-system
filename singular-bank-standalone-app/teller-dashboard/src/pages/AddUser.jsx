import { useState } from "react";
import { useNavigate } from "react-router-dom";
import { createUser } from "../api/tellerApi";
import { Card, CardContent, CardHeader, CardTitle } from "../components/ui/card";
import { Input } from "../components/ui/input";
import { Label } from "../components/ui/label";
import { Button } from "../components/ui/button";

export default function AddUser() {
  const [name, setName] = useState("");
  const [phone, setPhone] = useState("");
  const navigate = useNavigate();

  const handleSubmit = async () => {
    try {
      const user = { name, phone };
      await createUser(user);
      alert("User created!");
      navigate("/ledger");
    } catch (err) {
      console.error(err);
      alert("Error creating user");
    }
  };

  return (
    <div className="relative min-h-screen bg-gradient-to-br from-slate-50 via-white to-slate-100 px-4 py-16">
      <div className="absolute inset-x-0 top-10 mx-auto h-72 max-w-4xl rounded-3xl bg-glass-gradient blur-3xl" />
      <div className="relative mx-auto w-full max-w-2xl">
        <Card className="glass-panel">
          <CardHeader className="pb-2">
            <span className="text-xs font-semibold uppercase tracking-[0.35em] text-slate-400">
              New Customer
            </span>
            <CardTitle className="text-3xl">Create user profile</CardTitle>
            <p className="text-sm text-muted-foreground">
              Capture key details to issue accounts and cards faster.
            </p>
          </CardHeader>

          <CardContent className="space-y-6">
            <div className="space-y-2">
              <Label htmlFor="fullName">Full Name</Label>
              <Input
                id="fullName"
                placeholder="Jane Doe"
                value={name}
                onChange={(e) => setName(e.target.value)}
              />
            </div>

            <div className="space-y-2">
              <Label htmlFor="phone">Phone Number</Label>
              <Input
                id="phone"
                placeholder="(123) 456‑7890"
                value={phone}
                onChange={(e) => setPhone(e.target.value)}
              />
            </div>

            <div className="flex flex-col gap-3 pt-2 sm:flex-row sm:items-center sm:justify-between">
              <Button className="flex-1" onClick={handleSubmit}>
                Create user
              </Button>
              <Button
                variant="ghost"
                className="flex-1 text-sm text-muted-foreground"
                onClick={() => navigate("/ledger")}
              >
                ← Back to Ledger
              </Button>
            </div>
          </CardContent>
        </Card>
      </div>
    </div>
  );
}
